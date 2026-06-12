"""稳健策略模拟盘 — 每日自动执行脚本

用法:
  uv run python -m src.trader.conservative_sim run      # 执行一个完整交易日(收盘后调用)
  uv run python -m src.trader.conservative_sim report    # 查看累计报告
  uv run python -m src.trader.conservative_sim init      # 重置模拟盘(2万初始资金)

执行流程:
  1. 加载策略状态(复用conservative_state.json)
  2. 检查持仓: 扫描卖出信号(止损/止盈/到期)
  3. 模拟卖出: 更新现金+记录盈亏
  4. 扫描买入机会: ML信号+A级推荐
  5. 模拟买入: 扣款+记录持仓
  6. 保存状态 + 生成当日报告JSON
"""

import json
from datetime import datetime
from pathlib import Path
from dataclasses import asdict
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.trader.conservative_strategy import (
    ConservativeState, Position,
    check_sell_signal, check_buy_signal, calc_buy_amount,
    filter_candidate, scan_opportunities,
    CAPITAL, MAX_POSITIONS, STOP_LOSS_PCT,
    TAKE_PROFIT_PCT, TRAILING_STOP_PCT, MAX_HOLD_DAYS, DAILY_LOSS_LIMIT,
)
from src.trader.realtime_quote import get_realtime

# 模拟盘报告目录(独立于策略state)
SIM_DIR = Path("output/trader/conservative_sim")
SIM_REPORTS = SIM_DIR / "reports"
SIM_TRADELOG = SIM_DIR / "trades.json"


def _ensure_dirs():
    SIM_DIR.mkdir(parents=True, exist_ok=True)
    SIM_REPORTS.mkdir(parents=True, exist_ok=True)


def _pos_obj(p):
    """统一Position对象(可能是dict或dataclass)"""
    return p if hasattr(p, 'code') else Position(**p)


def init_sim():
    """重置模拟盘"""
    _ensure_dirs()
    state = ConservativeState(cash=CAPITAL)
    state.save()
    # 清空交易日志
    SIM_TRADELOG.write_text("[]")
    # 清空报告
    for f in SIM_REPORTS.glob("*.json"):
        f.unlink()
    return {"status": "initialized", "capital": CAPITAL}


def execute_daily_run():
    """执行一个完整交易日"""
    today = datetime.now().strftime("%Y-%m-%d")
    now = datetime.now()

    # 周末跳过
    if now.weekday() >= 5:
        return {"status": "skipped", "reason": "周末不交易", "date": today}

    _ensure_dirs()

    # 防止同一天重复执行
    existing_report = SIM_REPORTS / f"{today}.json"
    if existing_report.exists():
        return json.loads(existing_report.read_text())

    state = ConservativeState.load()

    # 重置日损
    if state.daily_pnl_date != today:
        state.daily_pnl = 0.0
        state.daily_pnl_date = today

    report = {
        "date": today,
        "run_time": now.strftime("%Y-%m-%d %H:%M:%S"),
        "start_cash": round(state.cash, 2),
        "start_positions": len(state.positions),
        "sells": [],
        "buys": [],
        "skipped": [],
        "actions_summary": [],
    }

    # ================================================================
    # 第一步: 检查卖出信号
    # ================================================================
    if state.positions:
        codes = [_pos_obj(p).code for p in state.positions]
        quotes = get_realtime(codes)

        to_sell = []
        for pos_data in state.positions:
            pos = _pos_obj(pos_data)
            q = quotes.get(pos.code, {})
            price = q.get("price", 0)

            if price <= 0:
                report["skipped"].append({
                    "code": pos.code, "name": pos.name,
                    "reason": "无法获取行情",
                })
                continue

            # 更新最高价
            if price > pos.highest_price:
                pos.highest_price = price
                # 同步回state.positions
                for i, pd in enumerate(state.positions):
                    if _pos_obj(pd).code == pos.code:
                        state.positions[i] = pos

            buy_date = datetime.strptime(pos.buy_date, "%Y-%m-%d")
            hold_days = (now - buy_date).days

            sell, reason = check_sell_signal(pos, price, hold_days)

            if sell:
                pnl = (price - pos.buy_price) * pos.shares
                pnl_pct = (price / pos.buy_price - 1) * 100
                sell_amount = price * pos.shares

                to_sell.append(pos.code)

                report["sells"].append({
                    "code": pos.code, "name": pos.name,
                    "buy_price": pos.buy_price, "sell_price": price,
                    "shares": pos.shares, "hold_days": hold_days,
                    "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2),
                    "reason": reason, "source": pos.source,
                })

                state.cash += sell_amount
                state.daily_pnl += pnl
                state.total_pnl += pnl
                state.total_trades += 1
                if pnl > 0:
                    state.win_trades += 1

                report["actions_summary"].append(
                    f"{'✅' if pnl > 0 else '❌'} 卖出 {pos.name} {pos.shares}股@{price:.2f} "
                    f"盈亏{pnl:+.0f}({pnl_pct:+.1f}%) [{reason}]"
                )

        # 移除已卖出
        state.positions = [p for p in state.positions if _pos_obj(p).code not in to_sell]

    # ================================================================
    # 第二步: 扫描买入机会
    # ================================================================
    if len(state.positions) < MAX_POSITIONS and state.daily_pnl > -DAILY_LOSS_LIMIT:
        opportunities = scan_opportunities()

        for opp in opportunities:
            if len(state.positions) >= MAX_POSITIONS:
                break
            if state.cash < 1000:
                break

            price = opp.get("current_price", 0)
            if price <= 0:
                continue

            shares, amount = calc_buy_amount(price, state.cash, len(state.positions))
            if shares < 100:
                continue

            pos = Position(
                code=opp["code"],
                name=opp["name"],
                buy_price=price,
                shares=shares,
                buy_date=today,
                highest_price=price,
                source=opp["source"],
            )

            state.positions.append(pos)
            state.cash -= amount

            report["buys"].append({
                "code": opp["code"], "name": opp["name"],
                "buy_price": price, "shares": shares,
                "amount": round(amount, 2),
                "source": opp["source"],
                "reason": opp.get("signal_reason", ""),
            })

            report["actions_summary"].append(
                f"🟢 买入 {opp['name']} {shares}股@{price:.2f}={amount:.0f}元 "
                f"[{opp.get('signal_reason', '')}]"
            )

    # ================================================================
    # 第三步: 记录扫描到但未买入的机会(事后分析用)
    # ================================================================
    try:
        all_opps = scan_opportunities()
        held_codes = {_pos_obj(p).code for p in state.positions}
        report["skipped_opportunities"] = [
            {
                "code": o["code"], "name": o["name"],
                "price": o.get("current_price", 0),
                "valid": o.get("valid", False),
                "reason": o.get("signal_reason", ""),
            }
            for o in all_opps if o["code"] not in held_codes
        ][:10]
    except Exception:
        report["skipped_opportunities"] = []

    # ================================================================
    # 第四步: 汇总统计
    # ================================================================
    if state.positions:
        codes = [_pos_obj(p).code for p in state.positions]
        quotes = get_realtime(codes)
        mv = sum(
            quotes.get(_pos_obj(p).code, {}).get("price", 0) * _pos_obj(p).shares
            for p in state.positions
        )
    else:
        mv = 0

    report["end_cash"] = round(state.cash, 2)
    report["end_positions"] = len(state.positions)
    report["daily_pnl"] = round(state.daily_pnl, 2)
    report["cumulative_pnl"] = round(state.total_pnl, 2)
    report["cumulative_pnl_pct"] = round(state.total_pnl / CAPITAL * 100, 2)
    report["position_value"] = round(mv, 2)
    report["total_assets"] = round(state.cash + mv, 2)
    report["total_trades"] = state.total_trades
    report["win_trades"] = state.win_trades
    report["win_rate"] = f"{state.win_trades / max(state.total_trades, 1) * 100:.1f}%"

    # 保存策略状态
    state.save()

    # 保存当日报告
    existing_report.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    # 追加交易日志
    trades = []
    if SIM_TRADELOG.exists():
        trades = json.loads(SIM_TRADELOG.read_text())
    for s in report["sells"]:
        trades.append({"date": today, "action": "sell", **s})
    for b in report["buys"]:
        trades.append({"date": today, "action": "buy", **b})
    SIM_TRADELOG.write_text(json.dumps(trades, ensure_ascii=False, indent=2))

    return report


def generate_weekly_report():
    """生成周报告"""
    reports = []
    for f in sorted(SIM_REPORTS.glob("*.json")):
        try:
            reports.append(json.loads(f.read_text()))
        except Exception:
            pass

    if not reports:
        return {"status": "no_data", "message": "尚无模拟盘数据"}

    last = reports[-1]
    return {
        "period": f"{reports[0]['date']} ~ {reports[-1]['date']}",
        "trading_days": len(reports),
        "initial_capital": CAPITAL,
        "final_assets": last.get("total_assets", CAPITAL),
        "total_pnl": last.get("cumulative_pnl", 0),
        "total_pnl_pct": last.get("cumulative_pnl_pct", 0),
        "total_trades": last.get("total_trades", 0),
        "win_trades": last.get("win_trades", 0),
        "win_rate": last.get("win_rate", "0%"),
        "daily_summary": [
            {"date": r["date"], "pnl": r.get("daily_pnl", 0),
             "actions": r.get("actions_summary", [])}
            for r in reports
        ],
    }


if __name__ == "__main__":
    import click

    @click.group()
    def cli():
        pass

    @cli.command()
    def run():
        """执行一个完整交易日"""
        result = execute_daily_run()
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))

    @cli.command()
    def report():
        """查看累计报告"""
        result = generate_weekly_report()
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))

    @cli.command()
    def init():
        """重置模拟盘"""
        result = init_sim()
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))

    cli()
