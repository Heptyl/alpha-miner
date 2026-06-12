"""每日交易计划生成器 v3 — 金牌交易员视角

核心原则:
  1. 用户真实持仓为重, 不轻易建议全卖
  2. 只有触发止损线才P1紧急卖出
  3. 清仓计划(电广/东方电气)按用户指定策略执行
  4. 新买入必须通过ML+情绪+三问过滤
  5. 用实时行情而非收盘价

数据源: 实时行情(腾讯API) + ML预测 + 情绪周期
"""

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"
PRED_PATH = PROJECT_ROOT / "output" / "ml" / "latest_prediction.json"
PLAN_PATH = PROJECT_ROOT / "output" / "trader" / "daily_plan.json"

# 用户真实持仓
# 持仓配置 — 统一从 portfolio.json 读取（同源）
from src.config.portfolio import get_legacy_portfolio_dict as _get_portfolio, get_cash as _get_cash
PORTFOLIO = _get_portfolio()
CASH = _get_cash()


@dataclass
class TradeAction:
    """一条交易指令"""
    code: str
    name: str
    action: str          # "卖出" or "买入"
    shares: int
    price: float         # 参考价
    reason: str
    priority: int        # 1=紧急(止损) 2=重要 3=普通
    score: float = 0.0
    est_amount: float = 0.0

    def to_dict(self) -> dict:
        return {
            "code": self.code, "name": self.name, "action": self.action,
            "shares": self.shares, "price": round(self.price, 2),
            "reason": self.reason, "priority": self.priority,
            "score": round(self.score, 4), "est_amount": round(self.est_amount, 0),
        }


@dataclass
class DailyPlan:
    """每日交易计划"""
    date: str
    market_phase: str = ""        # 情绪周期阶段
    market_zt_count: int = 0      # 涨停数
    sells: list[TradeAction] = field(default_factory=list)
    buys: list[TradeAction] = field(default_factory=list)
    holds: list[dict] = field(default_factory=list)
    cash_before: float = 0.0
    cash_after: float = 0.0
    notes: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "date": self.date,
            "market_phase": self.market_phase,
            "market_zt_count": self.market_zt_count,
            "sells": [a.to_dict() for a in self.sells],
            "buys": [a.to_dict() for a in self.buys],
            "holds": self.holds,
            "cash_before": round(self.cash_before, 0),
            "cash_after": round(self.cash_after, 0),
            "notes": self.notes,
            "warnings": self.warnings,
            "summary": self.summary(),
        }

    def summary(self) -> str:
        parts = []
        if self.sells:
            parts.append(f"卖出{len(self.sells)}只")
        if self.buys:
            parts.append(f"买入{len(self.buys)}只")
        if not parts:
            parts.append("无操作")
        mkt = f" [{self.market_phase}]" if self.market_phase else ""
        return "、".join(parts) + f" | 现金 ¥{self.cash_before:,.0f} → ¥{self.cash_after:,.0f}" + mkt


def _get_realtime_prices(codes: list[str]) -> dict[str, dict]:
    """获取实时行情(优先), fallback到DB收盘价"""
    quotes = {}
    try:
        from src.trader.realtime_quote import get_realtime
        rt = get_realtime(codes)
        for code, q in rt.items():
            if "error" not in q and q.get("price", 0) > 0:
                quotes[code] = {
                    "price": q["price"],
                    "change_pct": q.get("change_pct_calc", 0),
                    "high": q.get("high", 0),
                    "low": q.get("low", 0),
                    "source": "realtime",
                }
    except Exception:
        pass

    # fallback: 用DB收盘价
    missing = [c for c in codes if c not in quotes]
    if missing and DB_PATH.exists():
        conn = sqlite3.connect(str(DB_PATH))
        placeholders = ",".join("?" * len(missing))
        sql = f"""
            SELECT stock_code, close, change_pct
            FROM daily_price
            WHERE trade_date = (SELECT MAX(trade_date) FROM daily_price)
              AND stock_code IN ({placeholders})
        """
        try:
            rows = conn.execute(sql, missing).fetchall()
            for code, close, chg in rows:
                if close and close > 0:
                    quotes[code] = {"price": close, "change_pct": chg or 0, "source": "db"}
        finally:
            conn.close()

    return quotes


def _get_market_sentiment() -> tuple[str, int]:
    """获取市场情绪周期阶段"""
    zt_count = 0
    if DB_PATH.exists():
        conn = sqlite3.connect(str(DB_PATH))
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM zt_pool "
                "WHERE trade_date = (SELECT MAX(trade_date) FROM zt_pool)"
            ).fetchone()
            zt_count = row[0] if row else 0
        finally:
            conn.close()

    if zt_count < 20:
        return "冰点", zt_count
    elif zt_count < 50:
        return "低迷", zt_count
    elif zt_count < 80:
        return "复苏", zt_count
    else:
        return "高潮", zt_count


def generate_daily_plan(
    prediction: dict | None = None,
    portfolio: dict | None = None,
    cash: float | None = None,
) -> DailyPlan:
    """生成每日交易计划 — 金牌交易员视角

    卖出决策(按优先级):
      P1 止损: 现价 <= 止损线 → 全部卖出(无例外)
      P2 反弹清仓: 电广/东方电气达到目标价 → 按计划卖出
      P3 减仓观察: 接近止损线(5%以内) → 提醒关注

    买入决策:
      - 情绪周期冰点/低迷 → 不开新仓
      - ML候选得分>=0.01
      - 屏蔽科创板/北交所
      - 仓位≤3000/只, 最多3只
    """
    # 数据准备
    if prediction is None:
        if PRED_PATH.exists():
            prediction = json.loads(PRED_PATH.read_text())
        else:
            prediction = {}

    if portfolio is None:
        portfolio = PORTFOLIO
    if cash is None:
        cash = CASH

    today = datetime.now().strftime("%Y-%m-%d")
    if prediction:
        pred_date = prediction.get("date", today)
    else:
        pred_date = today

    # 情绪周期
    phase, zt_count = _get_market_sentiment()

    plan = DailyPlan(
        date=today,
        market_phase=phase,
        market_zt_count=zt_count,
        cash_before=cash,
        cash_after=cash,
    )

    # 获取实时价格
    all_codes = list(portfolio.keys())
    if prediction:
        for item in prediction.get("top7", []):
            if item["code"] not in all_codes:
                all_codes.append(item["code"])
        for item in prediction.get("all_top", [])[:20]:
            if item["code"] not in all_codes:
                all_codes.append(item["code"])

    prices = _get_realtime_prices(all_codes)

    # ===== 卖出决策 =====
    for code, info in portfolio.items():
        q = prices.get(code)
        if not q or q["price"] <= 0:
            continue

        cur_price = q["price"]
        cost = info["cost"]
        shares = info["shares"]
        stop = info.get("stop_loss", 0)
        name = info["name"]
        strategy = info.get("strategy", "")
        plan_text = info.get("plan", "")
        pnl_pct = (cur_price / cost - 1) * 100
        stop_dist = ((cur_price - stop) / stop * 100) if stop > 0 else 999

        # P1: 触发止损线(无例外, 必须卖)
        if stop > 0 and cur_price <= stop:
            plan.sells.append(TradeAction(
                code=code, name=name, action="卖出", shares=shares,
                price=cur_price,
                reason=f"触发止损线¥{stop:.2f}! 当前¥{cur_price:.2f}",
                priority=1, est_amount=cur_price * shares,
            ))
            continue

        # P2: 电广传媒分批清仓策略
        if code == "000917":
            if cur_price >= 10.0:
                plan.sells.append(TradeAction(
                    code=code, name=name, action="卖出", shares=300,
                    price=cur_price,
                    reason=f"电广第三批: 突破10元卖出300股",
                    priority=2, est_amount=cur_price * 300,
                ))
            elif cur_price >= 9.6:
                plan.sells.append(TradeAction(
                    code=code, name=name, action="卖出", shares=300,
                    price=cur_price,
                    reason=f"电广第二批: 反弹到9.6卖出300股",
                    priority=2, est_amount=cur_price * 300,
                ))
            elif cur_price >= 9.3:
                plan.sells.append(TradeAction(
                    code=code, name=name, action="卖出", shares=300,
                    price=cur_price,
                    reason=f"电广第一批: 反弹到9.3卖出300股",
                    priority=2, est_amount=cur_price * 300,
                ))
            else:
                plan.notes.append(f"📌 电广传媒: 等反弹到9.3+再卖(当前¥{cur_price:.2f})")
            continue

        # P2: 东方电气反弹清仓
        if code == "600875":
            if cur_price >= 38.5:
                plan.sells.append(TradeAction(
                    code=code, name=name, action="卖出", shares=shares,
                    price=cur_price,
                    reason=f"东方电气反弹到{cur_price:.2f}, 达到38.5目标清仓",
                    priority=2, est_amount=cur_price * shares,
                ))
            else:
                plan.notes.append(f"📌 东方电气: 等反弹到38.5+清仓(当前¥{cur_price:.2f})")
            continue

        # P3: 接近止损预警(不卖, 只提醒)
        if stop_dist <= 5:
            plan.warnings.append(
                f"⚠️ {name}({code}) 距止损线仅{stop_dist:.1f}%, "
                f"现价¥{cur_price:.2f} 止损¥{stop:.2f}"
            )

    # 卖出按优先级排序
    plan.sells.sort(key=lambda x: x.priority)

    # 计算卖出后现金
    sell_proceeds = sum(a.est_amount for a in plan.sells)
    available_cash = cash + sell_proceeds

    # ===== 买入决策 =====
    # 情绪冰点/低迷不开新仓
    can_buy = phase in ("复苏", "高潮")

    if prediction and can_buy and available_cash > 3000:
        # 稳健策略参数
        max_per_stock = 3000.0
        max_positions = 3

        # 当前持仓数(不算计划卖出的)
        sell_codes = {a.code for a in plan.sells}
        current_hold = len([c for c in portfolio if c not in sell_codes])
        slots = max(0, max_positions - current_hold)

        if slots > 0:
            top_items = prediction.get("top7", prediction.get("all_top", []))

            for item in top_items:
                if len(plan.buys) >= slots:
                    break

                code = item["code"]
                name = item.get("name", "")
                score = item.get("score", 0)

                # 过滤
                if code in portfolio and code not in sell_codes:
                    continue  # 已持仓
                if score < 0.01:
                    continue  # 低于ML门槛
                if code.startswith(("688", "689", "200", "8", "9")):
                    continue  # 科创板/北交所
                if "ST" in name or "st" in name:
                    continue  # ST股

                q = prices.get(code)
                if not q or q["price"] <= 0:
                    continue
                cur_price = q["price"]

                # 计算股数(单只≤3000)
                shares = int(min(max_per_stock, available_cash / max(len(plan.buys) + 1, 1)) / cur_price / 100) * 100
                if shares < 100:
                    continue

                est = cur_price * shares

                plan.buys.append(TradeAction(
                    code=code, name=name, action="买入", shares=shares,
                    price=cur_price,
                    reason=f"ML推荐 得分={score:.4f} | {phase}期可开仓",
                    priority=3, score=score, est_amount=est,
                ))
    elif not can_buy:
        plan.notes.append(f"📊 情绪周期={phase}(涨停{zt_count}只), 不建议开新仓")

    # 计算买入后现金
    buy_cost = sum(a.est_amount for a in plan.buys)
    plan.cash_after = available_cash - buy_cost

    # ===== 持仓状态 =====
    sell_codes = {a.code for a in plan.sells}
    for code, info in portfolio.items():
        q = prices.get(code)
        cur_price = q["price"] if q else info["cost"]
        pnl_pct = (cur_price / info["cost"] - 1) * 100 if info["cost"] > 0 else 0
        change_pct = q.get("change_pct", 0) if q else 0
        sold = code in sell_codes
        stop = info.get("stop_loss", 0)
        stop_dist = ((cur_price - stop) / stop * 100) if stop > 0 else 999

        plan.holds.append({
            "code": code, "name": info["name"], "shares": info["shares"],
            "cost": info["cost"], "price": round(cur_price, 2),
            "pnl_pct": round(pnl_pct, 1), "pnl_amount": round((cur_price - info["cost"]) * info["shares"]),
            "change_pct": round(change_pct, 2),
            "stop_loss": stop, "stop_dist": round(stop_dist, 1),
            "strategy": info.get("strategy", ""), "plan": info.get("plan", ""),
            "sold": sold,
        })

    # ===== 注释 =====
    if plan.cash_after < 0:
        plan.warnings.append(f"⚠ 现金不足! 计划后现金 ¥{plan.cash_after:,.0f}")
    if not plan.sells and not plan.buys:
        plan.notes.append("今日无操作建议 — 持仓观望")
    if plan.sells:
        plan.notes.append(f"卖出回笼 ¥{sell_proceeds:,.0f}")
    if plan.buys:
        plan.notes.append(f"买入花费 ¥{buy_cost:,.0f}")
    if phase in ("冰点", "低迷"):
        plan.notes.append(f"📊 大盘情绪={phase}(涨停{zt_count}只), 以防守为主")
    plan.notes.append(f"📅 ML预测日期: {pred_date}")

    # 保存
    PLAN_PATH.parent.mkdir(parents=True, exist_ok=True)
    PLAN_PATH.write_text(json.dumps(plan.to_dict(), ensure_ascii=False, indent=2))

    return plan
