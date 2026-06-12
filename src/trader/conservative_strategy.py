"""[已废弃] 稳健实盘策略 — 已被trading_daemon.py三策略体系替代

基于30天模拟盘数据分析的结论:
  - ML信号(0.01-0.02)胜率68%，9维高分(>0.80)胜率33%
  - A级推荐胜率75%，B级67%
  - 止损-5%太紧被洗出，持有5天胜率78%
  - 不追涨停，等回踩买入

规则:
  [已废弃] 已被trading_daemon.py三策略体系替代
  选股: ML信号+IC因子(策略A) / 涨停确认(策略B) / 反弹低吸(策略C)
  仓位: A/B 15%/只 C 10%/只, 最多6只(A/B:4+C:2)
  止损: A/B -8% / C -6%
  持有: A 7天 / B 5天 / C 1天
  日限: 单日亏>1000元停手(总资金2%)
"""

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional

import sys
sys.path.insert(0, ".")
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.trader.realtime_quote import get_realtime


# === 配置 ===
CAPITAL = 50000               # 总资金(已废弃,实际用trading_daemon.py)
MAX_POSITION_PCT = 0.15       # 单只仓位上限15%
MAX_POSITIONS = 6                 # 最多6只(A/B:4+C:2)
STOP_LOSS_PCT = -0.08         # 止损-8%
TAKE_PROFIT_PCT = 0.10        # 止盈+10%
TRAILING_STOP_PCT = 0.05      # 移动止盈(从高点回落5%)
MAX_HOLD_DAYS = 5             # 最长持有5天
DAILY_LOSS_LIMIT = 1000               # 日限亏1000元(5万的2%)
MIN_BUY_DIP_PCT = -0.02       # 至少回踩2%才买入(不追高)

# 屏蔽: 科创板(688/689)和北交所(8/9开头) — 资金不足50万无购买资格
BLOCKED_SECTORS = []
BLOCKED_CODES_PREFIX = ["688", "689", "8", "9", "4"]  # 科创板/北交所

# 状态文件
STATE_FILE = Path("output/trader/conservative_state.json")


@dataclass
class Position:
    code: str
    name: str
    buy_price: float
    shares: int
    buy_date: str
    highest_price: float  # 用于移动止盈
    source: str  # "ml" / "recommend_a"
    stop_loss: float = 0.0
    take_profit: float = 0.0

    def __post_init__(self):
        if self.stop_loss == 0:
            self.stop_loss = round(self.buy_price * (1 + STOP_LOSS_PCT), 2)
        if self.take_profit == 0:
            self.take_profit = round(self.buy_price * (1 + TAKE_PROFIT_PCT), 2)


@dataclass
class TradeRecord:
    code: str
    name: str
    action: str  # "buy" / "sell"
    price: float
    shares: int
    time: str
    source: str
    reason: str
    pnl: float = 0.0


@dataclass
class ConservativeState:
    cash: float = CAPITAL
    positions: list = field(default_factory=list)  # list of Position dicts
    trade_history: list = field(default_factory=list)  # list of TradeRecord dicts
    daily_pnl: float = 0.0
    daily_pnl_date: str = ""
    created_at: str = ""
    total_pnl: float = 0.0
    total_trades: int = 0
    win_trades: int = 0

    def save(self):
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "cash": self.cash,
            "positions": [asdict(p) if hasattr(p, '__dataclass_fields__') else p for p in self.positions],
            "trade_history": [asdict(t) if hasattr(t, '__dataclass_fields__') else t for t in self.trade_history],
            "daily_pnl": self.daily_pnl,
            "daily_pnl_date": self.daily_pnl_date,
            "created_at": self.created_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_pnl": self.total_pnl,
            "total_trades": self.total_trades,
            "win_trades": self.win_trades,
        }
        STATE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2))

    @classmethod
    def load(cls):
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text())
            positions = []
            for p in data.get("positions", []):
                positions.append(Position(**p))
            data["positions"] = positions
            history = []
            for t in data.get("trade_history", []):
                history.append(TradeRecord(**t))
            data["trade_history"] = history
            return cls(**{k: v for k, v in data.items()
                       if k in cls.__annotations__})
        return cls()


def filter_candidate(code: str, name: str, sector: str = "") -> tuple[bool, str]:
    """过滤候选股，返回(是否通过, 原因)"""
    for prefix in BLOCKED_CODES_PREFIX:
        if code.startswith(prefix):
            return False, f"代码前缀{prefix}被屏蔽"
    if "ST" in name:
        return False, "ST股不交易"
    if sector in BLOCKED_SECTORS:
        return False, f"{sector}板块被屏蔽(历史亏损)"
    return True, "通过"


def check_buy_signal(code: str, name: str, current_price: float, prev_close: float,
                     ml_score: float = 0, signal_level: str = "",
                     sector: str = "") -> tuple[bool, str]:
    """检查买入信号是否符合规则"""
    # 基本过滤
    ok, reason = filter_candidate(code, name, sector)
    if not ok:
        return False, reason

    # 信号来源检查: ML分数(0.005-0.02)或A级推荐
    is_ml = ml_score > 0.005 and ml_score < 0.02
    is_a_level = signal_level == "A"
    if not (is_ml or is_a_level):
        return False, f"信号不符合(ML={ml_score:.4f}, 等级={signal_level})，只接受ML(0.005-0.02)或A级推荐"

    # 不追高: 当日涨幅超过5%不买
    chg_pct = (current_price / prev_close - 1) * 100 if prev_close > 0 else 0
    if chg_pct > 5:
        return False, f"当日涨幅{chg_pct:+.1f}%，追高风险"
    if chg_pct > 9:
        return False, f"涨停附近，无法买入"

    # 等回踩: 至少从高点回踩2%
    if chg_pct > 2:
        return False, f"当日涨幅{chg_pct:+.1f}%，等回踩至少{MIN_BUY_DIP_PCT*100:.0f}%"

    return True, f"信号通过(ML={ml_score:.4f}, 等级={signal_level})"


def calc_buy_amount(price: float, cash: float, current_positions: int) -> tuple[int, float]:
    """计算可买股数和金额"""
    if current_positions >= MAX_POSITIONS:
        return 0, 0
    max_amount = CAPITAL * MAX_POSITION_PCT
    shares = int(min(max_amount, cash) / price / 100) * 100
    if shares < 100:
        return 0, 0
    amount = shares * price
    return shares, amount


def check_sell_signal(pos: Position, current_price: float, hold_days: int) -> tuple[bool, str]:
    """检查卖出信号"""
    pnl_pct = (current_price / pos.buy_price - 1)

    # 止损
    if pnl_pct <= STOP_LOSS_PCT:
        return True, f"触发止损{STOP_LOSS_PCT*100:.0f}%，当前{pnl_pct*100:+.1f}%"

    # 止盈
    if pnl_pct >= TAKE_PROFIT_PCT:
        return True, f"触发止盈{TAKE_PROFIT_PCT*100:.0f}%，当前{pnl_pct*100:+.1f}%"

    # 移动止盈: 从最高价回落5%
    if current_price < pos.highest_price * (1 - TRAILING_STOP_PCT):
        return True, f"移动止盈(从高点{pos.highest_price:.2f}回落{TRAILING_STOP_PCT*100:.0f}%)"

    # 到期卖出
    if hold_days >= MAX_HOLD_DAYS:
        return True, f"持有{hold_days}天到期"

    return False, f"继续持有({pnl_pct*100:+.1f}%，距止损{((current_price/pos.stop_loss-1)*100):+.1f}%)"


def scan_opportunities():
    """扫描当前所有候选，返回符合规则的买入机会"""
    opportunities = []

    # 1. ML候选
    ml_file = Path("output/ml/latest_prediction.json")
    if ml_file.exists():
        ml_data = json.loads(ml_file.read_text())
        preds = ml_data.get("top7", ml_data.get("predictions", []))[:10]
        for p in preds:
            code = p["code"]
            name = p.get("name", code)
            score = p.get("score", 0)
            ok, reason = filter_candidate(code, name)
            if ok:
                opportunities.append({
                    "code": code, "name": name,
                    "source": "ml", "score": score,
                    "reason": f"ML预测分{score:.4f}",
                })

    # 2. A级推荐
    rec_dir = Path("recommendations")
    for f in sorted(rec_dir.glob("*_recommend_v2.json"), reverse=True)[:2]:
        try:
            rec = json.loads(f.read_text(encoding="utf-8"))
            for s in rec.get("stocks", []):
                level = s.get("signal_level", "")
                if level != "A":
                    continue
                code = s["stock_code"]
                name = s["stock_name"]
                sector = s.get("industry", "")
                ok, reason = filter_candidate(code, name, sector)
                if ok:
                    opportunities.append({
                        "code": code, "name": name,
                        "source": "recommend_a", "score": s.get("composite_score", 0),
                        "reason": f"A级推荐 综合{s.get('composite_score', 0):.3f}",
                        "buy_price": s.get("buy_price", 0),
                        "stop_price": s.get("stop_loss", 0),
                        "target_price": s.get("target_price", 0),
                    })
        except Exception:
            pass

    # 获取实时行情
    if opportunities:
        codes = [o["code"] for o in opportunities]
        quotes = get_realtime(codes)
        if quotes and "error" not in quotes:
            for opp in opportunities:
                q = quotes.get(opp["code"], {})
                opp["current_price"] = q.get("price", 0)
                opp["prev_close"] = q.get("yesterday_close", 0)
                opp["change_pct"] = q.get("change_pct_calc", 0)

    # 过滤
    valid = []
    for opp in opportunities:
        price = opp.get("current_price", 0)
        prev = opp.get("prev_close", 0)
        if price <= 0:
            continue
        ok, reason = check_buy_signal(
            opp["code"], opp["name"], price, prev,
            ml_score=opp.get("score", 0) if opp["source"] == "ml" else 0,
            signal_level="A" if opp["source"] == "recommend_a" else "",
            sector=opp.get("sector", ""),
        )
        opp["valid"] = ok
        opp["signal_reason"] = reason
        if ok:
            valid.append(opp)

    return valid


def generate_trade_plan():
    """生成今日操作计划"""
    state = ConservativeState.load()
    today = datetime.now().strftime("%Y-%m-%d")

    # 重置日损
    if state.daily_pnl_date != today:
        state.daily_pnl = 0.0
        state.daily_pnl_date = today

    plan = {
        "date": today,
        "capital": CAPITAL,
        "cash": state.cash,
        "positions": len(state.positions),
        "actions": [],
    }

    # 1. 检查持仓卖出信号
    if state.positions:
        codes = [p.code if hasattr(p, 'code') else p["code"] for p in state.positions]
        quotes = get_realtime(codes)

        for pos in state.positions:
            p = pos if hasattr(pos, 'code') else Position(**pos)
            q = quotes.get(p.code, {})
            price = q.get("price", 0)
            if price <= 0:
                continue

            buy_date = datetime.strptime(p.buy_date, "%Y-%m-%d")
            hold_days = (datetime.now() - buy_date).days

            sell, reason = check_sell_signal(p, price, hold_days)
            plan["actions"].append({
                "type": "hold" if not sell else "sell",
                "code": p.code,
                "name": p.name,
                "price": price,
                "shares": p.shares,
                "pnl_pct": (price / p.buy_price - 1) * 100,
                "hold_days": hold_days,
                "reason": reason,
                "stop_loss": p.stop_loss,
                "take_profit": p.take_profit,
            })

    # 2. 检查买入机会
    if len(state.positions) < MAX_POSITIONS and state.daily_pnl > -DAILY_LOSS_LIMIT:
        opportunities = scan_opportunities()
        for opp in opportunities:
            shares, amount = calc_buy_amount(opp["current_price"], state.cash, len(state.positions))
            if shares > 0:
                plan["actions"].append({
                    "type": "buy",
                    "code": opp["code"],
                    "name": opp["name"],
                    "price": opp["current_price"],
                    "shares": shares,
                    "amount": amount,
                    "source": opp["source"],
                    "reason": opp["signal_reason"],
                })

    return plan


if __name__ == "__main__":
    import click
    @click.group()
    def cli():
        pass

    @cli.command()
    def plan():
        """生成今日操作计划"""
        p = generate_trade_plan()
        click.echo(json.dumps(p, ensure_ascii=False, indent=2))

    @cli.command()
    def status():
        """查看当前状态"""
        state = ConservativeState.load()
        click.echo(json.dumps({
            "cash": state.cash,
            "positions": [asdict(p) if hasattr(p, '__dataclass_fields__') else p for p in state.positions],
            "daily_pnl": state.daily_pnl,
            "total_pnl": state.total_pnl,
            "total_trades": state.total_trades,
            "win_trades": state.win_trades,
            "win_rate": f"{state.win_trades/max(state.total_trades,1)*100:.1f}%",
        }, ensure_ascii=False, indent=2))

    @cli.command()
    def scan():
        """扫描买入机会"""
        opps = scan_opportunities()
        for o in opps:
            icon = "✅" if o.get("valid") else "❌"
            click.echo(f"{icon} {o['code']} {o['name']} {o.get('current_price', 0):.2f} {o.get('signal_reason', '')}")

    cli()
