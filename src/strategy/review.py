"""盘后复盘模块 — 对比昨日推荐 vs 实际走势。

每日收盘后运行（建议15:30后），对比：
1. 昨日推荐的5只个股今日实际表现
2. 是否触及买入区间
3. 是否达到目标价
4. 是否触发止损
5. 汇总命中率和盈亏

输出：
- 复盘报告（文本 + JSON）
- 累计统计数据
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class StockReview:
    """单只推荐股的复盘结果。"""

    stock_code: str
    stock_name: str
    signal_level: str

    # 昨日推荐参数
    rec_buy_price: float
    rec_buy_zone_low: float
    rec_buy_zone_high: float
    rec_target: float
    rec_stop_loss: float
    rec_close: float  # 推荐时的收盘价（前一日）

    # 今日实际走势
    today_open: float = 0.0
    today_high: float = 0.0
    today_low: float = 0.0
    today_close: float = 0.0
    today_change_pct: float = 0.0  # 涨跌幅%

    # 命中判定
    hit_buy_zone: bool = False       # 最低价是否触及买入区间
    hit_target: bool = False         # 最高价是否达到目标价
    hit_stop_loss: bool = False      # 最低价是否跌破止损价

    # 模拟盈亏（假设在买入区间中位买入）
    entry_price: float = 0.0
    profit_pct: float = 0.0         # 相对买入价盈亏%
    profit_vs_target: float = 0.0   # 距目标价多少%

    def to_dict(self) -> dict:
        return {
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "signal_level": self.signal_level,
            "rec_buy_price": self.rec_buy_price,
            "rec_target": self.rec_target,
            "rec_stop_loss": self.rec_stop_loss,
            "rec_close": self.rec_close,
            "today_open": self.today_open,
            "today_high": self.today_high,
            "today_low": self.today_low,
            "today_close": self.today_close,
            "today_change_pct": round(self.today_change_pct, 2),
            "hit_buy_zone": self.hit_buy_zone,
            "hit_target": self.hit_target,
            "hit_stop_loss": self.hit_stop_loss,
            "entry_price": self.entry_price,
            "profit_pct": round(self.profit_pct, 2),
        }


@dataclass
class DailyReview:
    """每日复盘报告。"""

    review_date: str          # 复盘日期（今日）
    rec_date: str             # 推荐基于的日期（昨日）
    stocks: list[StockReview] = field(default_factory=list)

    # 汇总
    total: int = 0
    hit_buy_count: int = 0    # 触及买入区间数
    hit_target_count: int = 0 # 达到目标价数
    hit_stop_count: int = 0   # 触发止损数
    avg_profit_pct: float = 0.0
    win_rate: float = 0.0     # 盈利比例

    def to_dict(self) -> dict:
        return {
            "review_date": self.review_date,
            "rec_date": self.rec_date,
            "total": self.total,
            "hit_buy_count": self.hit_buy_count,
            "hit_target_count": self.hit_target_count,
            "hit_stop_count": self.hit_stop_count,
            "avg_profit_pct": round(self.avg_profit_pct, 2),
            "win_rate": round(self.win_rate, 2),
            "stocks": [s.to_dict() for s in self.stocks],
        }

    def to_text(self) -> str:
        lines = []
        lines.append("=" * 60)
        lines.append(f"  Alpha Miner 盘后复盘 — {self.review_date}")
        lines.append(f"  回顾 {self.rec_date} 的推荐")
        lines.append("=" * 60)

        if not self.stocks:
            lines.append("\n  无推荐记录")
        else:
            lines.append(f"\n  总推荐: {self.total} 只")
            lines.append(f"  触及买点: {self.hit_buy_count}/{self.total}")
            lines.append(f"  达到目标: {self.hit_target_count}/{self.total}")
            lines.append(f"  触发止损: {self.hit_stop_count}/{self.total}")
            lines.append(f"  平均盈亏: {self.avg_profit_pct:+.2f}%")
            lines.append(f"  盈利比例: {self.win_rate:.0f}%")

            lines.append(f"\n  {'─'*56}")
            for i, s in enumerate(self.stocks, 1):
                # 状态标记
                if s.hit_target:
                    status = "🎯命中目标"
                elif s.hit_stop_loss:
                    status = "🛑触发止损"
                elif s.profit_pct > 0:
                    status = f"📈+{s.profit_pct:.1f}%"
                else:
                    status = f"📉{s.profit_pct:.1f}%"

                buy_hit = "✅" if s.hit_buy_zone else "❌"

                lines.append(f"\n  {i}. [{s.signal_level}] {s.stock_code} {s.stock_name} {status}")
                lines.append(f"     推荐: 买{s.rec_buy_price:.2f} 目标{s.rec_target:.2f} 止损{s.rec_stop_loss:.2f}")
                lines.append(f"     今日: 开{s.today_open:.2f} 高{s.today_high:.2f} 低{s.today_low:.2f} 收{s.today_close:.2f} ({s.today_change_pct:+.1f}%)")
                lines.append(f"     触及买点: {buy_hit} | 模拟盈亏: {s.profit_pct:+.2f}%")

        lines.append(f"\n  ⚠ 复盘基于收盘价对比，仅供参考")
        lines.append("=" * 60)
        return "\n".join(lines)


def run_review(
    review_date: str,
    db_path: str = "data/alpha_miner.db",
    rec_dir: str = "recommendations",
) -> Optional[DailyReview]:
    """运行盘后复盘。

    Args:
        review_date: 复盘日期 YYYY-MM-DD（今日）
        db_path: 数据库路径
        rec_dir: 推荐文件目录

    Returns:
        DailyReview 或 None（无推荐记录时）
    """
    # 确定推荐日期 = review_date 的前一个交易日
    # 从推荐文件名中找
    rec_path = Path(rec_dir)

    # review_date 是今日，推荐基于昨日数据
    # 尝试找 review_date-1 或 review_date-2 的推荐文件
    # 因为推荐文件名是 推荐日期_recommend.json
    review_dt = datetime.strptime(review_date, "%Y-%m-%d")

    rec_json = None
    rec_date = None
    for delta in range(1, 5):
        candidate = review_dt - timedelta(days=delta)
        candidate_str = candidate.strftime("%Y-%m-%d")
        f = rec_path / f"{candidate_str}_recommend.json"
        if f.exists():
            rec_json = f
            rec_date = candidate_str
            break

    if rec_json is None:
        logger.warning("找不到 %s 之前的推荐文件", review_date)
        return None

    # 加载推荐
    with open(rec_json, "r", encoding="utf-8") as f:
        rec_data = json.load(f)

    rec_stocks = rec_data.get("stocks", [])
    if not rec_stocks:
        logger.warning("%s 推荐列表为空", rec_date)
        return None

    # 从数据库加载今日实际走势
    conn = sqlite3.connect(db_path)

    reviews = []
    for s in rec_stocks:
        code = s["stock_code"]
        # 今日K线
        row = conn.execute(
            "SELECT open, high, low, close FROM daily_price "
            "WHERE trade_date = ? AND stock_code = ?",
            (review_date, code),
        ).fetchone()

        if row is None:
            # 今日数据还没入库
            logger.warning("%s 无 %s K线数据，跳过", code, review_date)
            continue

        today_open, today_high, today_low, today_close = row

        # 昨日收盘（推荐时的收盘价）
        prev_row = conn.execute(
            "SELECT close FROM daily_price "
            "WHERE trade_date = ? AND stock_code = ?",
            (rec_date, code),
        ).fetchone()
        prev_close = prev_row[0] if prev_row else s.get("technical", {}).get("current_price", 0)

        # 涨跌幅
        change_pct = ((today_close - prev_close) / prev_close * 100) if prev_close > 0 else 0

        # 推荐参数
        rec_buy = s.get("buy_price", 0)
        rec_buy_low = s.get("buy_zone_low", 0)
        rec_buy_high = s.get("buy_zone_high", 0)
        rec_target = s.get("target_price", 0)
        rec_stop = s.get("stop_loss", 0)

        # 命中判定
        hit_buy = today_low <= rec_buy_high and today_high >= rec_buy_low
        hit_target = today_high >= rec_target if rec_target > 0 else False
        hit_stop = today_low <= rec_stop if rec_stop > 0 else False

        # 模拟买入价 = 买入区间中位，如果触及买入区间
        if hit_buy:
            # 假设在区间中位买入
            entry = (rec_buy_low + rec_buy_high) / 2
        else:
            # 没触及买点，按昨日收盘算浮盈浮亏（实际不会买入）
            entry = prev_close

        profit_pct = ((today_close - entry) / entry * 100) if entry > 0 else 0

        sr = StockReview(
            stock_code=code,
            stock_name=s.get("stock_name", ""),
            signal_level=s.get("signal_level", ""),
            rec_buy_price=rec_buy,
            rec_buy_zone_low=rec_buy_low,
            rec_buy_zone_high=rec_buy_high,
            rec_target=rec_target,
            rec_stop_loss=rec_stop,
            rec_close=prev_close,
            today_open=today_open,
            today_high=today_high,
            today_low=today_low,
            today_close=today_close,
            today_change_pct=change_pct,
            hit_buy_zone=hit_buy,
            hit_target=hit_target,
            hit_stop_loss=hit_stop,
            entry_price=round(entry, 2),
            profit_pct=round(profit_pct, 2),
        )
        reviews.append(sr)

    conn.close()

    if not reviews:
        return None

    # 汇总
    total = len(reviews)
    hit_buy_count = sum(1 for r in reviews if r.hit_buy_zone)
    hit_target_count = sum(1 for r in reviews if r.hit_target)
    hit_stop_count = sum(1 for r in reviews if r.hit_stop_loss)
    profits = [r.profit_pct for r in reviews if r.hit_buy_zone]
    avg_profit = float(np.mean(profits)) if profits else 0.0
    # 只统计触及买点的胜率
    wins = sum(1 for p in profits if p > 0)
    win_rate = (wins / len(profits) * 100) if profits else 0.0

    result = DailyReview(
        review_date=review_date,
        rec_date=rec_date,
        stocks=reviews,
        total=total,
        hit_buy_count=hit_buy_count,
        hit_target_count=hit_target_count,
        hit_stop_count=hit_stop_count,
        avg_profit_pct=round(avg_profit, 2),
        win_rate=round(win_rate, 1),
    )

    return result


def format_review_wechat(review: DailyReview) -> str:
    """格式化复盘微信消息。"""
    lines = []
    lines.append("📊 Alpha Miner 盘后复盘")
    lines.append(f"📅 {review.review_date} | 回顾 {review.rec_date} 推荐")
    lines.append("")

    lines.append(f"📈 汇总: {review.total}只推荐")
    lines.append(f"  触及买点: {review.hit_buy_count}/{review.total}")
    lines.append(f"  命中目标: {review.hit_target_count}/{review.total}")
    lines.append(f"  触发止损: {review.hit_stop_count}/{review.total}")
    if review.hit_buy_count > 0:
        lines.append(f"  平均盈亏: {review.avg_profit_pct:+.2f}%")
        lines.append(f"  胜率: {review.win_rate:.0f}%")
    lines.append("")

    for i, s in enumerate(review.stocks, 1):
        if s.hit_target:
            status = "🎯达标"
        elif s.hit_stop_loss:
            status = "🛑止损"
        elif s.profit_pct > 0:
            status = f"📈+{s.profit_pct:.1f}%"
        else:
            status = f"📉{s.profit_pct:.1f}%"

        buy_icon = "✅" if s.hit_buy_zone else "❌"
        lines.append(
            f"{i}. {status} {s.stock_code} {s.stock_name}\n"
            f"   收{s.today_close:.2f}({s.today_change_pct:+.1f}%) "
            f"买点{buy_icon} 盈亏{s.profit_pct:+.1f}%"
        )

    lines.append("")
    lines.append("⚠ 仅供参考")
    return "\n".join(lines)
