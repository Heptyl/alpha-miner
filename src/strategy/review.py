"""盘后复盘模块 — 对比前一日推荐与当日实际走势。

读取前一交易日的推荐报告，用当日行情验证：
  - 是否触及买点
  - 是否触及目标价
  - 是否触及止损
  - 实际盈亏
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional


@dataclass
class ReviewStock:
    """单只推荐股的复盘结果。"""
    stock_code: str
    stock_name: str
    signal_level: str
    rec_buy_price: float
    rec_target: float
    rec_stop_loss: float
    rec_close: float = 0.0
    today_open: float = 0.0
    today_high: float = 0.0
    today_low: float = 0.0
    today_close: float = 0.0
    today_change_pct: float = 0.0
    hit_buy_zone: bool = False
    hit_target: bool = False
    hit_stop_loss: bool = False
    entry_price: float = 0.0
    profit_pct: float = 0.0


@dataclass
class ReviewResult:
    """复盘结果汇总。"""
    review_date: str
    rec_date: str
    total: int = 0
    hit_buy_count: int = 0
    hit_target_count: int = 0
    hit_stop_count: int = 0
    avg_profit_pct: float = 0.0
    win_rate: float = 0.0
    stocks: List[ReviewStock] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "review_date": self.review_date,
            "rec_date": self.rec_date,
            "total": self.total,
            "hit_buy_count": self.hit_buy_count,
            "hit_target_count": self.hit_target_count,
            "hit_stop_count": self.hit_stop_count,
            "avg_profit_pct": round(self.avg_profit_pct, 2),
            "win_rate": round(self.win_rate, 1),
            "stocks": [
                {
                    "stock_code": s.stock_code,
                    "stock_name": s.stock_name,
                    "signal_level": s.signal_level,
                    "rec_buy_price": round(s.rec_buy_price, 2),
                    "rec_target": round(s.rec_target, 2),
                    "rec_stop_loss": round(s.rec_stop_loss, 2),
                    "rec_close": round(s.rec_close, 2),
                    "today_open": round(s.today_open, 2),
                    "today_high": round(s.today_high, 2),
                    "today_low": round(s.today_low, 2),
                    "today_close": round(s.today_close, 2),
                    "today_change_pct": round(s.today_change_pct, 2),
                    "hit_buy_zone": s.hit_buy_zone,
                    "hit_target": s.hit_target,
                    "hit_stop_loss": s.hit_stop_loss,
                    "entry_price": round(s.entry_price, 2),
                    "profit_pct": round(s.profit_pct, 2),
                }
                for s in self.stocks
            ],
        }


def _find_prev_trading_day(date_str: str, db_path: str) -> Optional[str]:
    """查找前一个有推荐的交易日。"""
    # 在 recommendations 目录中查找前一个推荐文件
    rec_dir = Path("recommendations")
    dt = datetime.strptime(date_str, "%Y-%m-%d")

    for _ in range(10):  # 最多回溯10天
        dt -= timedelta(days=1)
        d = dt.strftime("%Y-%m-%d")
        # 检查多种推荐文件格式
        for pattern in [f"{d}_recommend.json", f"{d}_recommend_v2.json", f"{d}.txt"]:
            if (rec_dir / pattern).exists():
                return d
    return None


def _load_recommendations(rec_date: str) -> List[dict]:
    """加载指定日期的推荐报告。"""
    rec_dir = Path("recommendations")

    # 优先 v2 json
    v2_path = rec_dir / f"{rec_date}_recommend_v2.json"
    if v2_path.exists():
        data = json.loads(v2_path.read_text(encoding="utf-8"))
        return _extract_stocks_from_json(data)

    # 普通 json
    json_path = rec_dir / f"{rec_date}_recommend.json"
    if json_path.exists():
        data = json.loads(json_path.read_text(encoding="utf-8"))
        return _extract_stocks_from_json(data)

    # 纯文本（无法解析）
    return []


def _extract_stocks_from_json(data: dict) -> List[dict]:
    """从推荐 JSON 中提取股票列表。"""
    if "stocks" in data:
        stocks = data["stocks"]
        if stocks and isinstance(stocks[0], dict):
            return stocks
    if "recommendations" in data:
        return data["recommendations"]
    return []


def _get_today_price(stock_code: str, review_date: str, db_path: str) -> dict:
    """从数据库获取当日行情。"""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            "SELECT open, high, low, close, pct_chg "
            "FROM daily_price WHERE code = ? AND date = ?",
            (stock_code, review_date),
        )
        row = cur.fetchone()
        conn.close()
        if row:
            return {
                "open": row["open"] or 0.0,
                "high": row["high"] or 0.0,
                "low": row["low"] or 0.0,
                "close": row["close"] or 0.0,
                "pct_chg": row["pct_chg"] or 0.0,
            }
    except Exception:
        pass
    return {"open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0, "pct_chg": 0.0}


def run_review(report_date: str, db_path: str = "data/alpha_miner.db") -> Optional[ReviewResult]:
    """执行复盘：对比推荐日和复盘日的行情。

    report_date: 复盘日期（今天）
    返回: ReviewResult 或 None（无前一日推荐）
    """
    # 查找前一个推荐日
    rec_date = _find_prev_trading_day(report_date, db_path)
    if not rec_date:
        return None

    # 加载推荐
    rec_stocks = _load_recommendations(rec_date)
    if not rec_stocks:
        return None

    result = ReviewResult(
        review_date=report_date,
        rec_date=rec_date,
    )

    for s in rec_stocks:
        code = s.get("code") or s.get("stock_code", "")
        name = s.get("name") or s.get("stock_name", "")
        level = s.get("signal_level", "C")
        buy_price = float(s.get("buy_price") or s.get("rec_buy_price") or 0)
        target = float(s.get("target_price") or s.get("rec_target") or 0)
        stop_loss = float(s.get("stop_loss") or s.get("rec_stop_loss") or 0)
        rec_close = float(s.get("close") or s.get("rec_close") or 0)

        # 获取当日行情
        price = _get_today_price(code, report_date, db_path)

        # 判断触及情况
        hit_buy = False
        hit_target = False
        hit_stop = False
        entry_price = 0.0
        profit_pct = 0.0

        if buy_price > 0 and price["low"] > 0:
            # 触及买点：最低价 <= 推荐买价 * 1.02（略高2%也算触及）
            hit_buy = price["low"] <= buy_price * 1.02
            if hit_buy:
                entry_price = min(price["open"], buy_price * 1.01)

            # 触及目标
            if target > 0:
                hit_target = price["high"] >= target

            # 触及止损
            if stop_loss > 0:
                hit_stop = price["low"] <= stop_loss

            # 计算盈亏
            if entry_price > 0 and price["close"] > 0:
                profit_pct = (price["close"] / entry_price - 1) * 100

        rs = ReviewStock(
            stock_code=code,
            stock_name=name,
            signal_level=level,
            rec_buy_price=buy_price,
            rec_target=target,
            rec_stop_loss=stop_loss,
            rec_close=rec_close,
            today_open=price["open"],
            today_high=price["high"],
            today_low=price["low"],
            today_close=price["close"],
            today_change_pct=price["pct_chg"],
            hit_buy_zone=hit_buy,
            hit_target=hit_target,
            hit_stop_loss=hit_stop,
            entry_price=entry_price,
            profit_pct=profit_pct,
        )
        result.stocks.append(rs)

    # 汇总统计
    result.total = len(result.stocks)
    result.hit_buy_count = sum(1 for s in result.stocks if s.hit_buy_zone)
    result.hit_target_count = sum(1 for s in result.stocks if s.hit_target)
    result.hit_stop_count = sum(1 for s in result.stocks if s.hit_stop_loss)

    if result.stocks:
        result.avg_profit_pct = sum(s.profit_pct for s in result.stocks) / len(result.stocks)
        wins = sum(1 for s in result.stocks if s.profit_pct > 0)
        result.win_rate = wins / len(result.stocks) * 100

    return result


def format_review_wechat(review: ReviewResult) -> str:
    """格式化微信推送消息。"""
    lines = [
        f"📊 【复盘 {review.rec_date}→{review.review_date}】",
        f"推荐 {review.total} 只 | "
        f"触及买点 {review.hit_buy_count}/{review.total} | "
        f"胜率 {review.win_rate:.0f}% | "
        f"均盈 {review.avg_profit_pct:+.2f}%",
        "",
    ]
    for i, s in enumerate(review.stocks, 1):
        icon = "🟢" if s.profit_pct > 0 else "🔴" if s.profit_pct < 0 else "⚪"
        buy_icon = "✅" if s.hit_buy_zone else "❌"
        lines.append(
            f"{i}. {icon} {s.stock_name}({s.stock_code}) "
            f"{s.profit_pct:+.1f}% "
            f"买{buy_icon} 收{s.today_close:.2f}"
        )
    return "\n".join(lines)
