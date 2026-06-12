"""
分时数据缓存 — 每次daemon扫描存储实时快照, 积累成分时序列

524笔5分钟K线回测结论(baostock, 2025-2026, 涨跌均衡样本):

1. 观察窗口: 5分钟最优, 但改善有限
   - 立即止损: 均亏-2.00%
   - 观察5min+按方向: 均亏-1.71%
   - 10/15/30分钟: 没有额外改善

2. 最有效信号(区分度1.33x, 反转率视角):
   开盘30分内 + 斜率>0.3%/5min → 反转率34.4% vs 弱势26.0%
   强势组均亏-1.42% vs 止损-2.00% (子集改善0.58%)
   综合策略均亏-1.78% vs 全止损-2.00% (整体改善0.22%)
   风险: 65.6%强势跳过后仍继续跌(均亏-2.88%), 13笔亏>5%

3. 无效信号(已删除):
   - 纯方向up/down: 区分度1.28x, 不够
   - 缩量/放量: 没有区分力
   - 触发bar阳线: 区分度1.33x, 辅助效果弱
   - 下午触发: 0%反转率, 必须止损

4. 触发时刻是最大影响因素:
   开盘30分: 27%反转率
   上午中段: 14%
   下午: 0%

规则:
  - 开盘30分钟内触发 + 5分钟斜率>0.3% → 跳过止损
  - 其他时段 → 一律止损
  - 数据不足 → 默认止损(安全第一)

注意: 综合策略整体只改善0.22%/笔。PF=0.08说明信号不稳定。
65.6%跳过止损后仍继续跌(均亏-2.88%), 13笔亏>5%。
策略B(-3%止损)和策略A(-2%止损)共用此逻辑, 策略B场景未单独验证。
"""

import logging
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)

OBSERVE_MINUTES = 5
SLOPE_THRESHOLD = 0.3  # %/5min, 回测验证的最优阈值


class IntradayCache:
    """盘中分时数据缓存
    
    daemon每15秒扫一次 → 5分钟约20个数据点
    """
    
    def __init__(self, max_minutes: int = 30):
        self._data: dict[str, list[tuple]] = defaultdict(list)
        self._max_seconds = max_minutes * 60
        self._last_date: str = ""
    
    def snapshot(self, code: str, price: float, volume: float, open_price: float):
        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        if today != self._last_date:
            self._data.clear()
            self._last_date = today
        ts = now.timestamp()
        self._data[code].append((ts, price, volume, open_price))
        self._cleanup(code, ts)
    
    def _cleanup(self, code: str, now_ts: float):
        cutoff = now_ts - self._max_seconds
        data = self._data[code]
        i = 0
        while i < len(data) and data[i][0] < cutoff:
            i += 1
        if i > 0:
            self._data[code] = data[i:]
    
    def get_series(self, code: str, minutes: int = OBSERVE_MINUTES) -> list[dict]:
        if code not in self._data:
            return []
        now_ts = datetime.now().timestamp()
        cutoff = now_ts - minutes * 60
        result = []
        for ts, price, vol, opn in self._data[code]:
            if ts >= cutoff:
                t = datetime.fromtimestamp(ts).strftime("%H:%M:%S")
                result.append({"ts": ts, "price": price, "volume": vol, "open": opn, "time": t})
        return result
    
    def analyze(self, code: str, minutes: int = OBSERVE_MINUTES) -> dict:
        """分析最近N分钟的盘中走势
        
        524笔回测结论:
        只有"开盘30分钟内 + 斜率>0.3%/5min"才值得跳过止损
        其他情况一律止损
        """
        series = self.get_series(code, minutes)
        
        if len(series) < 5:
            return {
                "slope": 0, "is_strong": False, "n_points": len(series),
                "prices": [], "detail": f"数据不足({len(series)}点<5)",
                "ready": False,
            }
        
        prices = [s["price"] for s in series]
        n = len(prices)
        slope = self._slope(prices)
        
        # 判断: 只有在开盘30分钟内 且 斜率足够强 才算强势
        # 回测: 9:30-10:00触发, 斜率>0.3%/5min → 反转率34.4% vs 弱势26.0%
        now = datetime.now()
        is_early_session = (
            (now.hour == 9 and now.minute >= 30) or 
            (now.hour == 10 and now.minute == 0)
        )
        
        # 524笔回测: 只有开盘30分+斜率>0.3%有效
        is_strong = is_early_session and slope > SLOPE_THRESHOLD
        
        detail_parts = [
            f"斜率={slope:+.3f}%/5min",
            f"{'开盘30分' if is_early_session else '非开盘30分'}",
            f"{n}点",
        ]
        if is_strong:
            detail_parts.append("→跳过止损")
        else:
            detail_parts.append("→止损")
        
        return {
            "slope": round(slope, 4),
            "is_strong": is_strong,
            "is_early_session": is_early_session,
            "n_points": n,
            "prices": [round(p, 2) for p in prices[-10:]],
            "detail": ", ".join(detail_parts),
            "ready": True,
        }
    
    def _slope(self, prices: list[float]) -> float:
        """线性回归斜率(%/5min)"""
        n = len(prices)
        if n < 2:
            return 0
        x_mean = (n - 1) / 2
        y_mean = sum(prices) / n
        num = sum((i - x_mean) * (p - y_mean) for i, p in enumerate(prices))
        den = sum((i - x_mean) ** 2 for i in range(n))
        if den == 0 or y_mean == 0:
            return 0
        # 每15秒斜率 → 转成每5分钟的%
        # 5分钟=20个15秒间隔, 斜率除以y_mean*100得%, 再乘20
        return (num / den) / y_mean * 100 * 20


_cache = IntradayCache()

def get_cache() -> IntradayCache:
    return _cache
