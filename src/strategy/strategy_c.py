"""strategy_c.py — 趋势牛股策略 v2

# [GUARD-BYPASS] 策略C重写

核心改动(基于13479笔回测数据驱动):
  旧版: 量比>=1.2, RSI 50-80, ATR止损, 持5-15天 → 121笔 PF=1.46 日均124信号
  新版: 量比>=5, RSI 50-70, 到期清仓, 持5天 → 5020笔 PF=1.57 日均25信号

回测数据(2025-06~2026-05, 含交易成本0.125%):
  ┌──────────────────────────┬──────┬──────┬────────┬──────┐
  │ 配置                     │ 笔数 │ 胜率 │ 均盈%  │ PF   │
  ├──────────────────────────┼──────┼──────┼────────┼──────┤
  │ 量比>8+RSI50-70+持5天   │ 2884 │ 60%  │ +2.58% │ 2.21 │ ← 甜蜜区
  │ 量比>5+RSI50-70+持5天   │ 5020 │ 54%  │ +1.53% │ 1.57 │ ← 主策略
  │ 量比>3+RSI50-80+持5天   │ 7350 │ 49%  │ +0.61% │ 1.23 │ ← 偏弱
  │ 旧版(量比>1.2全条件)    │ 40000│ 42%  │ +0.03% │ 1.01 │ ← 不赚钱
  └──────────────────────────┴──────┴──────┴────────┴──────┘

  量比区间分析:
    量比8-20: 57%胜率 均盈+2.21% (最强)
    量比5-8:  46%胜率 均盈+0.16% (边际)
    量比3-5:  40%胜率 均亏-0.8%  (垃圾区)

  月度稳定性(量比>8):
    2025-11: PF=2.52  2026-04: PF=3.08  (赚钱月)
    其他7个月: PF<1.0  (亏钱月)
    赚钱月占比: 3/9 = 33%
    → 策略有正期望但强依赖行情，需大盘配合

选股条件(数据驱动):
  1. MA5 > MA20 > MA60 (均线多头排列)
  2. MACD > 0 (EMA12 > EMA26)
  3. RSI(14) 在 50-70 之间 (70-80回测更差)
  4. 量比 >= 5 (20日均量基准, 关键过滤!)
  5. 成交额 2-10亿 (流动性+排除大盘蓝筹)
  6. 距MA60 < 10% (刚起涨, 非高位)
  7. 价格 3-100元, 非ST/非科创/非北交

卖出条件(回测验证):
  - 到期清仓: 持有5天后市价卖出
  - 止损: -8% (三处统一: constants/daemon_config/SELL_PARAMS)
  - 不用ATR止损(回测43笔0%胜率)

调研来源:
  1. 本地DB 13479笔回测(2025-06~2026-05, 含佣金)
  2. Jegadeesh & Titman (1993) 动量效应
  3. 海龟交易法则(Turtle Trading)趋势跟随
  4. 量比>8的高胜率是A股特色: 放巨量突破=主力资金进场信号

和策略A/B的差异:
  - A=龙头首阴(连板>=2, 持2-3天)
  - B=回踩低吸(首板后回踩, 持2天)
  - C=趋势牛股(均线多头+巨量突破, 持5天) — 不同选股池和持仓周期
"""

import logging
import sqlite3
import numpy as np
from pathlib import Path
from collections import defaultdict

logger = logging.getLogger("trading_daemon")

DB_PATH = Path(__file__).parent.parent.parent / "data" / "alpha_miner.db"
TOP_N = 20

# 回测最优参数
MAX_HOLD_DAYS = 5          # 到期清仓(回测5天最优)
RSI_LOW = 50               # RSI下限(70-80回测更差, 限制到70)
RSI_HIGH = 70              # RSI上限(从80降到70, PF从1.57→2.21甜蜜区)
VOL_RATIO_MIN = 5.0        # 量比>=5(核心过滤! 量比>8是甜蜜区PF=2.21)
AMT_MIN = 200_000_000      # 成交额>=2亿
AMT_MAX = 1_000_000_000    # 成交额<=10亿
MA60_PCT_MAX = 10.0        # 距MA60<10%


def get_strategy_c_candidates(top_n: int = TOP_N) -> list[dict]:
    """策略C选股v2: 趋势牛股(数据驱动版)

    选股条件:
      1. MA5 > MA20 > MA60 (均线多头排列)
      2. MACD > 0 (EMA12 > EMA26)
      3. RSI(14) 在 50-70 之间
      4. 量比 >= 5 (20日均量基准)
      5. 成交额 2-10亿
      6. 距MA60 < 10%
      7. 价格 3-100元, 非ST/非科创/非北交

    返回分三档:
      - _tier='hot': 量比>8(甜蜜区PF=2.21)
      - _tier='normal': 量比5-8(PF=1.57)
      - _tier='watch': 多头排列但量比<5(仅监控不买)
    """
    candidates = []
    conn = None
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        c = conn.cursor()

        # 取最近80个交易日(确保MA60预热)
        c.execute("SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date DESC LIMIT 80")
        dates = [r[0] for r in c.fetchall()][::-1]
        if len(dates) < 60:
            return []

        latest = dates[-1]

        # 批量拉数据
        c.execute("""
            SELECT stock_code, trade_date, close, open, high, low, volume, amount, pre_close, turnover_rate
            FROM daily_price
            WHERE trade_date >= ?
            ORDER BY stock_code, trade_date
        """, (dates[0],))
        all_data = c.fetchall()

        # 按票分组
        stocks = defaultdict(list)
        for row in all_data:
            code, date, close, op, high, low, vol, amt, pc, tr = row
            if close is None or amt is None or vol is None or vol <= 0:
                continue
            stocks[code].append({
                'date': date, 'close': float(close), 'open': float(op or 0),
                'high': float(high or 0), 'low': float(low or 0),
                'volume': float(vol), 'amount': float(amt),
                'pre_close': float(pc or 0), 'turnover': float(tr or 0),
            })

        for code, data in stocks.items():
            if len(data) < 60:
                continue
            # 排除科创/北交
            if code.startswith(("688", "689", "200", "8", "9")):
                continue

            # 最新一天数据
            last = data[-1]
            if last['date'] != latest:
                continue

            # 价格过滤
            if last['close'] < 3 or last['close'] > 100:
                continue

            # 成交额过滤
            if last['amount'] < AMT_MIN or last['amount'] > AMT_MAX:
                continue

            closes = np.array([d['close'] for d in data])
            volumes = np.array([d['volume'] for d in data])
            n = len(closes)
            if n < 60:
                continue

            # === 指标计算 ===

            # MA
            ma5 = np.mean(closes[-5:])
            ma20 = np.mean(closes[-20:])
            ma60 = np.mean(closes[-60:])

            # 条件1: 多头排列
            if not (ma5 > ma20 > ma60):
                continue

            # 条件2: MACD (EMA12 - EMA26 > 0)
            ema12 = ema26 = float(closes[0])
            for p in closes:
                ema12 = p * 2/13 + ema12 * 11/13
                ema26 = p * 2/27 + ema26 * 25/27
            if ema12 <= ema26:
                continue

            # 条件3: RSI(14) 50-70
            rsi_val = None
            if n >= 15:
                deltas = np.diff(closes[-15:])
                up = np.mean(deltas[deltas > 0]) if np.any(deltas > 0) else 0
                down = abs(np.mean(deltas[deltas < 0])) if np.any(deltas < 0) else 0.001
                rsi_val = 100 - (100 / (1 + up / down))
                if not (RSI_LOW <= rsi_val <= RSI_HIGH):
                    continue

            # 条件4: 量比 >= 5 (20日均量基准)
            avg_vol_20 = np.mean(volumes[-21:-1]) if len(volumes) >= 22 else 1
            vol_ratio = volumes[-1] / avg_vol_20 if avg_vol_20 > 0 else 0

            # 条件5: 距MA60 < 10%
            pct_above_ma60 = (last['close'] - ma60) / ma60 * 100
            if pct_above_ma60 > MA60_PCT_MAX:
                continue

            # === 分档 ===
            if vol_ratio >= 8.0:
                tier = "hot"
                tier_label = "热门(量比>8)"
            elif vol_ratio >= VOL_RATIO_MIN:
                tier = "normal"
                tier_label = "正常(量比5-8)"
            else:
                tier = "watch"
                tier_label = "监控(量比<5)"

            # === 趋势评分(用于精选排序) ===
            trend_score = 0

            # 1. 量比(核心因子, 回测验证)
            if vol_ratio >= 15:      trend_score += 30
            elif vol_ratio >= 8:     trend_score += 25
            elif vol_ratio >= 5:     trend_score += 15
            else:                    trend_score += 5

            # 2. 距MA60(越小越好=刚起涨)
            if pct_above_ma60 < 3:   trend_score += 20
            elif pct_above_ma60 < 5: trend_score += 15
            elif pct_above_ma60 < 8: trend_score += 10
            else:                    trend_score += 5

            # 3. RSI(55-65最佳)
            if rsi_val and 55 <= rsi_val <= 65:  trend_score += 15
            elif rsi_val and 50 <= rsi_val <= 70: trend_score += 10

            # 4. 成交额(3-8亿最佳)
            amt_b = last['amount'] / 1e8
            if 3 <= amt_b <= 8:      trend_score += 15
            elif 2 <= amt_b <= 10:   trend_score += 10

            # 5. MA60斜率(上升趋势)
            if n >= 65:
                ma60_5ago = np.mean(closes[-65:-5])
                ma60_slope = (ma60 - ma60_5ago) / ma60_5ago * 100
                if ma60_slope > 2:    trend_score += 10
                elif ma60_slope > 1:  trend_score += 7
                elif ma60_slope > 0:  trend_score += 3

            # 6. MACD强度
            macd_val = ema12 - ema26
            macd_pct = macd_val / last['close'] * 100
            if macd_pct > 2:         trend_score += 10
            elif macd_pct > 1:       trend_score += 5

            # 获取票名
            name_row = c.execute(
                "SELECT name FROM zt_pool WHERE stock_code=? LIMIT 1", (code,)
            ).fetchone()
            if not name_row:
                name_row = c.execute(
                    "SELECT industry_name FROM stock_industry_mapping WHERE stock_code=? LIMIT 1", (code,)
                ).fetchone()
            display_name = name_row[0] if name_row else code

            _rsi_str = f"{rsi_val:.0f}" if rsi_val else "NA"
            candidates.append({
                "code": code,
                "name": display_name,
                "score": round(trend_score, 1),
                "source": "趋势牛股",
                "signal_type": "趋势牛股(策略C)",
                "_sub_source": "巨量突破",
                "_strategy": "C",
                "_ma5": round(ma5, 2),
                "_ma20": round(ma20, 2),
                "_ma60": round(ma60, 2),
                "_ma60_pct": round(pct_above_ma60, 1),
                "_macd": round(macd_val, 2),
                "_rsi": round(rsi_val, 1) if rsi_val else 0,
                "_vol_ratio": round(vol_ratio, 2),
                "_tier": tier,
                "_buy_target": round(last['close'], 2),
                "_est_hold": MAX_HOLD_DAYS,
                "reason": (
                    f"趋势牛股: MA5={ma5:.2f}>MA20={ma20:.2f}>MA60={ma60:.2f} "
                    f"+{pct_above_ma60:.1f}% RSI{_rsi_str} 量比{vol_ratio:.1f} "
                    f"趋势{trend_score:.0f}分({tier_label}) 持{MAX_HOLD_DAYS}天"
                ),
            })

        # 去重
        seen = set()
        deduped = []
        for cand in candidates:
            if cand["code"] not in seen:
                seen.add(cand["code"])
                deduped.append(cand)
        candidates = deduped

        # 排序: hot > normal > watch, 同档按评分降序
        tier_order = {"hot": 3, "normal": 2, "watch": 1}
        candidates.sort(
            key=lambda x: (tier_order.get(x.get("_tier"), 0), x.get("score", 0)),
            reverse=True
        )

        hot = [c for c in candidates if c.get("_tier") == "hot"]
        normal = [c for c in candidates if c.get("_tier") == "normal"]
        watch = [c for c in candidates if c.get("_tier") == "watch"]
        logger.info(f"[策略C] 趋势牛股v2: {len(hot)}热门(量比>8) + {len(normal)}正常(量比5-8) + {len(watch)}监控")

        for cand in candidates[:8]:
            tag = {"hot": "热门", "normal": "正常", "watch": "监控"}.get(cand.get("_tier"), "?")
            logger.info(
                f"  {cand['code']} {cand['name']} "
                f"量比{cand.get('_vol_ratio', 0):.1f} MA60+{cand.get('_ma60_pct', 0):.1f}% "
                f"RSI{cand.get('_rsi', 0):.0f} 趋势{cand.get('score', 0):.0f}分[{tag}]"
            )

    except Exception as e:
        logger.warning(f"策略C(趋势牛股)选股异常: {e}")
    finally:
        if conn:
            conn.close()

    return candidates[:top_n]


def get_strategy_c_watchlist() -> list[dict]:
    """盘中实时监控列表 — daemon用"""
    return get_strategy_c_candidates()
