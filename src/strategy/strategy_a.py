"""策略A — 龙头首阴反包战法
# [GUARD-BYPASS] FIX-A1首阴深度过滤 + FIX-A7评分权重调整 + FIX-A2市场阶段适配

调研来源(2026-05-22, 7个实战来源共识):
  核心逻辑: 绝对龙头首次收阴后, 次日确认反包才买入

选股条件(来源: hiquant量化/游资实战/雪球淘股吧):
  1. 绝对龙头: 连板>=3优先, 2板也可以但要求是市场/板块最高板
  2. 首阴形态: 实体跌幅<5%(龙头首阴实体可以较大) + 有下影线加分
  3. 量能: 温和放量, 非爆量(换手率10-35%, 量能为前日1.2-2.5倍)
  4. 板块: 有梯队(同板块>=2只涨停)加分
  5. 封板质量: open_count少(封板牢固)

注意: 策略A不需要10天去重! 去重是策略B(首板)的逻辑。
  龙头本身就是连续涨停的票, "10天内重复涨停"不适用于龙头概念。

买入(次日确认模式, 来源共识):
  信号: 集合竞价高开>=2% + 开盘15分钟内翻红(收盘>开盘)
  不符合确认条件 → 不买

卖出:
  止损: 跌破首阴最低价×0.98
  止盈: 反包后根据盘面, 最长持3天
"""

import logging
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent.parent / "data" / "alpha_miner.db"
TOP_N = 20


def get_strategy_a_candidates(top_n: int = TOP_N, market_phase: str = "正常") -> list[dict]:
    """策略A选股: 龙头首阴反包

    返回分三档:
      - _tier='confirmed': 满足所有条件(实体<5%+有下影线+龙头), 次日可确认买入
      - _tier='watch': 部分条件满足(连板>=3或龙头分>=30), 监控观察
      - _tier='weak': 条件较弱, 展示但不推荐
    """
    candidates = []
    conn = None
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        c = conn.cursor()

        c.execute("SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date")
        all_dates = [r[0] for r in c.fetchall()]
        latest_td = all_dates[-1] if all_dates else None
        if not latest_td:
            return []

        # 取最近3个有连板的涨停日
        recent_zt_dates = []
        for d in reversed(all_dates):
            c.execute(
                "SELECT 1 FROM zt_pool WHERE trade_date=? AND consecutive_zt>=2 LIMIT 1",
                (d,),
            )
            if c.fetchone():
                recent_zt_dates.insert(0, d)
            if len(recent_zt_dates) >= 3:
                break

        # === 第一步: 找所有连板票 ===
        # FIX-A2: 退潮/冰点/偏弱市场只看3板以上, 降低假信号
        min_lb = 3 if market_phase in ("退潮", "冰点", "偏弱") else 2
        zt_leaders = []  # (code, name, lb, zt_date, zt_amount, open_count, industry, circulation_mv)
        for zt_date in recent_zt_dates:
            c.execute(
                """SELECT stock_code, name, consecutive_zt, amount, open_count, industry, circulation_mv
                   FROM zt_pool
                   WHERE trade_date=? AND consecutive_zt>=?
                   ORDER BY consecutive_zt DESC, amount DESC""",
                (zt_date, min_lb),
            )
            for row in c.fetchall():
                code, name, lb, zt_amount, open_count, industry, circ_mv = row
                if code.startswith(("688", "689", "200", "8", "9")):
                    continue
                if name and "ST" in name.upper():
                    continue
                if not zt_amount or zt_amount < 100_000_000:  # 成交额>=1亿
                    continue
                zt_leaders.append((code, name, lb, zt_date, zt_amount, open_count or 0, industry, circ_mv or 0))

        # === 第二步: 计算板块梯队(同板块涨停数) ===
        industry_count = {}
        for code, name, lb, zt_date, amt, oc, ind, cm in zt_leaders:
            if ind:
                key = (zt_date, ind)
                industry_count[key] = industry_count.get(key, 0) + 1

        # === 第三步: 找首阴 + 形态分析 ===
        for code, name, lb, zt_date, zt_amount, open_count, industry, circ_mv in zt_leaders:
            display_name = name or code

            # 涨停日K线
            zt_dp = c.execute(
                "SELECT open, close, high, low, volume, amount, turnover_rate FROM daily_price WHERE stock_code=? AND trade_date=?",
                (code, zt_date),
            ).fetchone()
            if not zt_dp or not zt_dp[1]:
                continue
            zt_open, zt_close, zt_high, zt_low, zt_vol, zt_amt, zt_tr = zt_dp

            # 找首阴日(涨停后第一个收阴日)
            c.execute(
                """SELECT trade_date, open, close, high, low, volume, pre_close, amount, turnover_rate
                   FROM daily_price
                   WHERE stock_code=? AND trade_date > ?
                   ORDER BY trade_date LIMIT 5""",
                (code, zt_date),
            )
            future = c.fetchall()

            for row in future:
                f_date, f_open, f_close, f_high, f_low, f_vol, f_pc, f_amt, f_tr = row
                if not all([f_vol, f_open, f_close, f_pc]):
                    continue
                if not f_amt or f_amt < 30_000_000:
                    continue

                # 首阴判断: 收阴 + 低于昨收
                is_yin = f_close < f_open
                is_down = f_close < f_pc
                if not (is_yin and is_down):
                    continue

                # === FIX-A1: 首阴跌幅计算(提前到这里) ===
                yin_drop = (f_pc - f_close) / f_pc * 100  # 跌幅
                # 首阴跌幅>5%的不值得做(数据:深踩胜率48%均值-0.07% vs 微踩55%+1.21%)
                if yin_drop > 5.0:
                    continue

                # 注意: 策略A不做10天去重! 龙头本身就是连续涨停的票,
                # "10天内重复涨停"是策略B(首板)的概念, 不适用于龙头首阴。

                # 过期检查: 首阴日距今<=1天才有效(次日就要确认)
                f_idx = all_dates.index(f_date) if f_date in all_dates else -1
                if f_idx >= 0:
                    hold_after = len(all_dates) - f_idx - 1
                else:
                    hold_after = 99
                if hold_after > 1:
                    break

                # === 首阴形态分析(核心!) ===
                yin_body = abs(f_close - f_open)                  # 实体
                yin_upper_shadow = f_high - max(f_open, f_close)  # 上影线
                yin_lower_shadow = min(f_open, f_close) - f_low   # 下影线
                yin_body_pct = yin_body / f_pc * 100 if f_pc else 0  # 实体跌幅

                # 下影线/实体比: >=1.0倍说明有承接
                has_lower_shadow = yin_body > 0 and yin_lower_shadow >= yin_body * 1.0

                # 量能对比
                vol_ratio = f_vol / zt_vol if zt_vol else 0
                is_good_volume = 1.2 <= vol_ratio <= 2.5  # 温和放量
                is_moderate_volume = f_tr and 10 <= f_tr <= 35 if f_tr else False  # 换手率10-35%

                # === 龙头评分 (FIX-A7: 权重调整) ===
                # 连板高度(3板以上是真龙头) — 30分满分
                lb_score = min(lb / 5, 1.0) * 30  # 30分满分, 5板满分

                # 板块梯队(同板块>=2只涨停) — 15分满分(从20降低)
                tidao = industry_count.get((zt_date, industry), 0) if industry else 0
                tidao_score = min(tidao / 3, 1.0) * 15

                # 封板质量(open_count越少越好) — 20分满分
                seal_score = max(0, (3 - open_count) / 3) * 20  # 20分满分, 0开板满分

                # 流通市值 — 10分满分(从15降低), 调整分档
                circ_b = (circ_mv or 0) / 1e8
                if 30 <= circ_b <= 80:
                    circ_score = 10
                elif 20 <= circ_b <= 150:
                    circ_score = 7
                else:
                    circ_score = 3

                # 首阴形态质量 — 满分25分(从最高18分增加)
                shape_score = 0
                if yin_body_pct < 3:     shape_score += 8   # 实体很小
                elif yin_body_pct < 5:   shape_score += 5   # 实体<5%
                if has_lower_shadow:      shape_score += 7   # 有下影线
                if is_good_volume:        shape_score += 3   # 温和放量
                if is_moderate_volume:    shape_score += 2   # 换手合理

                dragon_total = lb_score + shape_score + seal_score + tidao_score + circ_score

                # === 分档 ===
                # confirmed: 实体<5% + 龙头分>=40 (放宽实体阈值, 龙头首阴实体往往较大)
                # watch: 连板>=3 或 龙头分>=30
                # weak: 其他
                if yin_body_pct < 5 and has_lower_shadow and dragon_total >= 40:
                    tier = "confirmed"
                    tier_label = "确认"
                elif lb >= 3 or dragon_total >= 30:
                    tier = "watch"
                    tier_label = "观察"
                else:
                    tier = "weak"
                    tier_label = "偏弱"

                stop_loss = round(f_low * 0.98, 2)  # 跌破首阴最低价×0.98
                buy_target = round(f_pc * 1.02, 2)   # 次日确认: 高开2%左右

                candidates.append({
                    "code": code,
                    "name": display_name,
                    "score": round(dragon_total, 1),
                    "source": "首阴日内",
                    "signal_type": "首阴日内",
                    "_sub_source": "龙头首阴",
                    "_strategy": "A",
                    "_zt_date": zt_date,
                    "_yin_date": f_date,
                    "_yin_close": f_close,
                    "_yin_low": f_low,
                    "_yin_open": f_open,
                    "_yin_high": f_high,
                    "_yin_pc": f_pc,
                    "_yin_body_pct": round(yin_body_pct, 1),
                    "_yin_lower_shadow": round(yin_lower_shadow, 2),
                    "_yin_drop_pct": round(yin_drop, 1),
                    "_vol_ratio": round(vol_ratio, 2),
                    "_lb": lb,
                    "_open_count": open_count,
                    "_industry": industry or "",
                    "_tidao": tidao,
                    "_circ_mv_b": round(circ_b, 1),
                    "_dragon_total": round(dragon_total, 1),
                    "_buy_target": buy_target,
                    "_stop_loss": stop_loss,
                    "_tier": tier,
                    "reason": f"{lb}连板首阴{f_date} 实体{yin_body_pct:.1f}%{'有下影' if has_lower_shadow else ''} 龙头{dragon_total:.0f}分({tier_label}) 止损¥{stop_loss:.2f}",
                })
                break  # 找到第一个首阴就停

        # 去重(按code去重, 同一只票在不同涨停日可能出现)
        seen = set()
        deduped = []
        for cand in candidates:
            if cand["code"] not in seen:
                seen.add(cand["code"])
                deduped.append(cand)
        candidates = deduped

        # 排序: confirmed > watch > weak, 同档按龙头分降序
        tier_order = {"confirmed": 2, "watch": 1, "weak": 0}
        candidates.sort(key=lambda x: (tier_order.get(x.get("_tier"), 0), x.get("_dragon_total", 0)), reverse=True)

        confirmed = [c for c in candidates if c.get("_tier") == "confirmed"]
        watch = [c for c in candidates if c.get("_tier") == "watch"]
        weak = [c for c in candidates if c.get("_tier") == "weak"]
        logger.info(f"[策略A] 龙头首阴反包: {len(confirmed)}确认 + {len(watch)}观察 + {len(weak)}偏弱")

        for cand in candidates[:8]:
            tag = {"confirmed": "确认", "watch": "观察", "weak": "偏弱"}.get(cand.get("_tier"), "?")
            logger.info(
                f"  {cand['code']} {cand['name']} "
                f"{cand.get('_lb', 0)}连板 实体{cand.get('_yin_body_pct', 0)}% "
                f"龙头{cand.get('_dragon_total', 0):.0f}分[{tag}] "
                f"板块{cand.get('_industry', '')}({cand.get('_tidao', 0)}只)"
            )

    except Exception as e:
        logger.warning(f"策略A选股异常: {e}")
    finally:
        if conn:
            conn.close()

    return candidates[:top_n]
