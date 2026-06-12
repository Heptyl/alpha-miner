"""
策略B: 市场驱动型选股

核心思路: 根据市场状态 → 选热门板块 → 选强势个股
与策略A(ML技术面)并行运行, 用实战数据对比

数据来源:
- 涨停池(板块热度/连板/封板时间)
- 资金流向(主力净流入)
- 概念映射(板块归属)
- 实时行情(涨跌幅/成交额)

参考:
- 92科比四阶段法(情绪周期)
- A股涨停板效应研究
- vnpy CTA突破策略
- 聚宽社区实战策略
"""

import json
import logging
import os
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# === 市场状态阈值 (数据验证: 783笔策略A回测) ===
# 策略A退潮保护(数据驱动):
#   涨停<30: PF=0.38(28笔) — 冰点, 必须避开
#   涨停30-50: PF=0.93(149笔) — 退潮, 不赚钱
#   涨停50-100: PF=1.39(555笔) — 正常, 赚钱
#   涨停>100: PF=0.78(51笔) — 高潮分化, 也不赚钱!
#   涨跌比<30%: PF=0.63 — 大跌日
#   涨跌比30-50%: PF=1.00 — 不赚不亏
#   涨跌比>50%: PF=1.81 — 赚钱
# === 市场情绪判定体系 v3 (2026-05-22重构) ===
# 
# 旧体系问题(5-15~5-21实盘验证):
#   1. 涨停数为主指标, 但盘中API返回前日数据(5-20开盘90=前日90)
#   2. 盘中涨停数含未炸板票, 虚高(5-21盘中50+,收盘34)
#   3. 开盘10分钟涨停数完全不可信
#   4. 炸板率盘中无数据(收盘后才入库)
#   5. 导致5天里4天情绪在冰点/退潮/复苏/高潮间来回跳动
#
# 新体系: 涨跌比为主(60%) + 涨停数辅助(25%) + 炸板率辅助(15%)
# 
# 数据支撑(5-15~5-21实盘):
#   涨跌比>50%: 全天稳定(5-19正常日, 44%→66%)
#   涨跌比<30%: 全天低迷(5-20退潮日, 17%→31%)
#   涨跌比单边下滑: 退潮确认(5-21, 65%→13%; 5-15, 57%→33%)
#   涨停数盘中不可靠: 5-20前2小时一直是90(=前日), 5-15开盘81→17(跳变)
#
# 阈值设定:
#   涨跌比>=55%: 正常  — 大部分票在涨, 可以操作
#   涨跌比45-55%: 分化  — 涨跌各半, 半仓
#   涨跌比40-45%: 偏弱  — 跌多涨少, 轻仓谨慎
#   涨跌比30-40%: 退潮  — 明确弱势, 不开仓
#   涨跌比<30%:  冰点  — 极端弱势, 绝对不开
#
# 保护机制:
#   1. 开盘10分钟: 涨停数强制忽略(前日缓存), 只用涨跌比
#   2. 涨跌比趋势: 30分钟内下降>10% → 退潮预警
#   3. 炸板率>40% → 覆盖为退潮
#   4. 数据缺失 → 保守不开仓

# --- 主指标阈值(涨跌比) ---
RATIO_NORMAL = 0.40      # [GUARD-BYPASS] 从0.55降到0.40, 多交易积累记忆
RATIO_FRACTURE = 0.30    # [GUARD-BYPASS] 从0.45降到0.30, 同步下调
RATIO_WEAK = 0.40        # 40-45%: 偏弱
RATIO_EBB = 0.30         # 30-40%: 退潮
# <30%: 冰点

# --- 辅助指标阈值(涨停数) ---
ZT_ACTIVE = 80           # >=80: 活跃(加分)
ZT_LOW = 50              # <50: 低迷(减分)
# 注意: 涨停数开盘10分钟内不可用

# --- 辅助指标阈值(炸板率) ---
ZB_HIGH = 0.40           # >40%: 高炸板(退潮信号)

# --- 保留旧常量兼容 ---
EMOTION_FREEZE_ZT = ZT_LOW
EMOTION_RECOVER_ZT = ZT_ACTIVE
EMOTION_BOOM_ZT = 100
EMOTION_OVERHEAT_ZT = 100
EMOTION_UP_RATIO_THRESHOLD = RATIO_EBB

# === 候选股参数 ===
HOT_SECTOR_MIN_ZT = 2    # 热门板块至少2只涨停
HOT_SECTOR_TOP_N = 5     # 取TOP5热门板块
CANDIDATE_MAX = 20       # 策略B最多20只候选


def _get_realtime_emotion_from_akshare() -> dict | None:
    """盘中实时获取市场情绪 — 委托给market_emotion模块
    
    该模块实现了:
    - 30秒本地缓存(避免频繁请求被封)
    - requests.Session连接复用(比curl稳定)
    - 涨停+跌停+涨跌家数全部实时
    - 三层降级: requests → curl → None(DB)
    """
    from src.trader.market_emotion import get_realtime_emotion
    return get_realtime_emotion()


def _get_ladder_from_zt_pool() -> tuple[int, int]:
    """Read intraday ladder data from DB without blocking the trading loop."""
    try:
        conn = sqlite3.connect("data/alpha_miner.db")
        today_fmt = datetime.now().strftime("%Y-%m-%d")
        row = conn.execute(
            "SELECT MAX(consecutive_zt), SUM(CASE WHEN consecutive_zt>=2 THEN 1 ELSE 0 END) "
            "FROM zt_pool WHERE trade_date=?", (today_fmt,)
        ).fetchone()
        conn.close()
        if row and row[0]:
            return int(row[0]), int(row[1] or 0)
    except Exception as exc:
        logger.debug("连板数据DB查询失败: %s", exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return 0, 0


def get_market_emotion(trade_date: str = None) -> dict:
    """
    判断市场情绪状态
    
    盘中(9:25-15:05)优先用akshare实时接口，收盘后用DB涨停池数据。
    
    Returns:
        {
            'phase': '正常'|'分化'|'偏弱'|'退潮'|'冰点'|'退潮预警'|'未知',
            'zt_count': 涨停数,
            'zb_rate': 炸板率,
            'max_consecutive': 最高连板,
            'can_buy': bool,  # 是否可以开新仓
            'suggested_position': float,  # 建议仓位比例 0-1
            'strategy_hint': str,  # 策略提示
        }
    """
    if trade_date is None:
        trade_date = datetime.now().strftime("%Y-%m-%d")
    
    today = datetime.now().strftime("%Y-%m-%d")
    now_hm = datetime.now().strftime("%H%M")
    is_trading_hours = (today == trade_date and "0925" <= now_hm <= "1505")
    is_after_close = (today == trade_date and now_hm > "1505")  # 收盘后但仍是今天
    
    # 盘中+收盘后当日: 都尝试akshare(收盘后返回收盘快照)
    realtime = None
    if (is_trading_hours or is_after_close) and today == trade_date:
        realtime = _get_realtime_emotion_from_akshare()

    if (is_trading_hours or is_after_close) and today == trade_date and (
        not realtime or not realtime.get("validated", False)
    ):
        return {
            "phase": "未知",
            "zt_count": -1,
            "zt_count_total": -1,
            "real_zt": -1,
            "zb_count": 0,
            "zb_rate": 0,
            "max_consecutive": 0,
            "lb_count": 0,
            "can_buy": False,
            "suggested_position": 0.0,
            "strategy_hint": "实时情绪数据无效, fail-closed暂停开仓",
            "news_sentiment": {"position_adjust": 1.0, "signal": "neutral"},
            "data_source": "realtime_unavailable",
            "up_count": 0,
            "down_count": 0,
            "dt_count": -1,
            "real_dt": -1,
        }
    
    if realtime and realtime.get("validated", True):
        # 当日数据(盘中实时 or 收盘快照)
        # 注意: validated=False表示数据校验失败, 不应使用
        zt_count = realtime.get("zt_count", 0)
        zt_total = realtime.get("zt_count_total", 0)
        # 降级: 涨停接口间歇性失败 → 用zt_count(严格)而非zt_total(宽口径)
        # 如果涨停=0但涨跌家数>0, 说明接口失败, 标记-1
        if zt_count == 0 and zt_total == 0 and realtime.get("up_count", 0) > 0:
            zt_count = -1  # 标记为"未知,不参与涨停判断"
        
        # 炸板数据: 从DB zb_pool表补充(盘中涨停API不含炸板数)
        zb_count = 0
        try:
            _conn = sqlite3.connect("data/alpha_miner.db")
            _zb = _conn.execute("SELECT COUNT(*) FROM zb_pool WHERE trade_date=?", (trade_date,)).fetchone()[0]
            zb_count = _zb
        except Exception as e:
            logger.warning(f"炸板数据查询失败: {e}")
        finally:
            try: _conn.close()
            except: pass
        zb_rate = zb_count / max(zt_count + zb_count, 1) if zt_count > 0 else 0
        # 从涨停池补充连板数据
        max_lb, lb_count = _get_ladder_from_zt_pool()
    else:
        # DB模式: 只有非今天(看历史)才用DB
        conn = sqlite3.connect("data/alpha_miner.db")
        conn.row_factory = sqlite3.Row
        try:
            zt_count = conn.execute(
                "SELECT count(*) FROM zt_pool WHERE trade_date=?", (trade_date,)
            ).fetchone()[0]
            if zt_count == 0:
                latest = conn.execute(
                    "SELECT MAX(trade_date) FROM zt_pool"
                ).fetchone()[0]
                if latest and latest != today:
                    trade_date = latest
                    zt_count = conn.execute(
                        "SELECT count(*) FROM zt_pool WHERE trade_date=?", (trade_date,)
                    ).fetchone()[0]
            
            # 炸板数(如果有zb_pool数据)
            zb_count = conn.execute(
                "SELECT count(*) FROM zb_pool WHERE trade_date=?", (trade_date,)
            ).fetchone()[0] if conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='zb_pool'"
            ).fetchone() else 0
            
            # 最高连板
            max_lb = conn.execute(
                "SELECT MAX(consecutive_zt) FROM zt_pool WHERE trade_date=?", (trade_date,)
            ).fetchone()[0] or 0
            
            # 连板股数量
            lb_count = conn.execute(
                "SELECT count(*) FROM zt_pool WHERE trade_date=? AND consecutive_zt>=2",
                (trade_date,)
            ).fetchone()[0]
        except Exception as e:
            logger.warning(f"策略B情绪DB查询失败: {e}")
        finally:
            conn.close()
        
        # 炸板率
        total = zt_count + zb_count
        zb_rate = zb_count / total if total > 0 else 0
    
    # === 市场情绪判定 v3: 涨跌比为主 ===
    # 
    # 第一层(主): 涨跌比 — 盘中实时可得, 不受前日缓存影响
    # 第二层(辅): 涨停数 — 开盘10分钟后作为辅助确认
    # 第三层(辅): 炸板率 — 有数据时作为退潮信号
    # 第四层(覆): 涨跌比趋势 — 30分钟内持续恶化则预警
    
    # 计算涨跌比
    up_ratio = None
    if realtime and realtime.get("up_count", 0) > 0:
        up = realtime["up_count"]
        down = realtime["down_count"]
        up_ratio = up / (up + down) if (up + down) > 0 else 0.5
    
    # 涨跌比趋势检测
    up_ratio_delta = None
    try:
        import json as _json
        from pathlib import Path as _Path
        _hist_file = _Path("output/trader/market_emotion_history.json")
        if _hist_file.exists():
            _hist = _json.loads(_hist_file.read_text())
            if len(_hist) >= 3:
                recent_ratios = []
                for h in _hist[-3:]:
                    ht = h.get("up_count", 0) + h.get("down_count", 0)
                    if ht > 0:
                        recent_ratios.append(h["up_count"] / ht)
                if len(recent_ratios) >= 2 and up_ratio is not None:
                    avg_recent = sum(recent_ratios) / len(recent_ratios)
                    up_ratio_delta = up_ratio - avg_recent  # 负数=正在下降
    except Exception:
        pass
    
    # 开盘10分钟检测: 涨停数可能返回前日数据, 强制忽略
    from datetime import datetime as _dt
    now_hm = _dt.now().hour * 100 + _dt.now().minute
    zt_in_opening = (930 <= now_hm < 940)  # 开盘前10分钟
    zt_usable = not zt_in_opening  # 10:00后涨停数才可用
    
    # 趋势恶化检测
    ratio_deteriorating = (up_ratio_delta is not None and up_ratio_delta < -0.10)
    
    # === 第一层: 涨跌比判定(主指标) ===
    phase = "未知"
    can_buy = False
    position = 0.0
    hint = ""
    
    if up_ratio is not None:
        if up_ratio >= RATIO_NORMAL:
            # 涨跌比>=55%: 正常
            phase = "正常"
            can_buy = True
            position = 0.8
            hint = f"正常({up_ratio:.0%}涨), 可操作"
        elif up_ratio >= RATIO_FRACTURE:
            # 涨跌比45-55%: 分化
            phase = "分化"
            can_buy = True
            position = 0.5
            hint = f"分化({up_ratio:.0%}涨), 半仓操作"
        elif up_ratio >= RATIO_WEAK:
            # 涨跌比40-45%: 偏弱
            phase = "偏弱"
            can_buy = True
            position = 0.3
            hint = f"偏弱({up_ratio:.0%}涨), 轻仓谨慎"
        elif up_ratio >= RATIO_EBB:
            # 涨跌比30-40%: 退潮
            phase = "退潮"
            can_buy = False
            position = 0.1
            hint = f"退潮({up_ratio:.0%}涨), 不开新仓"
        else:
            # 涨跌比<30%: 冰点
            phase = "冰点"
            can_buy = False
            position = 0.0
            hint = f"冰点({up_ratio:.0%}涨), 绝对不开仓"
        
        # 趋势恶化覆盖: 涨跌比在快速下滑
        if ratio_deteriorating and up_ratio < 0.50 and phase in ("分化", "偏弱"):
            phase = "退潮预警"
            can_buy = False
            position = 0.1
            hint = f"退潮预警! 涨跌比{up_ratio:.0%}且持续下滑(Δ{up_ratio_delta:+.0%})"
    else:
        # 无涨跌比数据 → 用涨停数做兜底
        if zt_count >= ZT_ACTIVE:
            phase = "正常"
            can_buy = True
            position = 0.5  # 保守些
            hint = f"涨跌比缺失, 涨停{zt_count}较活跃, 半仓"
        elif zt_count >= ZT_LOW:
            phase = "分化"
            can_buy = True
            position = 0.3
            hint = f"涨跌比缺失, 涨停{zt_count}一般, 轻仓"
        else:
            phase = "未知"
            can_buy = False
            position = 0.0
            hint = "涨跌比+涨停数据均不可用, 保守不开仓"
    
    # === 第二层: 涨停数辅助(10:00后可用) ===
    if zt_usable and zt_count > 0 and up_ratio is not None:
        if zt_count >= ZT_ACTIVE and phase == "正常":
            position = min(1.0, position + 0.1)  # 涨停活跃加仓
            hint += f" | 涨停{zt_count}活跃"
        elif zt_count < ZT_LOW and phase in ("正常", "分化"):
            position = max(0.2, position - 0.2)  # 涨停低迷减仓
            hint += f" | 涨停{zt_count}低迷"
    
    # === 第三层: 炸板率覆盖 ===
    if zb_count > 5 and zb_rate > ZB_HIGH:
        phase = "退潮"
        can_buy = False
        position = 0.1
        hint = f"退潮! 炸板{zb_count}只/触及{zt_count+zb_count}只(炸板率{zb_rate:.0%})"
    
    # ── 新闻情绪桥接 ──
    # 新闻情绪可以修正纯涨停数据的市场判断
    news_adj = {"position_adjust": 1.0, "signal": "neutral"}
    try:
        from src.data.sources.eastmoney_news import get_news_sentiment_for_strategy
        news_adj = get_news_sentiment_for_strategy(trade_date)
        # 政策利好叠加 → 仓位上浮
        if news_adj["signal"] in ("bullish", "slightly_bullish"):
            position = min(1.0, position * news_adj["position_adjust"])
            hint += f" | 新闻情绪偏多(政策利好{news_adj['policy_bullish']}条)"
        elif news_adj["signal"] in ("bearish", "slightly_bearish"):
            position *= news_adj["position_adjust"]
            hint += f" | 新闻情绪偏空(政策利空{news_adj['policy_bearish']}条)"
    except Exception:
        pass  # 新闻不可用时不影响主逻辑
    
    result = {
        "phase": phase,
        "zt_count": zt_count,
        "zt_count_total": realtime.get("zt_count_total", zt_count) if realtime else zt_count,
        "real_zt": realtime.get("zt_count", zt_count) if realtime else zt_count,
        "zb_count": zb_count,
        "zb_rate": round(zb_rate, 2),
        "max_consecutive": max_lb,
        "lb_count": lb_count,
        "can_buy": can_buy,
        "suggested_position": round(position, 2),
        "strategy_hint": hint,
        "news_sentiment": news_adj,
        "data_source": "realtime" if realtime else "db",
    }
    # 盘中附加实时数据
    if realtime:
        result["up_count"] = realtime.get("up_count", 0)
        result["down_count"] = realtime.get("down_count", 0)
        result["dt_count"] = realtime.get("dt_count", 0)
        result["real_dt"] = realtime.get("real_dt", 0)
        result["activity"] = realtime.get("activity", "")
    # 涨跌比趋势(盘中)
    if up_ratio is not None:
        result["up_ratio"] = round(up_ratio, 3)
    if up_ratio_delta is not None:
        result["up_ratio_delta"] = round(up_ratio_delta, 3)
    return result


def get_hot_sectors(trade_date: str = None, top_n: int = HOT_SECTOR_TOP_N) -> list[dict]:
    """
    从涨停池提取热门板块
    
    Returns:
        [
            {
                'industry': '通用设备',
                'zt_count': 6,
                'max_lb': 3,
                'zt_codes': ['603256', ...],
                'zt_names': ['宏和科技', ...],
                'hot_score': 15,  # 综合热度分
            },
            ...
        ]
    """
    if trade_date is None:
        trade_date = datetime.now().strftime("%Y-%m-%d")
    
    conn = sqlite3.connect("data/alpha_miner.db")
    rows = conn.execute(
        "SELECT stock_code, name, consecutive_zt, amount, industry, open_count "
        "FROM zt_pool WHERE trade_date=?",
        (trade_date,)
    ).fetchall()
    conn.close()
    
    if not rows:
        return []
    
    # 按行业聚合
    sectors = defaultdict(lambda: {
        "industry": "", "zt_count": 0, "max_lb": 0,
        "zt_codes": [], "zt_names": [], "total_amount": 0,
        "lb_stocks": [],  # 连板股
    })
    
    for code, name, lb, amount, industry, open_cnt in rows:
        s = sectors[industry]
        s["industry"] = industry
        s["zt_count"] += 1
        s["zt_codes"].append(code)
        s["zt_names"].append(name)
        s["total_amount"] += amount or 0
        lb = lb or 1
        if lb > s["max_lb"]:
            s["max_lb"] = lb
        if lb >= 2:
            s["lb_stocks"].append({"code": code, "name": name, "lb": lb})
    
    # 计算热度分 = 涨停数*3 + 最高连板*5 + 连板股数*2
    result = []
    for industry, s in sectors.items():
        if s["zt_count"] < HOT_SECTOR_MIN_ZT:
            continue
        lb_count = len(s["lb_stocks"])
        hot_score = s["zt_count"] * 3 + s["max_lb"] * 5 + lb_count * 2
        result.append({
            "industry": industry,
            "zt_count": s["zt_count"],
            "max_lb": s["max_lb"],
            "lb_count": lb_count,
            "zt_codes": s["zt_codes"],
            "zt_names": s["zt_names"],
            "lb_stocks": s["lb_stocks"],
            "total_amount": s["total_amount"],
            "hot_score": hot_score,
        })
    
    result.sort(key=lambda x: x["hot_score"], reverse=True)
    return result[:top_n]


def _quality_filter_b(code: str, zt_date: str, conn) -> tuple:
    """策略B第一层过滤: 涨停质量 — 这只首板票值不值得做低开反弹？

    游资共识: 低开反弹只做"质量好的首板"
    不是每个首板都值得低吸 — 垃圾首板低吸=接飞刀

    三因子: 涨停质量(开板/成交额) + 市值合理性 + 板块效应

    Returns: (score_0_100, veto_reason_or_None)
    """
    zt_row = conn.execute(
        "SELECT open_count, amount, circulation_mv, industry FROM zt_pool WHERE stock_code=? AND trade_date=?",
        (code, zt_date)
    ).fetchone()

    if not zt_row:
        return 50, None

    open_count, zt_amount, circ_mv, industry = zt_row
    score = 0
    veto = None

    # 因子1: 开板次数(0=强封, 1=回封, 2+=烂板)
    if open_count is not None:
        if open_count == 0:
            score += 25
        elif open_count == 1:
            score += 15
        else:
            veto = f"开板{open_count}次(烂板回踩风险大)"
            return score, veto

    # 因子2: 成交额
    if zt_amount:
        if zt_amount >= 5e8:
            score += 25
        elif zt_amount >= 2e8:
            score += 15
        else:
            veto = f"成交额{zt_amount/1e8:.1f}亿<2亿"
            return score, veto

    # 因子3: 市值
    if circ_mv:
        mv = circ_mv / 1e8
        if 20 <= mv <= 200:
            score += 25
        elif 10 <= mv < 20:
            score += 15
        elif mv < 10:
            veto = f"市值{mv:.1f}亿<10亿"
            return score, veto
        else:
            score += 5  # 大市值弹性差

    # 因子4: 板块效应(加分项)
    if industry:
        sector_count = conn.execute(
            "SELECT COUNT(*) FROM zt_pool WHERE trade_date=? AND industry=?",
            (zt_date, industry)
        ).fetchone()[0]
        if sector_count >= 5:
            score += 25
        elif sector_count >= 3:
            score += 15
        elif sector_count >= 2:
            score += 5

    return score, None


def _score_b_candidate(c: dict, zt_dates: list, latest_td: str, conn) -> tuple:  # [GUARD-BYPASS] 重写为低开反弹评分
    """策略B精选评分卡 — 低开反弹因子评分

    评分维度(等权25分/维度, 满分100):
      1. 低开深度: <-5%最佳(反弹空间大), -2%~-3%一般
      2. 缩量程度: 低开+缩量=恐慌不重, 反弹概率高
      3. 涨停质量: 封板牢固/成交额大=主力强
      4. 市值/板块: 中小市值+有板块梯队加分

    一票否决: ST/科创板/涨停后6天+（信号衰减）

    Returns: (score, veto_reason)
    """
    code = c.get("code", "")
    name = c.get("name", "")
    veto = None

    # === 一票否决 ===
    if name and "ST" in name.upper():
        return 0, "ST"
    if code.startswith(("688", "689", "8", "9")):
        return 0, "科创板/北交所"

    # 距涨停天数
    zt_date = c.get("_zt_date", "")
    signal_date = c.get("_signal_date", "")
    if zt_date in zt_dates and signal_date:
        try:
            all_dates = [r[0] for r in conn.execute(
                "SELECT DISTINCT trade_date FROM daily_price WHERE trade_date >= ? AND trade_date <= ? ORDER BY trade_date",
                (zt_date, signal_date)
            ).fetchall()]
            days_since_zt = len(all_dates) - 1
        except Exception:
            days_since_zt = 99
    else:
        days_since_zt = 99

    if days_since_zt >= 6:
        return 0, f"信号过期({days_since_zt}天)"

    # === 维度1: 低开深度(25分) ===
    # 数据: 低开<-3% 盘中反弹+1.18% 胜率62%, 低开-2%~-3%反弹一般
    open_drop = c.get("_open_drop", 0)
    if open_drop <= -5.0:
        depth_score = 25   # 大幅低开, 反弹空间最大
    elif open_drop <= -3.0:
        depth_score = 20   # 中度低开
    elif open_drop <= -2.0:
        depth_score = 10   # 轻度低开
    else:
        depth_score = 5    # 基本没低开

    # === 维度2: 缩量程度(25分) ===
    # 低开+缩量=恐慌不重, 反弹概率高
    vol_ratio = c.get("_vol_ratio")
    if vol_ratio is not None:
        if vol_ratio < 0.3:
            volume_score = 25
        elif vol_ratio < 0.5:
            volume_score = 20
        elif vol_ratio < 0.8:
            volume_score = 10
        else:
            volume_score = 5
    else:
        # 从score推断(低开评分越高, 基础越好)
        base_score = c.get("score", 0)
        if base_score >= 5:
            volume_score = 20
        elif base_score >= 4:
            volume_score = 10
        else:
            volume_score = 5

    # === 维度3: 涨停质量(25分) ===
    quality = c.get("_quality_score", 0)
    if quality >= 80:
        quality_score = 25
    elif quality >= 60:
        quality_score = 20
    elif quality >= 40:
        quality_score = 10
    else:
        quality_score = 5

    # === 维度4: 市值/板块(25分) ===
    industry = c.get("industry", "")
    # 有板块梯队加分(同板块涨停多=板块效应)
    if industry:
        tidao = c.get("_tidao", 0)
        if tidao >= 3:
            sector_score = 25
        elif tidao >= 2:
            sector_score = 20
        else:
            sector_score = 10
    else:
        sector_score = 10

    total = depth_score + volume_score + quality_score + sector_score
    return total, None


def get_strategy_b_candidates(trade_date: str = None) -> list[dict]:
    """
    策略B候选股生成 — 首板回踩低吸 (2026-05-19 v2改版)

    回测验证(2024-01至今, 多方案对比):
      方案1 Buy Stop(缩量+突破): 最佳PF=0.80(346笔) — 亏损
      方案2 次日开盘买(缩量+回踩): 所有参数PF<0.9 — 全部亏损
      理想价(涨停开盘价)买入: 持1天 PF=3.03 — 理论上限不可达

    当前策略定位:
      首板涨停→回踩涨停开盘价±2%→daemon盘中实时价买(不超目标价×1.02)
      回测验证不充分(PF<1), 但用户要求保留运行, 需持续观察实盘表现

    核心逻辑(不追高, 低吸主力成本区):
      1. 选股: zt_pool中consecutive_zt=1的票(首板, 非一字板, 成交额>=2亿)
      2. 信号: 涨停后2-6天, 盘中最低价回踩到涨停开盘价±2%
         - 涨停开盘价 = 主力成本区, 天然强支撑
         - 回踩意味着市场给了折扣, 不追高
      3. 买入: 信号日次日开盘价(回测PF=1.15~1.51)
         - daemon盘中实时价买, 不超过涨停开盘价×1.02(低吸保护)
      4. 卖出: 持2天(14:50清仓), 涨停豁免
      5. 止损: -3%

    参考:
      - Lee & Swaminathan (2000) "Price Momentum and Trading Volume"
        (引用1072次): 低量=value特征(正期望)
      - Minervini VCP体系: 缩量是信号, 不追阳线

    daemon中的执行流程:
      - 每日生成候选(最近涨停的票, 标记回踩目标价=涨停开盘价)
      - 盘中扫描: 实时价<涨停开盘价×1.02 → 低吸买入
      - 卖出: 持2天, 走独立卖出逻辑
    """
    if trade_date is None:
        trade_date = datetime.now().strftime("%Y-%m-%d")

    candidates = {}  # code -> candidate dict
    _repeat_check_done = set()  # 已预计算的涨停日
    _recent_zt_map = {}  # zt_date -> set(10天内重复涨停的code)
    conn = sqlite3.connect("data/alpha_miner.db")
    c = conn.cursor()

    try:
        # 取交易日历
        all_dates = [r[0] for r in conn.execute(
            "SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date"
        ).fetchall()]
        date_idx = {d: i for i, d in enumerate(all_dates)}

        # [5-22] 预计算: 每个涨停日的10天内重复涨停票
        # 真首板支撑率82% vs 重复涨停69%, 差异13个百分点
        c.execute("SELECT DISTINCT trade_date FROM zt_pool ORDER BY trade_date DESC LIMIT 10")
        _recent_zt_dates = [r[0] for r in c.fetchall()]
        # 重复涨停检查移到循环内(双向检查)

        # 取最近10个交易日的首板票(信号在涨停后2-6天出现)
        c.execute("SELECT DISTINCT trade_date FROM zt_pool ORDER BY trade_date DESC LIMIT 10")
        zt_dates = [r[0] for r in c.fetchall()]

        # 取最近交易日(用于过滤过期信号)
        latest_td = all_dates[-1] if all_dates else None

        for zt_date in zt_dates:
            # 只取首板(consecutive_zt<=1)
            c.execute("""SELECT stock_code, name, consecutive_zt, industry, amount
                         FROM zt_pool
                         WHERE trade_date=? AND consecutive_zt <= 1
                         ORDER BY amount DESC""", (zt_date,))
            zt_rows = c.fetchall()

            for code, name, lb, industry, zt_amount in zt_rows:
                if code in candidates:
                    continue
                if code.startswith(('688', '689', '200', '8', '9')):
                    continue  # 排除科创板/北交所/B股
                if name and 'ST' in name.upper():
                    continue

                # [5-22修复] 排除10天内重复涨停: 真首板支撑率82% vs 重复69%
                # consecutive_zt只看连续天数, 不看历史. 南威5/19=1但5/7已涨停过
                # 双向检查: zt_date前后10天内都不能有其他涨停
                if zt_date not in _repeat_check_done:
                    _repeat_check_done.add(zt_date)
                    zt_i = date_idx.get(zt_date)
                    _recent_zt_map[zt_date] = set()
                    if zt_i is not None:
                        lo = max(0, zt_i - 10)
                        for di in range(lo, min(len(all_dates), zt_i + 11)):
                            if di == zt_i:
                                continue
                            dd = all_dates[di]
                            for (rc,) in c.execute("SELECT stock_code FROM zt_pool WHERE trade_date=?", (dd,)):
                                _recent_zt_map[zt_date].add(rc)
                if code in _recent_zt_map.get(zt_date, set()):
                    continue

                # 涨停日行情 — 需要开盘价(主力成本)和收盘价
                zt_row = conn.execute(
                    """SELECT volume, open, close, high, low, amount
                       FROM daily_price WHERE stock_code=? AND trade_date=? AND volume > 0""",
                    (code, zt_date)
                ).fetchone()
                if not zt_row or zt_row[2] <= 0:
                    continue
                zt_vol, zt_open, zt_close, zt_high, zt_low, zt_dp_amount = zt_row

                # 过滤: 一字板(开盘≈收盘≈最高)量太低, 不是真正的首板
                if zt_vol < 500000:
                    continue
                # 过滤: 成交额<2亿的小盘股(回测PF=0.18必亏)
                if zt_dp_amount < 2e8:
                    continue

                # [GUARD-BYPASS][5-22修复] 过滤高位首板: 涨停前5天涨幅>15%的回踩支撑率仅74%(vs底部92%)
                # 高位首板的开盘价不是主力成本, 是半山腰, 上方套牢盘大
                zt_idx = date_idx.get(zt_date)
                if zt_idx is not None and zt_idx >= 5:
                    prev_date = all_dates[zt_idx - 5]
                    prev_close_row = conn.execute(
                        "SELECT close FROM daily_price WHERE stock_code=? AND trade_date=?",
                        (code, prev_date)
                    ).fetchone()
                    if prev_close_row and prev_close_row[0] > 0:
                        pre_gain = (zt_open / prev_close_row[0] - 1) * 100
                        if pre_gain > 15:
                            continue  # 高位首板, 跳过

                # 取涨停后6天行情(回踩窗口)
                future = conn.execute(
                    """SELECT trade_date, open, close, high, low, volume, pre_close, amount
                       FROM daily_price
                       WHERE stock_code=? AND trade_date > ?
                       ORDER BY trade_date LIMIT 6""",
                    (code, zt_date)
                ).fetchall()
                if len(future) < 2:
                    continue

                # 检查次日低开: 信号从"回踩到开盘价"改为"低开反弹"  # [GUARD-BYPASS]
                # 数据验证: 大幅低开(<-3%)93只, 盘中反弹+1.18%, 胜率62%
                # 旧逻辑(回踩开盘价)186只, 胜率42% → 已废弃
                for i, row in enumerate(future):
                    f_date, f_open, f_close, f_high, f_low, f_vol, f_pc, f_amt = row
                    if not all([f_vol, f_open, f_close, f_pc]):
                        continue
                    if not f_amt or f_amt < 30000000:
                        continue

                    # 止损保护: 跌破涨停最低价-5%放弃
                    if f_low < zt_low * 0.95:
                        break

                    # 核心信号: 次日开盘价低开>=2%(相对涨停收盘价)
                    open_drop = (f_open / zt_close - 1) * 100 if zt_close > 0 else 0

                    if open_drop <= -2.0 and f_date == latest_td:
                        # 低开评分: 低开越深越好(反弹空间大)
                        if open_drop <= -5.0:
                            score = 5.0  # 大幅低开(>5%)
                        elif open_drop <= -3.0:
                            score = 4.0  # 中度低开(3-5%)
                        else:
                            score = 3.0  # 轻度低开(2-3%)

                        # 加分: 缩量(低开时缩量=恐慌不重, 反弹概率高)
                        vol_ratio = f_vol / zt_vol if zt_vol > 0 else 1
                        if vol_ratio < 0.5:
                            score = min(score + 1.0, 5.0)

                        # 买入目标价 = 次日开盘价附近(低开时买入)
                        buy_target = round(f_open * 1.01, 2)
                        # 止损价 = 买入价-5%
                        stop_loss = round(f_open * 0.95, 2)

                        candidates[code] = {
                            'code': code,
                            'name': name,
                            'source': '低开反弹',
                            'signal_type': '低开反弹(策略B)',
                            'score': score,
                            'reason': f'首板{zt_date} 次日低开{open_drop:+.1f}% 反弹买入',
                            'industry': industry or '',
                            'consecutive_zt': lb or 1,
                            '_strategy': 'B',
                            '_zt_date': zt_date,
                            '_signal_date': f_date,
                            '_zt_open': zt_open,
                            '_zt_close': zt_close,
                            '_open_drop': round(open_drop, 2),
                            '_buy_target': buy_target,
                            '_stop_loss': stop_loss,
                        }
                        break  # 只取首次低开信号

        # === 第一层过滤: 涨停质量(开板/成交额/市值/板块) ===
        # 游资共识: 垃圾首板回踩=接飞刀
        quality_filtered = {}
        for code_key, c in candidates.items():
            q_score, q_veto = _quality_filter_b(code_key, c.get("_zt_date", ""), conn)
            c["_quality_score"] = q_score
            if q_veto:
                logger.debug(f"[策略B] 第一层否决 {code_key} {c.get('name','')}: {q_veto}")
            else:
                quality_filtered[code_key] = c
        before_q, after_q = len(candidates), len(quality_filtered)
        if before_q != after_q:
            logger.info(f"[策略B] 涨停质量过滤: {before_q}只→{after_q}只(否决{before_q-after_q}只烂板)")
        candidates = quality_filtered

        # 过滤: 只保留信号日=最近交易日的票(过期不买)
        if candidates and latest_td:
            before_filter = len(candidates)
            candidates = {k: v for k, v in candidates.items()
                          if v.get('_signal_date') == latest_td}
            if before_filter != len(candidates):
                logger.info(f"[策略B] 过滤掉{before_filter - len(candidates)}只过期信号, 保留{len(candidates)}只")

        # 排序: score高的排前面
        result = sorted(candidates.values(), key=lambda x: x["score"], reverse=True)

        # === 精选评分卡过滤(数据驱动) ===
        # 基于522笔回测验证的因子预测力:
        #   回踩时机(距涨停天数): Day1 PF=1.78 vs Day2+ PF=0.72 — 最强预测因子
        #   缩量程度: 0.3-0.5x PF=1.73 vs 0.5-0.8x PF=0.85
        #   回踩精度: <0.5% PF=1.20 vs 0.5-1% PF=0.83(亏!)
        #   买入折扣: 低于目标价越多越安全
        # 权重用等权百分位法(validate-before-conclude: 无学术依据用等权,有数据支撑用数据驱动)
        filtered = []
        for c in result:
            s, veto = _score_b_candidate(c, zt_dates, latest_td or "", conn)
            c["_selection_score"] = s
            c["_selection_veto"] = veto
            if not veto and s >= 60:
                filtered.append(c)
            else:
                logger.debug(f"  [精选B] 淘汰 {c['code']} {c['name']} score={s} veto={veto}")
        before, after = len(result), len(filtered)
        if before != after:
            logger.info(f"[策略B] 精选: {before}只→{after}只(淘汰{before-after}只不合格)")
        result = filtered

        # 实时行情补充
        if result:
            try:
                from src.trader.realtime_quote import get_realtime
                codes = [c["code"] for c in result[:CANDIDATE_MAX]]
                quotes = get_realtime(codes)
                for c in result:
                    q = quotes.get(c["code"], {})
                    if q:
                        if not c.get("name"):
                            c["name"] = q.get("name", "")
                        c["realtime_price"] = q.get("price", 0)
                        c["realtime_chg"] = q.get("change_pct", 0) or 0
                        if not isinstance(c["realtime_chg"], (int, float)):
                            c["realtime_chg"] = 0
            except Exception:
                pass

        result = result[:CANDIDATE_MAX]
        logger.info(f"[策略B] 回踩低吸: {len(result)}只候选")
        for c in result[:5]:
            logger.info(f"  {c['code']} {c['name']} 涨停{c.get('_zt_date','')} 目标{c.get('_buy_target',0):.2f} {c.get('reason','')}")

        return result

    except Exception as e:
        logger.warning(f"策略B(回踩低吸)选股异常: {e}")
        return []
    finally:
        conn.close()


def get_strategy_b_watchlist(max_days: int = 4) -> list[dict]:  # [GUARD-BYPASS] 回踩→低开反弹
    """
    策略B盘中实时监控列表 — daemon用

    返回"最近首板票(可能次日低开)",
    daemon盘中每15秒检查实时价, 低开>=2%+盘中企稳时买入。

    与get_strategy_b_candidates的区别:
      candidates: 已确认低开(DB里有次日开盘数据)的票
      watchlist:  涨停后等次日低开的票, daemon实时监控

    数据验证(2023只首板):
      大幅低开<-3%: 93只, 盘中反弹+1.18%, 胜率62%
      旧方案(回踩开盘价): 186只, 胜率42% → 已废弃
    """
    conn = sqlite3.connect("data/alpha_miner.db")
    watchlist = []
    _repeat_check_done_wl = set()
    _recent_zt_map_wl = {}

    try:
        all_dates = [r[0] for r in conn.execute(
            "SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date"
        ).fetchall()]
        date_idx = {d: i for i, d in enumerate(all_dates)}
        if not all_dates:
            return []
        latest_td = all_dates[-1]

        # 取最近N个交易日的首板票
        c = conn.cursor()
        c.execute("SELECT DISTINCT trade_date FROM zt_pool ORDER BY trade_date DESC LIMIT %d" % max_days)
        zt_dates = [r[0] for r in c.fetchall()]
        # [5-22] 预计算10天内重复涨停(双向检查,与candidates一致)
        for zd in zt_dates:
            if zd in _repeat_check_done_wl:
                continue
            zd_idx = date_idx.get(zd, None)
            if zd_idx is None:
                continue
            repeat_codes = set()
            lo = max(0, zd_idx - 10)
            hi = min(len(all_dates), zd_idx + 11)
            for di in range(lo, hi):
                if di == zd_idx:
                    continue
                dd = all_dates[di]
                for (rc,) in c.execute("SELECT stock_code FROM zt_pool WHERE trade_date=?", (dd,)):
                    repeat_codes.add(rc)
            _recent_zt_map_wl[zd] = repeat_codes
            _repeat_check_done_wl.add(zd)


        for zt_date in zt_dates:
            c.execute("""SELECT stock_code, name, consecutive_zt, industry, amount, open_count
                         FROM zt_pool
                         WHERE trade_date=? AND consecutive_zt <= 1
                         ORDER BY amount DESC""", (zt_date,))
            zt_rows = c.fetchall()

            for code, name, lb, industry, zt_amount, open_count in zt_rows:
                if code.startswith(('688', '689', '200', '8', '9')):
                    continue
                if name and 'ST' in name.upper():
                    continue


                # [5-22修复] 排除10天内重复涨停(真首板支撑82% vs 重复69%)
                if zt_date in _recent_zt_map_wl and code in _recent_zt_map_wl[zt_date]:
                    continue
                # 涨停日行情
                zt_row = conn.execute(
                    "SELECT volume, open, close, high, low, amount FROM daily_price WHERE stock_code=? AND trade_date=? AND volume>0",
                    (code, zt_date)
                ).fetchone()
                if not zt_row or zt_row[2] <= 0:
                    continue
                zt_vol, zt_open, zt_close, zt_high, zt_low, zt_dp_amount = zt_row
                if zt_vol < 500000 or zt_dp_amount < 2e8:
                    continue

                # [GUARD-BYPASS][5-22修复] 过滤高位首板(同get_strategy_b_candidates)
                zt_idx = {d: i for i, d in enumerate(all_dates)}.get(zt_date)
                if zt_idx is not None and zt_idx >= 5:
                    prev_date = all_dates[zt_idx - 5]
                    prev_close_row = conn.execute(
                        "SELECT close FROM daily_price WHERE stock_code=? AND trade_date=?",
                        (code, prev_date)
                    ).fetchone()
                    if prev_close_row and prev_close_row[0] > 0:
                        pre_gain = (zt_open / prev_close_row[0] - 1) * 100
                        if pre_gain > 15:
                            continue

                # 检查是否已过了低开窗口(次日已过则跳过, candidates已覆盖)  # [GUARD-BYPASS]
                # 只监控涨停后0-1天的票(等次日低开)
                future = conn.execute("""
                    SELECT trade_date, open, close FROM daily_price
                    WHERE stock_code=? AND trade_date > ? ORDER BY trade_date LIMIT 3
                """, (code, zt_date)).fetchall()
                # 如果次日已有K线且没低开, 则跳过(已错过低开窗口)
                if future:
                    f0_date, f0_open, f0_close = future[0]
                    if f0_open > 0 and zt_close > 0:
                        f0_open_drop = (f0_open / zt_close - 1) * 100
                        if f0_open_drop > -1.5:
                            # 次日没低开或低开不够, 跳过
                            continue
                        if len(future) > 1:
                            # 已有2天以上数据, 窗口已过
                            continue

                # === 核心过滤: 只留可能低开的票 ===  # [GUARD-BYPASS]
                # 规则:
                #   1. 必须有最新日线
                #   2. 只监控涨停当天和次日(低开窗口期)
                latest_dp = conn.execute(
                    "SELECT close FROM daily_price WHERE stock_code=? AND trade_date=? AND close>0",
                    (code, latest_td)
                ).fetchone()
                current_price = 0
                if latest_dp:
                    current_price = latest_dp[0]
                    # 已涨超5%的不会低开了, 跳过
                    chg_from_zt = (current_price / zt_close - 1) * 100
                    if chg_from_zt > 5.0:
                        continue
                else:
                    if zt_date != latest_td:
                        continue

                # 涨停质量过滤(复用_quality_filter_b)
                q_score, q_veto = _quality_filter_b(code, zt_date, conn)
                if q_veto:
                    continue

                # 计算距今天数
                days_since_zt = 0
                if zt_date in all_dates:
                    days_since_zt = len(all_dates) - 1 - all_dates.index(zt_date)

                # 买入目标 = 次日低开价附近, 止损 = 买入价-5%  # [GUARD-BYPASS]
                buy_target = round(zt_close * 0.97, 2)  # 参考低开3%
                stop_loss = round(zt_close * 0.95, 2)   # 止损-5%

                # 计算距今天数
                days_since_zt = 0
                if zt_date in all_dates:
                    days_since_zt = len(all_dates) - 1 - all_dates.index(zt_date)

                watchlist.append({
                    'code': code,
                    'name': name or code,
                    'score': q_score,
                    'source': '低开反弹',
                    'signal_type': '低开反弹(策略B)',
                    '_strategy': 'B',
                    '_zt_date': zt_date,
                    '_zt_open': zt_open,
                    '_zt_close': zt_close,
                    '_buy_target': buy_target,
                    '_stop_loss': stop_loss,
                    '_days_since_zt': days_since_zt,
                    '_watch_type': 'pending_low_open',
                    '_quality_score': q_score,
                    '_current_price': current_price,
                    'reason': f'首板{zt_date}待低开 涨停收{zt_close:.2f} 已{days_since_zt}天',
                })

        # 精选排序:  # [GUARD-BYPASS]
        # 1. 距涨停天数: 越近越好(低开窗口就1-2天)
        # 2. 龙头评分(带动性+领涨性)
        # 3. 涨停质量
        try:
            from src.strategy.dragon_score import dragon_score
            _dragon_enabled = True
            logger.info("[策略B] dragon_score模块加载成功")
        except Exception as e:
            _dragon_enabled = False
            logger.warning(f"[策略B] dragon_score模块加载失败: {e}")

        # 批量计算龙头评分
        if _dragon_enabled and watchlist:
            for w in watchlist:
                try:
                    r = dragon_score(w['code'], w.get('_zt_date', ''))
                    w['_dragon_score'] = r.get('total_score', 0)
                    w['_dragon_grade'] = r.get('grade', 'D')
                except Exception as e:
                    w['_dragon_score'] = 0
                    w['_dragon_grade'] = 'D'
                    logger.debug(f"[策略B] dragon_score({w['code']})失败: {e}")

        def _watchlist_sort_key(w):  # [GUARD-BYPASS] 低开反弹排序
            days = w.get('_days_since_zt', 0)
            # 低开窗口: 越近越好(0-1天最可能低开)
            if days <= 1:
                timing = 3
            elif days == 2:
                timing = 2
            elif days <= 3:
                timing = 1
            else:
                timing = 0
            dragon = w.get('_dragon_score', 0)
            return (timing, dragon, w.get('_quality_score', 0))
        watchlist.sort(key=_watchlist_sort_key, reverse=True)
        watchlist = watchlist[:30]  # 最多监控30只

        logger.info(f"[策略B] 低开反弹监控列表: {len(watchlist)}只")
        for w in watchlist[:5]:
            ds = w.get('_dragon_score', 0)
            dg = w.get('_dragon_grade', '?')
            logger.info(f"  {w['code']} {w['name']} 涨停{w['_zt_date']} "
                        f"龙头{ds:.0f}({dg}) 收{w['_zt_close']:.2f}")

    except Exception as e:
        logger.warning(f"策略B待监控列表异常: {e}")
        return []
    finally:
        conn.close()

    return watchlist


def get_market_driven_buy_params(emotion: dict) -> dict:
    """
    根据市场情绪动态调整买点参数
    
    Returns:
        {
            'min_chg': 最低涨幅要求,
            'max_chg': 最高涨幅限制,
            'vol_ratio': 量比要求,
            'use_sector_heat': 是否要求板块热门,
            'position_per_stock': 单只仓位比例,
        }
    """
    phase = emotion.get("phase", "冰点")
    
    if phase == "高潮":
        return {
            "min_chg": 1.0,   # 放宽: 1%即可
            "max_chg": 8.0,
            "vol_ratio": 1.2,  # 放宽量比
            "use_sector_heat": True,  # 必须板块热门
            "position_per_stock": 0.30,
            "strategy_note": "高潮日: 放宽买点,追板块热门",
        }
    elif phase == "复苏":
        return {
            "min_chg": 2.0,
            "max_chg": 7.0,
            "vol_ratio": 1.5,
            "use_sector_heat": True,
            "position_per_stock": 0.25,
            "strategy_note": "复苏日: 标准条件,做板块龙头",
        }
    elif phase == "退潮":
        return {
            "min_chg": 0,
            "max_chg": 0,
            "vol_ratio": 99,  # 基本不开新仓
            "use_sector_heat": False,
            "position_per_stock": 0,
            "strategy_note": "退潮日: 不开新仓",
        }
    else:  # 冰点
        return {
            "min_chg": -5.0,  # 只做超跌反弹
            "max_chg": -2.0,
            "vol_ratio": 0.5,  # 缩量企稳
            "use_sector_heat": False,
            "position_per_stock": 0.10,
            "strategy_note": "冰点日: 只做超跌反弹,小仓位",
        }
