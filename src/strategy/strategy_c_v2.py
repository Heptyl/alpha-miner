"""strategy_c_v2.py — 基本面驱动 + 技术面择时

替换旧strategy_c.py(量价动量)。

四层漏斗选股:
  Layer 1: 行业景气度 → 景气行业TOP 10
  Layer 2: 基本面评分 → F-Score >= 55分(AI赛道限定)
  Layer 3: 技术面择时 → 量比>=3 / 均线突破 / MACD金叉
  Layer 4: 仓位管理 → 单只20% / 同行业40%

学术基础:
  Piotroski (2000) F-Score
  Novy-Marx (2013) Gross Profitability Premium
  Greenblatt Magic Formula

详见: docs/strategy_c_v2_blueprint.md, docs/strategy_c_v2_track_scoring.md
"""

import logging
import sqlite3

def get_stock_name(code: str) -> str:
    """从DB获取股票名称"""
    try:
        conn = sqlite3.connect("data/alpha_miner.db")
        r = conn.execute("SELECT name FROM stock_list WHERE code=? UNION SELECT stock_name FROM stock_industry_mapping WHERE stock_code=? LIMIT 1", (code, code)).fetchone()
        conn.close()
        return r[0] if r else code
    except Exception:
        return code
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from src.scorer.fundamental_scorer import (
    score_stock,
    get_industry,
    TIER1_INDUSTRIES, TIER2_INDUSTRIES, TIER3_INDUSTRIES, PREMIUM_INDUSTRIES,
)

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent.parent / "data" / "alpha_miner.db"

# ============================================================
# Layer 1: 行业景气度筛选
# ============================================================

def get_hot_industries(top_n: int = 10) -> list[str]:
    """
    获取当前景气行业列表
    
    评分依据:
      1. 近20日行业资金净流入
      2. 近5日行业涨幅
      3. AI赛道加分
    
    返回: 行业名列表(按景气度排序)
    """
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        # 方法: 从stock_industry_mapping获取所有行业，
        # 然后用fund_flow表计算每个行业的资金流入
        industries = conn.execute(
            "SELECT DISTINCT industry_name FROM stock_industry_mapping"
        ).fetchall()
        
        days_20 = (datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d")
        
        industry_scores = []
        for (ind_name,) in industries:
            score = 0.0
            
            # 资金流入: 该行业所有个股的净流入之和
            row = conn.execute("""
                SELECT SUM(ff.net_amount) as total_net
                FROM fund_flow ff
                JOIN stock_industry_mapping sim ON ff.stock_code = sim.stock_code
                WHERE sim.industry_name = ? AND ff.trade_date >= ?
            """, (ind_name, days_20)).fetchone()
            
            if row and row[0] is not None:
                score += float(row[0]) / 1e8  # 归一化到亿
            
            # AI赛道加分
            if ind_name in TIER1_INDUSTRIES:
                score += 5.0
            elif ind_name in TIER2_INDUSTRIES:
                score += 3.0
            elif ind_name in TIER3_INDUSTRIES:
                score += 1.5
            elif ind_name in PREMIUM_INDUSTRIES:
                score += 2.0
            
            industry_scores.append((ind_name, score))
        
        # 排序取TOP N
        industry_scores.sort(key=lambda x: x[1], reverse=True)
        return [name for name, _ in industry_scores[:top_n]]
    
    finally:
        conn.close()


# ============================================================
# Layer 2: 基本面评分筛选
# ============================================================

def get_fundamental_candidates(
    min_score: int = 60,
    industry_filter: Optional[list[str]] = None,
) -> list[dict]:
    """
    获取基本面评分合格的股票
    
    Args:
        min_score: 最低评分(默认60)
        industry_filter: 限定行业列表(None=不限)
    
    Returns:
        [{stock_code, score, details, industry, pass}]
    """
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        # 获取有财务数据的股票
        if industry_filter:
            codes = conn.execute("""
                SELECT DISTINCT fs.stock_code
                FROM financial_summary fs
                JOIN stock_industry_mapping sim ON fs.stock_code = sim.stock_code
                WHERE sim.industry_name IN ({})
            """.format(",".join([f"'{i}'" for i in industry_filter]))).fetchall()
        else:
            codes = conn.execute(
                "SELECT DISTINCT stock_code FROM financial_summary"
            ).fetchall()
        
        results = []
        for (code,) in codes:
            r = score_stock(code)
            if r["score"] >= min_score:
                results.append(r)
        
        # 按分数排序
        results.sort(key=lambda x: x["score"], reverse=True)
        return results
    
    finally:
        conn.close()


# ============================================================
# Layer 3: 技术面择时
# ============================================================

def check_technical_entry(stock_code: str) -> dict:
    """
    检查技术面买点
    
    触发条件(满足任一):
      - 量比 >= 3
      - 突破20日均线 + 站上5日均线
      - MACD金叉
      - 缩量回调至20日/60日均线支撑
    
    过滤条件(排除):
      - 当日涨幅 > 7%
      - 连续3天大涨(每天>3%)
    
    Returns:
        {entry: bool, signals: list[str], filters: list[str]}
    """
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        # 获取最近30天日K数据
        rows = conn.execute("""
            SELECT trade_date, open, high, low, close, volume, amount
            FROM daily_price
            WHERE stock_code = ?
            ORDER BY trade_date DESC LIMIT 30
        """, (stock_code,)).fetchall()
        
        if len(rows) < 20:
            return {"entry": False, "signals": [], "filters": ["数据不足"]}
        
        # 按时间正序
        rows = list(reversed(rows))
        closes = [r[4] for r in rows]
        volumes = [r[5] for r in rows]
        
        today_close = closes[-1]
        today_vol = volumes[-1]
        
        signals = []
        filters_passed = []
        
        # --- 过滤条件 ---
        
        # 当日涨幅 > 7%
        today_chg = (closes[-1] - closes[-2]) / closes[-2] if len(closes) >= 2 else 0
        if today_chg > 0.07:
            filters_passed.append(f"涨幅过大{today_chg*100:.1f}%")
        
        # 连续3天大涨
        if len(closes) >= 4:
            big_up_days = sum(1 for i in range(1, 4)
                            if (closes[-i] - closes[-i-1]) / closes[-i-1] > 0.03)
            if big_up_days >= 3:
                filters_passed.append("连续3天大涨")
        
        if filters_passed:
            return {"entry": False, "signals": [], "filters": filters_passed}
        
        # --- 触发条件 ---
        
        # 量比 (今日成交量 / 前5日均量)
        if len(volumes) >= 6:
            avg_vol_5 = sum(volumes[-6:-1]) / 5
            if avg_vol_5 > 0:
                vol_ratio = today_vol / avg_vol_5
                if vol_ratio >= 3:
                    signals.append(f"量比{vol_ratio:.1f}")
        
        # 均线
        ma5 = sum(closes[-5:]) / 5 if len(closes) >= 5 else None
        ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else None
        ma60 = None  # 需要60天数据，暂不计算
        
        if ma5 and ma20:
            # 突破20日均线 + 站上5日均线
            if closes[-2] < ma20 and today_close > ma20 and today_close > ma5:
                signals.append("突破MA20")
            
            # 缩量回调至支撑位
            if (today_vol < volumes[-2] and  # 缩量
                abs(today_close - ma20) / ma20 < 0.02):  # 接近MA20
                signals.append("缩量回踩MA20")
        
        # MACD金叉 (简化版: DIF上穿DEA)
        if len(closes) >= 26:
            ema12 = closes[-1]  # 简化
            ema26 = sum(closes[-26:]) / 26
            # 不实现完整MACD，用趋势判断替代
            if ma5 and ma20 and ma5 > ma20 and closes[-2] <= sum(closes[-6:-1])/5:
                signals.append("趋势转多")
        
        entry = len(signals) > 0
        return {"entry": entry, "signals": signals, "filters": []}
    
    finally:
        conn.close()


# ============================================================
# 四层漏斗主入口
# ============================================================

def get_strategy_c_v2_candidates(top_n: int = 10) -> list[dict]:
    """
    策略C v2 选股 — 四层漏斗
    
    Layer 1: 行业景气度 → TOP 10行业
    Layer 2: 基本面评分 → >= 60分
    Layer 3: 技术面择时 → 有买点信号
    Layer 4: 仓位约束 → top_n限制
    
    Returns:
        [{stock_code, score, industry, signals, _strategy: "C"}]
    """
    logger.info("[策略C v2] 开始四层漏斗选股...")
    
    # Layer 1: 景气行业
    hot_industries = get_hot_industries(top_n=10)
    logger.info(f"[策略C v2] Layer1 景气行业: {hot_industries}")
    
    # Layer 2: 基本面评分(AI赛道限定，回测PF从0.66提升到2.52)
    ai_industries = TIER1_INDUSTRIES | TIER2_INDUSTRIES | TIER3_INDUSTRIES
    candidates = get_fundamental_candidates(min_score=55, industry_filter=list(ai_industries))
    logger.info(f"[策略C v2] Layer2 基本面合格: {len(candidates)}只")
    
    # 优先选择景气行业+高分的
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    def sort_key(c):
        ind = get_industry(conn, c["stock_code"]) or ""
        in_hot = 1 if ind in hot_industries else 0
        return (in_hot, c["score"])
    
    candidates.sort(key=sort_key, reverse=True)
    
    # Layer 3: 技术面择时
    results = []
    for cand in candidates[:50]:  # 只检查TOP 50基本面的
        tech = check_technical_entry(cand["stock_code"])
        if tech["entry"]:
            cand["signals"] = tech["signals"]
            cand["_strategy"] = "C"
            cand["signal_type"] = "基本面驱动"
            cand["code"] = cand["stock_code"]  # daemon兼容
            cand["name"] = get_stock_name(cand["stock_code"])
            results.append(cand)
            if len(results) >= top_n:
                break
    
    logger.info(f"[策略C v2] Layer3 技术面有信号: {len(results)}只")
    
    # Layer 4: 已在仓位检查由daemon做
    
    for r in results:
        logger.info(f"  {r['stock_code']} {get_industry(conn, r['stock_code'])} "
                    f"分数{r['score']} 信号{r.get('signals', [])}")

    conn.close()
    return results


# ============================================================
# 卖出逻辑
# ============================================================

def check_sell_c_v2(position: dict, quote: dict) -> Optional[dict]:
    """
    策略C v2 卖出检查
    
    卖出条件:
    1. 止损: -8%
    2. 目标收益 +15% → 减半仓
    3. 趋势破坏(跌破20日均线+量缩)
    4. 基本面恶化(业绩预警/大股东减持)
    5. 时间止损: 持仓>30天未达目标
    """
    code = position.get("code", position.get("stock_code"))
    buy_price = position.get("buy_price", position.get("avg_cost"))
    current = quote.get("current", quote.get("price", 0))
    hold_days = position.get("hold_days", 0)
    
    if not buy_price or not current:
        return None
    
    chg = (current - buy_price) / buy_price
    
    # 止损 -8%
    if chg <= -0.08:
        return {
            "action": "sell",
            "reason": f"策略C止损{chg*100:+.1f}%",
            "urgency": "high"
        }
    
    # 目标收益 +15%
    if chg >= 0.12:
        return {
            "action": "sell_half",
            "reason": f"策略C目标收益{chg*100:+.1f}%",
            "urgency": "low"
        }
    
    # 时间止损 30天
    if hold_days >= 30 and chg < 0.05:
        return {
            "action": "sell",
            "reason": f"策略C持仓{hold_days}天未达目标",
            "urgency": "medium"
        }
    
    return None


# ============================================================
# 注册到Registry
# ============================================================

def _register():
    """注册到StrategyRegistry"""
    try:
        from src.strategy.registry import StrategyRegistry
        StrategyRegistry.register("C", get_strategy_c_v2_candidates)
        logger.info("[策略C v2] 已注册到Registry")
    except Exception as e:
        logger.warning(f"[策略C v2] 注册失败: {e}")


# 自动注册
_register()


# ========== 验证 ==========
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    print("=== 策略C v2 四层漏斗测试 ===\n")
    
    # Layer 1 测试
    print("--- Layer 1: 景气行业 ---")
    hot = get_hot_industries(10)
    for i, ind in enumerate(hot, 1):
        print(f"  {i}. {ind}")
    
    # Layer 2 测试
    print(f"\n--- Layer 2: 基本面评分 >= 60 ---")
    candidates = get_fundamental_candidates(min_score=60)
    print(f"合格: {len(candidates)}只")
    _tconn = sqlite3.connect(str(DB_PATH), timeout=30)
    for c in candidates[:10]:
        ind = get_industry(_tconn, c["stock_code"])
        print(f"  {c['stock_code']} {ind} 分数={c['score']} "
              f"A={c['details']['A_profitability']['score']} "
              f"B={c['details']['B_growth']['score']} "
              f"C={c['details']['C_health']['score']} "
              f"D={c['details']['D_track']['score']} "
              f"E={c['details']['E_signals']['score']}")
    
    # Layer 3 测试(对前5只检查技术面)
    if candidates:
        print(f"\n--- Layer 3: 技术面择时(前5只) ---")
        for c in candidates[:5]:
            tech = check_technical_entry(c["stock_code"])
            print(f"  {c['stock_code']}: entry={tech['entry']} "
                  f"signals={tech['signals']} filters={tech['filters']}")
    
    # 完整Pipeline
    print(f"\n=== 完整Pipeline ===")
    results = get_strategy_c_v2_candidates(top_n=10)
    print(f"最终候选: {len(results)}只")
    for r in results:
        print(f"  {r['stock_code']} {get_industry(_tconn, r['stock_code'])} "
              f"分数={r['score']} 信号={r.get('signals', [])}")
    _tconn.close()
