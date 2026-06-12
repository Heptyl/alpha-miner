"""调试第2步：模拟追高保护+价格校验，找到真正的过滤点。"""
import sqlite3
import sys
sys.path.insert(0, '/home/ccy/alpha-miner')

conn = sqlite3.connect('data/alpha_miner.db')
report_date = '2026-05-08'

# 取通过基本过滤的40只代码
# 先复用前面的打分逻辑拿到top40
from src.data.storage import Storage
from src.strategy.recommend import RecommendEngine
from datetime import datetime

db = Storage()
engine = RecommendEngine(db)

# 直接调recommend看完整流程
as_of = datetime(2026, 5, 8, 15, 0, 0)
result = engine.recommend(as_of=as_of, report_date=report_date, top_n=10)

print(f"推荐结果: {len(result.stocks)}只")
print(f"涨停={result.zt_count} 跌停={result.dt_count} 市场状态={result.market_regime}")

if not result.stocks:
    # 逐步调试
    print("\n=== 逐步调试 ===")
    
    # 1. 候选池
    candidates = engine._build_candidates(as_of, report_date)
    print(f"[1] 候选池: {len(candidates)}只")
    
    if candidates:
        # 2. 因子
        factor_map = engine._load_factors(report_date)
        candidate_factors = {k: v for k, v in factor_map.items() if k in candidates}
        print(f"[2] 有因子值的候选: {len(candidate_factors)}/{len(candidates)}只")
        
        # 3. 资金流
        fund_map = engine._load_fund_flow(report_date)
        candidate_fund = {k: v for k, v in fund_map.items() if k in candidates}
        print(f"[3] 有资金流的候选: {len(candidate_fund)}/{len(candidates)}只")
        
        # 4. 逐个构建推荐
        recs = []
        for code, info in candidates.items():
            from src.strategy.recommend import StockRecommendation
            from src.strategy.technical import compute_technical
            
            factors = factor_map.get(code, {})
            fund = fund_map.get(code, 0.0)
            
            rec = StockRecommendation(
                stock_code=code,
                stock_name=info.get("name", ""),
                industry=info.get("industry", ""),
                concepts=[],
                factor_scores=factors,
                consecutive_zt=info.get("consecutive_zt", 0),
                open_count=info.get("open_count", 0),
                amount=info.get("amount", 0),
                circulation_mv=info.get("circulation_mv", 0),
                fund_net_amount=fund,
            )
            
            # 技术分析
            price_df = engine._load_price_history(code, as_of, 30)
            if not price_df.empty:
                ta = compute_technical(price_df)
                rec.technical = ta
                if ta:
                    rec.factor_scores["momentum_score"] = ta.momentum_score
                    rec.factor_scores["volume_ratio"] = min(ta.volume_ratio / 3.0, 1.0)
            
            # 打分
            rec.composite_score = engine._compute_score(rec)
            engine._compute_price_levels(rec)
            rec.signal_level = engine._signal_level(rec.composite_score)
            
            recs.append(rec)
        
        print(f"\n[4] 构建推荐: {len(recs)}只")
        
        # 5. 基本面过滤
        codes = [r.stock_code for r in recs]
        passed_codes, rejected = engine._filter_fundamentals(codes)
        rejected_count = len(codes) - len(passed_codes)
        before_fund = len(recs)
        recs = [r for r in recs if r.stock_code in passed_codes]
        print(f"[5] 基本面过滤: {before_fund}→{len(recs)} (拒绝{rejected_count}只)")
        if rejected:
            print(f"    被拒示例: {list(rejected.items())[:5]}")
        
        # 6. 追高保护
        if recs:
            try:
                from src.strategy.chase_protection import batch_chase_risk
                zt_codes = {r.stock_code for r in recs if r.consecutive_zt >= 1}
                chase_risks = batch_chase_risk(codes, report_date, zt_codes=zt_codes)
                extreme_count = 0
                for rec in recs:
                    risk = chase_risks.get(rec.stock_code)
                    if risk:
                        rec.composite_score = rec.composite_score * (1 - risk.score_penalty)
                        if risk.risk_level == "extreme":
                            extreme_count += 1
                print(f"[6] 追高保护: extreme={extreme_count}只")
                # 分数下降最多的
                for rec in recs[:10]:
                    risk = chase_risks.get(rec.stock_code)
                    penalty = risk.score_penalty if risk else 0
                    print(f"    {rec.stock_code} {rec.stock_name:8s} penalty={penalty:.2f} score_after={rec.composite_score:.3f}")
            except Exception as e:
                print(f"[6] 追高保护跳过: {e}")
        
        # 7. 历史胜率回测
        if recs:
            try:
                from src.strategy.win_rate_backtest import backtest_pattern
                low_win_count = 0
                for rec in recs:
                    bt = backtest_pattern(
                        rec.stock_code, report_date, rec.consecutive_zt,
                        hold_days=3, db_path=db.db_path,
                    )
                    if bt:
                        if bt.win_rate < 50:
                            penalty = (50 - bt.win_rate) / 100
                            rec.composite_score = rec.composite_score * (1 - penalty)
                            low_win_count += 1
                print(f"[7] 历史胜率: 低胜率惩罚={low_win_count}只")
            except Exception as e:
                print(f"[7] 历史胜率跳过: {e}")
        
        # 8. _apply_filters
        if recs:
            before_filter = len(recs)
            filtered = engine._apply_filters(recs)
            print(f"[8] _apply_filters: {before_filter}→{len(filtered)}")
        
        # 9. 价格校验 (_validate_prices)
        if recs:
            print(f"\n[9] 价格校验前: {len(recs)}只")
            engine._validate_prices(recs, report_date)
            valid = [r for r in recs if r.composite_score >= 0 and r.technical is not None]
            invalid = [r for r in recs if r.composite_score < 0 or r.technical is None]
            print(f"[9] 价格校验后: 有效={len(valid)}只, 无效={len(invalid)}只")
            if invalid:
                print(f"    被标无效的(前10):")
                for r in invalid[:10]:
                    reason = "无K线" if r.technical is None else f"score={r.composite_score:.3f}"
                    print(f"      {r.stock_code} {r.stock_name:8s} {reason}")
        
        # 10. 最终分数分布
        print(f"\n[10] 最终分数分布:")
        for rec in sorted(recs, key=lambda x: x.composite_score, reverse=True)[:15]:
            ta_str = f"价格={rec.technical.current_price:.2f}" if rec.technical else "无TA"
            print(f"    {rec.stock_code} {rec.stock_name:8s} score={rec.composite_score:.3f} {ta_str}")

conn.close()
