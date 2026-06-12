#!/usr/bin/env python3
"""
Walk-Forward验证 — 检验回测是否过拟合
方法: 
  1. 把208天分成多段(滚动窗口)
  2. 每段独立跑回测
  3. 看各段的收益是否一致(稳定赚钱)还是某一段暴利拉高了整体

同时计算Deflated Sharpe Ratio:
  - 试了4组参数(2%/3%/4%/5%), 最优Sharpe=1.853
  - Deflated Sharpe考虑了"试多组参数取最优"的运气成分
"""
import sqlite3
import math
import json
from collections import defaultdict
from datetime import datetime

# 导入backtest_engine_v3的核心组件
import sys
sys.path.insert(0, '.')
from scripts.backtest_engine_v3 import (
    load_daily_prices, load_zt_pool, BacktestEngine, INITIAL_CASH
)

DB_PATH = "data/alpha_miner.db"


def walk_forward_test(by_date, by_stock, zt_by_date, sorted_dates, n_splits=4):
    """
    Walk-Forward: 把时间均分成n_splits段, 每段独立回测
    如果每段都赚钱 → 策略稳定, 不是过拟合
    如果只有某段赚钱 → 可能过拟合或市场环境依赖
    """
    total_days = len(sorted_dates)
    segment_size = total_days // n_splits
    
    print("=" * 65)
    print(f"  Walk-Forward验证 ({n_splits}段滚动)")
    print("=" * 65)
    print(f"  总天数: {total_days}天, 每段约{segment_size}天")
    print()
    
    results = []
    for i in range(n_splits):
        start_idx = i * segment_size
        end_idx = (i + 1) * segment_size if i < n_splits - 1 else total_days
        
        seg_start = sorted_dates[start_idx]
        seg_end = sorted_dates[end_idx - 1]
        seg_days = end_idx - start_idx
        
        # 用5%止盈(全样本最优参数)
        engine = BacktestEngine(trailing_stop_pct=0.05)
        report = engine.run(by_date, by_stock, zt_by_date, seg_start, seg_end)
        
        # 按策略拆分
        b_stats = report.get("strategy_stats", {}).get("B", {})
        a_stats = report.get("strategy_stats", {}).get("A", {})
        
        result = {
            "segment": i + 1,
            "period": f"{seg_start}~{seg_end}",
            "days": seg_days,
            "total_return": report.get("total_return_pct", 0),
            "sharpe": report.get("sharpe_ratio", 0),
            "max_dd": report.get("max_drawdown_pct", 0),
            "trades": report.get("total_trades", 0),
            "b_trades": b_stats.get("trades", 0),
            "b_winrate": b_stats.get("win_rate", 0),
            "b_pnl": b_stats.get("total_pnl", 0),
            "a_trades": a_stats.get("trades", 0),
            "a_pnl": a_stats.get("total_pnl", 0),
        }
        results.append(result)
        
        print(f"  第{i+1}段: {seg_start}~{seg_end} ({seg_days}天)")
        print(f"    收益: {result['total_return']:+.2f}%  Sharpe: {result['sharpe']:.3f}  回撤: {result['max_dd']:.2f}%")
        print(f"    交易: {result['trades']}笔 (策略B: {result['b_trades']}笔, 胜率{result['b_winrate']}%)")
        print(f"    策略B盈亏: ¥{result['b_pnl']:+,.2f}")
        print()
    
    # 汇总
    print("=" * 65)
    print("  Walk-Forward汇总")
    print("=" * 65)
    
    segments_positive = sum(1 for r in results if r["total_return"] > 0)
    avg_return = sum(r["total_return"] for r in results) / len(results)
    avg_sharpe = sum(r["sharpe"] for r in results) / len(results)
    avg_b_winrate = sum(r["b_winrate"] for r in results if r["b_winrate"] > 0) / max(1, sum(1 for r in results if r["b_winrate"] > 0))
    
    print(f"  盈利段数: {segments_positive}/{len(results)}")
    print(f"  平均收益: {avg_return:+.2f}%")
    print(f"  平均Sharpe: {avg_sharpe:.3f}")
    print(f"  策略B平均胜率: {avg_b_winrate:.1f}%")
    
    print()
    print(f"  {'段':<4} {'区间':<28} {'收益':<10} {'Sharpe':<8} {'回撤':<8} {'B交易':<6} {'B胜率':<8} {'B盈亏'}")
    print(f"  {'-'*80}")
    for r in results:
        print(f"  {r['segment']:<4} {r['period']:<28} {r['total_return']:+.2f}%{' '*4} {r['sharpe']:.3f}{' '*3} {r['max_dd']:.2f}%{' '*3} {r['b_trades']:<6} {r['b_winrate']}%{' '*4} ¥{r['b_pnl']:+,.0f}")
    
    # 判断
    print()
    if segments_positive == len(results):
        print("  ✓ 所有段都盈利 → 策略稳定, 过拟合风险低")
    elif segments_positive >= len(results) * 0.75:
        print("  ⚠ 大部分段盈利 → 策略基本稳定, 但有市场环境依赖")
    else:
        print("  ✗ 盈利段不足75% → 可能过拟合或策略不稳定")
    
    return results


def deflated_sharpe_test():
    """
    Deflated Sharpe Ratio (Bailey & Lopez de Prado)
    校正"试了多组参数取最优"带来的过拟合
    
    公式: DS = (Sharpe_hat - E[max(Sharpe)]) / SD[max(Sharpe)]
    其中E[max] ≈ (1-γ)·Φ^{-1}(1-1/N) + γ·Φ^{-1}(1-1/(N·e))
    N=试验次数(4组参数), γ=欧拉常数≈0.5772
    """
    print()
    print("=" * 65)
    print("  Deflated Sharpe Ratio校正")
    print("=" * 65)
    print()
    
    # 我们试了4组参数的结果
    trials = {
        "2%": 1.517,
        "3%": 1.349,
        "4%": 1.427,
        "5%": 1.853,
    }
    N = len(trials)  # 4组
    best_sharpe = max(trials.values())  # 1.853
    
    print(f"  试验参数: {N}组")
    print(f"  各组Sharpe: {trials}")
    print(f"  最优Sharpe: {best_sharpe:.3f}")
    print()
    
    # 估算"试N次取最优"的期望最大Sharpe
    T = 208 / 252  # 年化时间
    se_sharpe = 1.0 / math.sqrt(T)
    
    gamma = 0.5772  # 欧拉常数
    from statistics import NormalDist
    nd = NormalDist()
    
    # E[max of N iid N(0,1)] ≈ sqrt(2*ln(N)) - gamma/(2*sqrt(2*ln(N)))
    if N > 1:
        ln_N = math.log(N)
        expected_max_z = math.sqrt(2 * ln_N) - gamma / (2 * math.sqrt(2 * ln_N))
    else:
        expected_max_z = 0
    
    expected_max_sharpe = expected_max_z * se_sharpe
    sd_max = se_sharpe * math.pi / math.sqrt(6)  # Gumbel SD
    
    # Deflated Sharpe
    if sd_max > 0:
        ds = (best_sharpe - expected_max_sharpe) / sd_max
    else:
        ds = best_sharpe
    
    # P值(标准正态)
    p_value = 1 - nd.cdf(ds) if ds > 0 else nd.cdf(ds)
    
    print(f"  回测时长: {T:.2f}年 ({208}天)")
    print(f"  Sharpe标准误差(SE): {se_sharpe:.3f}")
    print(f"  N次试验期望最大Sharpe(零假设下): {expected_max_sharpe:.3f}")
    print(f"  SD of max Sharpe: {sd_max:.3f}")
    print()
    print(f"  Deflated Sharpe: {ds:.3f}")
    print(f"  P值: {p_value:.4f}")
    print()
    
    if p_value < 0.05:
        print(f"  ✓ P={p_value:.4f} < 0.05 → 即使考虑了'试4组取最优',")
        print(f"    Sharpe 1.853仍然显著, 不是纯靠运气")
    elif p_value < 0.10:
        print(f"  ⚠ P={p_value:.4f} < 0.10 → 边缘显著, 有一定运气成分")
    else:
        print(f"  ✗ P={p_value:.4f} ≥ 0.10 → 不显著, 可能是运气")
    
    return ds, p_value


def rolling_window_test(by_date, by_stock, zt_by_date, sorted_dates, window=60):
    """
    滚动窗口测试: 用60天窗口滚动, 看收益分布
    比Walk-Forward更细粒度
    """
    print()
    print("=" * 65)
    print(f"  滚动窗口测试 ({window}天窗口)")
    print("=" * 65)
    print()
    
    total = len(sorted_dates)
    step = 20  # 每20天滚一次
    window_returns = []
    
    for start_idx in range(0, total - window, step):
        end_idx = start_idx + window
        seg_start = sorted_dates[start_idx]
        seg_end = sorted_dates[end_idx]
        
        engine = BacktestEngine(trailing_stop_pct=0.05)
        report = engine.run(by_date, by_stock, zt_by_date, seg_start, seg_end)
        
        window_returns.append({
            "period": f"{seg_start}~{seg_end}",
            "return": report.get("total_return_pct", 0),
            "trades": report.get("total_trades", 0),
            "sharpe": report.get("sharpe_ratio", 0),
        })
    
    # 统计
    returns = [w["return"] for w in window_returns]
    positive = sum(1 for r in returns if r > 0)
    avg_ret = sum(returns) / len(returns) if returns else 0
    
    print(f"  窗口数: {len(window_returns)}")
    print(f"  盈利窗口: {positive}/{len(window_returns)} ({positive/len(window_returns)*100:.0f}%)")
    print(f"  平均收益: {avg_ret:+.2f}%")
    print(f"  最低: {min(returns):+.2f}%  最高: {max(returns):+.2f}%")
    
    # 分布
    print()
    print("  收益分布:")
    for r in window_returns:
        bar = "█" * max(1, int(abs(r["return"]) / 2))
        sign = "+" if r["return"] > 0 else ""
        print(f"  {r['period']}: {sign}{r['return']:.1f}% ({r['trades']}笔) {bar}")
    
    return window_returns


if __name__ == "__main__":
    print("加载数据...")
    by_date, by_stock = load_daily_prices()
    zt_by_date = load_zt_pool(use_sim=True)
    sorted_dates = sorted(by_date.keys())
    
    print(f"数据: {len(sorted_dates)}天, {len(by_stock)}只, 涨停池{len(zt_by_date)}天")
    print()
    
    # 1. Walk-Forward 4段
    wf_results = walk_forward_test(by_date, by_stock, zt_by_date, sorted_dates, n_splits=4)
    
    # 2. Deflated Sharpe (加import math)
    try:
        ds, p_val = deflated_sharpe_test()
    except Exception as e:
        print(f"\nDeflated Sharpe计算出错: {e}")
        import math as math_mod
        T = 208 / 252
        se = 1.0 / math_mod.sqrt(T)
        N = 4
        gamma = 0.5772
        ln_N = math_mod.log(N)
        expected_max_z = math_mod.sqrt(2 * ln_N) - gamma / (2 * math_mod.sqrt(2 * ln_N))
        expected_max = expected_max_z * se
        sd_max = se * math_mod.pi / math_mod.sqrt(6)
        best_sharpe = 1.853
        ds = (best_sharpe - expected_max) / sd_max
        from statistics import NormalDist
        p_val = 1 - NormalDist().cdf(ds)
        print(f"  手动计算: DS={ds:.3f}, P={p_val:.4f}")
    
    # 3. 滚动窗口
    rw_results = rolling_window_test(by_date, by_stock, zt_by_date, sorted_dates, window=50)
    
    print()
    print("=" * 65)
    print("  最终结论")
    print("=" * 65)
    positive_segments = sum(1 for r in wf_results if r["total_return"] > 0)
    print(f"  Walk-Forward: {positive_segments}/4段盈利")
    print(f"  Deflated Sharpe: {ds:.3f} (P={p_val:.4f})")
    positive_windows = sum(1 for r in rw_results if r["return"] > 0)
    print(f"  滚动窗口: {positive_windows}/{len(rw_results)}盈利")
