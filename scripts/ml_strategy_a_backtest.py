#!/usr/bin/env python3
"""
ML策略A真正的交易回测 — 性能优化版

优化点:
1. 特征+标签只merge一次
2. 按日期预索引，避免重复过滤
3. 训练/预测只取子集，不每次都全量操作
"""
import sqlite3, math, json, sys, warnings, os, time
from collections import defaultdict
from datetime import datetime
import numpy as np
import pandas as pd

sys.path.insert(0, '.')
from scripts.backtest_engine_v3 import (
    load_daily_prices, load_zt_pool, INITIAL_CASH,
    STOP_LOSS_PCT, SELL_PARAMS_DEFAULT, COMMISSION_RATE, STAMP_TAX_RATE,
    SLIPPAGE, MAX_POSITIONS, BUY_AMOUNT_MAX
)

warnings.filterwarnings("ignore")
try:
    import lightgbm as lgb
except ImportError:
    print("需要安装lightgbm: uv add lightgbm")
    sys.exit(1)


def run_ml_backtest(merged, feature_cols, by_date, sorted_dates, 
                    start_date, end_date, retrain_interval=20, top_n=20):
    """
    ML策略A回测 — 真正的交易模拟
    
    merged: 已合并的特征+标签DataFrame (只merge一次)
    feature_cols: 特征列名列表
    """
    dates = [d for d in sorted_dates if start_date <= d <= end_date]
    if not dates:
        return None
    
    # 预按日期分组
    merged_by_date = {}
    for d, grp in merged.groupby("trade_date"):
        merged_by_date[d] = grp
    
    # 模拟账户
    cash = INITIAL_CASH
    positions = []
    completed_trades = []
    daily_nav = []
    
    # ML训练状态
    last_model = None
    last_feature_cols = None
    retrain_counter = 0
    
    for i, trade_date in enumerate(dates):
        day_stocks = {r['code']: r for r in by_date.get(trade_date, [])}
        if not day_stocks:
            continue
        
        # --- 卖出检查 ---
        to_remove = []
        for pos in positions:
            if pos['code'] not in day_stocks:
                continue
            data = day_stocks[pos['code']]
            price = data['close']
            buy_price = pos['buy_price']
            highest = pos['highest']
            pnl_pct = (price / buy_price - 1)
            
            if price > highest:
                pos['highest'] = price
                highest = price
            
            hold_days = (datetime.strptime(trade_date, "%Y-%m-%d") -
                        datetime.strptime(pos['buy_date'], "%Y-%m-%d")).days
            if hold_days < 1:
                continue
            
            params = SELL_PARAMS_DEFAULT["A"]
            sell_reason = None
            
            if pnl_pct <= STOP_LOSS_PCT:
                sell_reason = f"止损 {pnl_pct*100:+.1f}% [A]"
            elif hold_days >= params['max_hold_days']:
                sell_reason = f"最长持有{params['max_hold_days']}天 {pnl_pct*100:+.1f}% [A]"
            elif highest > buy_price:
                drawdown = (price / highest - 1)
                if drawdown <= -params['trailing_stop_pct']:
                    sell_reason = f"移动止盈 从{highest:.2f}回落{abs(drawdown)*100:.1f}% [A]"
            elif hold_days >= params['time_stop_days'] and pnl_pct < params['time_stop_threshold']:
                sell_reason = f"时间止损 {hold_days}天 {pnl_pct*100:+.1f}% [A]"
            
            if sell_reason:
                sell_price = price * (1 - SLIPPAGE)
                proceeds = sell_price * pos['shares'] * (1 - COMMISSION_RATE + STAMP_TAX_RATE)
                pnl = proceeds - pos['buy_price'] * pos['shares']
                pnl_pct_actual = (sell_price / pos['buy_price'] - 1)
                
                cash += proceeds
                completed_trades.append({
                    "code": pos['code'], "buy_price": pos['buy_price'],
                    "sell_price": round(sell_price, 3), "shares": pos['shares'],
                    "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct_actual * 100, 2),
                    "hold_days": hold_days, "sell_reason": sell_reason,
                    "buy_date": pos['buy_date'], "sell_date": trade_date,
                    "strategy": "A", "signal_type": pos['signal_type'],
                })
                to_remove.append(pos)
        
        for pos in to_remove:
            positions.remove(pos)
        
        # --- ML训练/预测 ---
        retrain_counter += 1
        if retrain_counter >= retrain_interval or last_model is None:
            # 取trade_date之前60天作为训练集
            td_idx = sorted_dates.index(trade_date) if trade_date in sorted_dates else -1
            if td_idx < 0:
                continue
            train_start_i = max(0, td_idx - 60)
            train_dates = set(sorted_dates[train_start_i:td_idx])
            
            # 收集训练数据
            train_frames = []
            for td in train_dates:
                if td in merged_by_date:
                    train_frames.append(merged_by_date[td])
            
            if len(train_frames) < 30:
                retrain_counter = 0
                continue
            
            train_df = pd.concat(train_frames, ignore_index=True)
            X_train_full = train_df[feature_cols].values
            y_train_full = train_df["ret_1d"].values
            
            valid_mask = ~np.isnan(y_train_full)
            X_train_full = X_train_full[valid_mask]
            y_train_full = y_train_full[valid_mask]
            
            if len(X_train_full) < 100:
                retrain_counter = 0
                continue
            
            # 验证集
            val_split = max(1, int(len(X_train_full) * 0.2))
            X_tr, X_val = X_train_full[:-val_split], X_train_full[-val_split:]
            y_tr, y_val = y_train_full[:-val_split], y_train_full[-val_split:]
            
            params = {
                "objective": "regression",
                "metric": "mse",
                "num_leaves": 31,
                "learning_rate": 0.05,
                "feature_fraction": 0.8,
                "bagging_fraction": 0.8,
                "bagging_freq": 5,
                "lambda_l1": 0.1,
                "lambda_l2": 0.1,
                "min_child_samples": 20,
                "verbose": -1,
                "seed": 42,
            }
            
            try:
                dtrain = lgb.Dataset(X_tr, y_tr)
                dval = lgb.Dataset(X_val, y_val)
                last_model = lgb.train(
                    params, dtrain,
                    num_boost_round=200,
                    valid_sets=[dval],
                    callbacks=[lgb.early_stopping(20, verbose=False)]
                )
                last_feature_cols = feature_cols
            except Exception:
                last_model = None
            
            retrain_counter = 0
        
        # --- 预测今天 ---
        if last_model is None:
            continue
        
        if trade_date not in merged_by_date:
            continue
        
        pred_df = merged_by_date[trade_date]
        if len(pred_df) < 10:
            continue
        
        X_pred = pred_df[feature_cols].values
        scores = last_model.predict(X_pred, num_iteration=last_model.best_iteration)
        
        # 选top N
        pred_codes = pred_df["stock_code"].values
        valid = ~np.isnan(scores)
        top_indices = np.argsort(scores)[::-1][:top_n]
        top_indices = [i for i in top_indices if valid[i]]
        
        # --- 买入 ---
        held_codes = {p['code'] for p in positions}
        for idx in top_indices:
            code = str(pred_codes[idx])
            score = float(scores[idx])
            
            if code in held_codes:
                continue
            if len(positions) >= MAX_POSITIONS:
                break
            if cash < 1000:
                break
            if code not in day_stocks:
                continue
            
            data = day_stocks[code]
            
            # 过滤
            if code.startswith(("688", "689", "8", "9")):
                continue
            if data['amount'] < 1_000_000:
                continue
            
            buy_price = data['close'] * (1 + SLIPPAGE)
            shares = int(BUY_AMOUNT_MAX / buy_price / 100) * 100
            if shares < 100:
                shares = 100
            cost = buy_price * shares * (1 + COMMISSION_RATE)
            
            if cost > cash:
                continue
            
            cash -= cost
            positions.append({
                "code": code, "buy_price": round(buy_price, 3),
                "buy_date": trade_date, "shares": shares,
                "highest": buy_price, "strategy": "A",
                "signal_type": f"ML选股 score={score:.4f}",
            })
        
        # 记录净值
        mv = 0
        for pos in positions:
            if pos['code'] in day_stocks:
                mv += day_stocks[pos['code']]['close'] * pos['shares']
            else:
                mv += pos['buy_price'] * pos['shares']
        
        nav = cash + mv
        daily_nav.append({
            "date": trade_date, "nav": round(nav, 2),
            "cash": round(cash, 2), "market_value": round(mv, 2),
            "positions": len(positions),
        })
    
    # 汇总
    if not daily_nav:
        return None
    
    navs = [d['nav'] for d in daily_nav]
    total_return = (navs[-1] / INITIAL_CASH - 1) * 100
    
    peak = navs[0]
    max_dd = 0
    for n in navs:
        if n > peak:
            peak = n
        dd = (peak - n) / peak
        if dd > max_dd:
            max_dd = dd
    
    if len(navs) > 1:
        daily_returns = [(navs[i]/navs[i-1] - 1) for i in range(1, len(navs))]
        avg_ret = sum(daily_returns) / len(daily_returns)
        std_ret = math.sqrt(sum((r - avg_ret)**2 for r in daily_returns) / len(daily_returns))
        sharpe = (avg_ret / std_ret * math.sqrt(252)) if std_ret > 0 else 0
    else:
        sharpe = 0
    
    wins = [t for t in completed_trades if t['pnl'] > 0]
    losses = [t for t in completed_trades if t['pnl'] <= 0]
    
    sell_reasons = defaultdict(int)
    for t in completed_trades:
        sell_reasons[t['sell_reason'].split()[0]] += 1
    
    avg_pnl = sum(t['pnl'] for t in completed_trades) / len(completed_trades) if completed_trades else 0
    avg_win = sum(t['pnl'] for t in wins) / len(wins) if wins else 0
    avg_loss = sum(t['pnl'] for t in losses) / len(losses) if losses else 0
    avg_hold = sum(t['hold_days'] for t in completed_trades) / len(completed_trades) if completed_trades else 0
    win_rate = len(wins) / len(completed_trades) * 100 if completed_trades else 0
    
    return {
        "total_return_pct": round(total_return, 2),
        "sharpe_ratio": round(sharpe, 3),
        "max_drawdown_pct": round(max_dd * 100, 2),
        "total_trades": len(completed_trades),
        "total_days": len(daily_nav),
        "final_nav": round(navs[-1], 2),
        "win_rate": round(win_rate, 1),
        "total_pnl": round(sum(t['pnl'] for t in completed_trades), 2),
        "avg_pnl": round(avg_pnl, 2),
        "avg_win": round(avg_win, 2),
        "avg_loss": round(avg_loss, 2),
        "avg_hold_days": round(avg_hold, 1),
        "sell_reasons": dict(sell_reasons),
        "completed_trades": completed_trades,
        "daily_nav": daily_nav,
    }


def main():
    t0 = time.time()
    print("=" * 70)
    print("  ML策略A 真正的交易回测 (性能优化版)")
    print("=" * 70)
    
    # 加载预计算特征
    print("\n[1/3] 加载ML数据...")
    feat_path = "/tmp/ml_features.parquet"
    if os.path.exists(feat_path):
        print(f"  从缓存加载: {feat_path}")
        features = pd.read_parquet(feat_path)
        print(f"  特征: {len(features):,}行 x {len(features.columns)-2}个")
    else:
        print("  缓存不存在! 先运行特征预计算")
        sys.exit(1)
    
    # 构建标签
    print("  构建标签...")
    from src.ml.labeler import build_labels
    labels = build_labels(db_path="data/alpha_miner.db")
    print(f"  标签: {len(labels):,}行")
    
    # 一次性merge
    print("  合并特征+标签...")
    meta_cols = ["stock_code", "trade_date"]
    merged = features.merge(labels, on=meta_cols, how="inner")
    merged = merged.sort_values("trade_date").reset_index(drop=True)
    print(f"  合并后: {len(merged):,}行")
    
    # 特征列
    exclude_cols = set(meta_cols) | {
        "ret_1d", "ret_3d", "ret_5d",
        "label_1d", "label_3d", "label_5d", "rank_1d",
        "open", "high", "low", "close", "volume", "amount",
        "turnover_rate", "pre_close",
    }
    feature_cols = [c for c in merged.columns if c not in exclude_cols]
    print(f"  特征数: {len(feature_cols)}")
    
    # 加载行情
    print("\n[2/3] 加载行情数据...")
    by_date, by_stock = load_daily_prices()
    zt_by_date = load_zt_pool(use_sim=True)
    sorted_dates = sorted(by_date.keys())
    print(f"  {len(sorted_dates)}天, {len(by_stock)}只")
    
    # Walk-Forward 4段
    print("\n[3/3] ML策略A Walk-Forward回测...")
    n_splits = 4
    total_days = len(sorted_dates)
    segment_size = total_days // n_splits
    
    print(f"  每20天重训LightGBM, top_n=20")
    print()
    
    all_results = []
    for i in range(n_splits):
        start_idx = i * segment_size
        end_idx = (i + 1) * segment_size if i < n_splits - 1 else total_days
        seg_start = sorted_dates[start_idx]
        seg_end = sorted_dates[end_idx - 1]
        
        t_seg = time.time()
        print(f"  第{i+1}段: {seg_start}~{seg_end}...", end=" ", flush=True)
        
        result = run_ml_backtest(
            merged, feature_cols, by_date, sorted_dates,
            seg_start, seg_end, retrain_interval=20, top_n=20
        )
        
        elapsed = time.time() - t_seg
        if result:
            all_results.append({"segment": i+1, "start": seg_start, "end": seg_end, **result})
            print(f"收益={result['total_return_pct']:+.2f}% Sharpe={result['sharpe_ratio']:.3f} "
                  f"回撤={result['max_drawdown_pct']:.2f}% {result['total_trades']}笔 "
                  f"胜率={result['win_rate']}% ({elapsed:.0f}s)")
        else:
            all_results.append({"segment": i+1, "start": seg_start, "end": seg_end, "error": "无交易"})
            print(f"无交易 ({elapsed:.0f}s)")
    
    # 全区间
    print(f"\n  全区间回测...", end=" ", flush=True)
    t_full = time.time()
    full_result = run_ml_backtest(
        merged, feature_cols, by_date, sorted_dates,
        sorted_dates[0], sorted_dates[-1], retrain_interval=20, top_n=20
    )
    print(f"({time.time()-t_full:.0f}s)")
    
    # 汇总输出
    print()
    print("=" * 70)
    print("  ML策略A 回测结果")
    print("=" * 70)
    
    for r in all_results:
        if 'error' in r:
            print(f"  第{r['segment']}段: {r['start']}~{r['end']} — {r['error']}")
        else:
            print(f"  第{r['segment']}段: {r['start']}~{r['end']}")
            print(f"    收益={r['total_return_pct']:+.2f}% Sharpe={r['sharpe_ratio']:.3f} 回撤={r['max_drawdown_pct']:.2f}%")
            print(f"    {r['total_trades']}笔 胜率={r['win_rate']}% 总盈亏=¥{r['total_pnl']:+.2f}")
            print(f"    均盈=¥{r['avg_win']:+.2f} 均亏=¥{r['avg_loss']:+.2f} 均持有={r['avg_hold_days']}天")
            print(f"    卖出原因: {r['sell_reasons']}")
    
    if full_result:
        wr = full_result['win_rate']
        avg_w = full_result['avg_win']
        avg_l = full_result['avg_loss']
        pnl_ratio = abs(avg_w / avg_l) if avg_l != 0 else 999
        
        print()
        print(f"  全区间:")
        print(f"    收益={full_result['total_return_pct']:+.2f}% Sharpe={full_result['sharpe_ratio']:.3f} 回撤={full_result['max_drawdown_pct']:.2f}%")
        print(f"    {full_result['total_trades']}笔 胜率={wr}%")
        print(f"    总盈亏=¥{full_result['total_pnl']:+.2f} 均盈亏=¥{full_result['avg_pnl']:+.2f}")
        print(f"    均盈=¥{avg_w:+.2f} 均亏=¥{avg_l:+.2f} 盈亏比={pnl_ratio:.2f}")
        print(f"    卖出原因: {full_result['sell_reasons']}")
    
    # 对比
    print()
    print("=" * 70)
    print("  三方对比")
    print("=" * 70)
    print(f"  规则策略A: -18.83%  Sharpe=-0.721  回撤=41.40%  204笔  胜率39.7%  盈亏比1.27")
    print(f"  策略B:     +108.94% Sharpe=+2.795  回撤=15.86%  353笔  胜率39.1%  盈亏比2.40")
    if full_result:
        print(f"  ML策略A:   {full_result['total_return_pct']:+.2f}%  Sharpe={full_result['sharpe_ratio']:+.3f}  "
              f"回撤={full_result['max_drawdown_pct']:.2f}%  {full_result['total_trades']}笔  "
              f"胜率{full_result['win_rate']}%  盈亏比{pnl_ratio:.2f}")
    
    # 结论
    print()
    if full_result:
        ret = full_result['total_return_pct']
        if ret > 0:
            print(f"  结论: ML选股比规则好（+{ret:.2f}% vs -18.83%），但{'+108.94%' if ret < 108.94 else '需进一步分析'}的策略B更强")
        else:
            print(f"  结论: ML选股也亏（{ret:.2f}%），说明策略A的核心问题不只是规则，而是低吸策略本身")
    
    # 保存
    output = {
        "ml_strategy_a": {k: v for k, v in (full_result or {}).items()
                          if k not in ('completed_trades', 'daily_nav')},
        "segments": [{k: v for k, v in r.items() 
                      if k not in ('completed_trades', 'daily_nav')} 
                     for r in all_results],
        "elapsed_seconds": round(time.time() - t0, 1),
    }
    with open("output/ml/ml_strategy_a_backtest.json", "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False, default=str)
    print(f"\n  结果已保存到 output/ml/ml_strategy_a_backtest.json")
    print(f"  总耗时: {time.time()-t0:.0f}秒")


if __name__ == "__main__":
    main()
