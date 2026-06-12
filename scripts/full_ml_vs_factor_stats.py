#!/usr/bin/env python3
"""
全量统计验证: ML预测力 + IC因子筛选力
189天全部数据, 每天Top20 vs Bot20 vs 随机20 vs 全市场

输出:
1. ML模型预测力: Top20/Bottom20/随机20 次日实际收益对比
2. IC因子筛选力: 同上
3. 策略B基础数据: 涨停池次日表现
"""
import sys, math, time, json, os
import numpy as np
import pandas as pd
sys.path.insert(0, '.')
import warnings
warnings.filterwarnings('ignore')
import lightgbm as lgb
from src.ml.labeler import build_labels

DB = 'data/alpha_miner.db'

def load_data():
    """加载特征+标签+行情"""
    print("[1/4] 加载特征...")
    features = pd.read_parquet('/tmp/ml_features.parquet')
    print(f"  特征: {len(features)}行 × {len(features.columns)}列")

    print("[2/4] 构建标签...")
    labels = build_labels(db_path=DB)
    print(f"  标签: {len(labels)}行")

    meta_cols = ['stock_code', 'trade_date']
    merged = features.merge(labels, on=meta_cols, how='inner')
    merged = merged.sort_values('trade_date').reset_index(drop=True)
    print(f"  合并后: {len(merged)}行")

    exclude_cols = set(meta_cols) | {
        'ret_1d', 'ret_3d', 'ret_5d',
        'label_1d', 'label_3d', 'label_5d', 'rank_1d',
        'open', 'high', 'low', 'close', 'volume', 'amount',
        'turnover_rate', 'pre_close',
    }
    feature_cols = [c for c in merged.columns if c not in exclude_cols]

    by_date = {d: grp for d, grp in merged.groupby('trade_date')}
    sorted_dates = sorted(by_date.keys())
    return merged, by_date, sorted_dates, feature_cols


def ml_daily_stats(by_date, sorted_dates, feature_cols):
    """逐日ML预测: 训练→预测→统计Top/Bottom/随机"""
    print("[3/4] 逐日ML预测(189天, 约3-5分钟)...")

    results = []
    good_dates = [d for d in sorted_dates if len(by_date[d]) >= 2000]
    print(f"  数据充足(>=2000只)的日期: {len(good_dates)}")

    for i, test_date in enumerate(good_dates):
        if (i+1) % 30 == 0:
            print(f"  进度: {i+1}/{len(good_dates)}")

        td_idx = sorted_dates.index(test_date)
        train_dates = sorted_dates[max(0, td_idx-60):td_idx]

        # 训练集
        train_frames = [by_date[td] for td in train_dates if td in by_date]
        if len(train_frames) < 20:
            continue
        train_df = pd.concat(train_frames, ignore_index=True)

        X_train = train_df[feature_cols].values
        y_train = train_df['ret_1d'].values
        valid = ~np.isnan(y_train)
        X_train, y_train = X_train[valid], y_train[valid]

        if len(X_train) < 1000:
            continue

        val_split = max(1, int(len(X_train)*0.2))
        X_tr, X_val = X_train[:-val_split], X_train[-val_split:]
        y_tr, y_val = y_train[:-val_split], y_train[-val_split:]

        try:
            dtrain = lgb.Dataset(X_tr, y_tr)
            dval = lgb.Dataset(X_val, y_val)
            model = lgb.train(
                {'objective':'regression','metric':'mse','num_leaves':31,
                 'learning_rate':0.05,'feature_fraction':0.8,'bagging_fraction':0.8,
                 'bagging_freq':5,'lambda_l1':0.1,'lambda_l2':0.1,
                 'min_child_samples':20,'verbose':-1,'seed':42},
                dtrain, num_boost_round=200, valid_sets=[dval],
                callbacks=[lgb.early_stopping(20, verbose=False)]
            )
        except Exception:
            continue

        # 预测test_date
        pred_df = by_date[test_date]
        X_pred = pred_df[feature_cols].values
        scores = model.predict(X_pred, num_iteration=model.best_iteration)

        actual = pred_df['ret_1d'].values
        valid_mask = ~(np.isnan(scores) | np.isnan(actual))
        vs = scores[valid_mask]
        ac = actual[valid_mask]

        if len(vs) < 100:
            continue

        # Top/Bottom/随机
        sorted_idx = np.argsort(vs)[::-1]
        top20_idx = sorted_idx[:20]
        bot20_idx = sorted_idx[-20:]

        np.random.seed(42)
        rand20_idx = np.random.choice(len(vs), 20, replace=False)

        top20_ret = np.nanmean(ac[top20_idx])
        bot20_ret = np.nanmean(ac[bot20_idx])
        rand20_ret = np.nanmean(ac[rand20_idx])
        market_ret = np.nanmean(ac)

        # Top50, Top100 更大范围
        top50_idx = sorted_idx[:50]
        top100_idx = sorted_idx[:100]
        top50_ret = np.nanmean(ac[top50_idx])
        top100_ret = np.nanmean(ac[top100_idx])

        # IC (Spearman)
        from scipy.stats import spearmanr
        ic, _ = spearmanr(vs, ac)

        results.append({
            'date': test_date,
            'n_stocks': int(len(vs)),
            'top20_ret': float(np.nanmean(ac[top20_idx])),
            'bot20_ret': float(np.nanmean(ac[bot20_idx])),
            'rand20_ret': float(np.nanmean(ac[rand20_idx])),
            'market_ret': float(np.nanmean(ac)),
            'top50_ret': float(np.nanmean(ac[top50_idx])),
            'top100_ret': float(np.nanmean(ac[top100_idx])),
            'ic': float(ic),
            'score_std': float(vs.std()),
            'top20_win': bool(top20_ret > market_ret),
            'top20_beat_bot': bool(top20_ret > bot20_ret),
        })

    return results


def factor_daily_stats(by_date, sorted_dates, feature_cols):
    """逐日IC因子筛选: 用8个有效因子打分→Top/Bottom统计"""
    print("[4/4] 逐日IC因子筛选统计...")

    # 8个有效因子(从IC评估得到) + 方向
    factor_config = {
        'KLEN': -1,          # 反向: 小=好
        'VRA_10': -1,        # 反向
        'VRA_5': -1,         # 反向
        'VSTD_5': -1,        # 反向
        'VSTD_10': -1,       # 反向
        'MA_20': -1,         # 反向
        'BBANDS_WIDTH': -1,  # 反向
        'ATR_RATIO': -1,     # 反向
    }

    results = []
    good_dates = [d for d in sorted_dates if len(by_date[d]) >= 2000]

    for i, test_date in enumerate(good_dates):
        if (i+1) % 50 == 0:
            print(f"  进度: {i+1}/{len(good_dates)}")

        pred_df = by_date[test_date]
        actual = pred_df['ret_1d'].values

        # 计算因子得分
        available = [f for f in factor_config if f in pred_df.columns]
        if len(available) < 3:
            continue

        scores = np.zeros(len(pred_df))
        n_valid = 0
        for fname, direction in factor_config.items():
            if fname not in pred_df.columns:
                continue
            vals = pred_df[fname].values.copy()
            valid = ~np.isnan(vals)
            if valid.sum() < 100:
                continue
            # 排名百分位
            ranks = np.full_like(vals, np.nan, dtype=float)
            ranks[valid] = vals[valid].argsort().argsort() / valid.sum()
            if direction == -1:
                ranks = 1 - ranks
            mask = ~np.isnan(ranks)
            scores[mask] += ranks[mask]
            n_valid += 1

        if n_valid < 3:
            continue

        valid_mask = ~(np.isnan(scores) | np.isnan(actual))
        vs = scores[valid_mask]
        ac = actual[valid_mask]
        if len(vs) < 100:
            continue

        sorted_idx = np.argsort(vs)[::-1]
        top20_idx = sorted_idx[:20]
        bot20_idx = sorted_idx[-20:]

        np.random.seed(42)
        rand20_idx = np.random.choice(len(vs), 20, replace=False)

        results.append({
            'date': test_date,
            'n_stocks': int(len(vs)),
            'top20_ret': float(np.nanmean(ac[top20_idx])),
            'bot20_ret': float(np.nanmean(ac[bot20_idx])),
            'rand20_ret': float(np.nanmean(ac[rand20_idx])),
            'market_ret': float(np.nanmean(ac)),
            'top50_ret': float(np.nanmean(ac[sorted_idx[:50]])),
            'top100_ret': float(np.nanmean(ac[sorted_idx[:100]])),
            'n_factors': int(n_valid),
            'top20_win': bool(np.nanmean(ac[top20_idx]) > np.nanmean(ac)),
            'top20_beat_bot': bool(np.nanmean(ac[top20_idx]) > np.nanmean(ac[bot20_idx])),
        })

    return results


def print_summary(ml_results, factor_results):
    """打印汇总统计"""
    print("\n" + "=" * 70)
    print("  全量验证报告 (189天)")
    print("=" * 70)

    # ML部分
    if ml_results:
        ml = pd.DataFrame(ml_results)
        n = len(ml)
        print(f"\n{'─'*70}")
        print(f"  一、ML模型预测力 ({n}天)")
        print(f"{'─'*70}")

        print(f"\n  次日平均收益:")
        print(f"    ML Top20:   {ml['top20_ret'].mean()*100:+.4f}%  (中位数: {ml['top20_ret'].median()*100:+.4f}%)")
        print(f"    ML Top50:   {ml['top50_ret'].mean()*100:+.4f}%")
        print(f"    ML Top100:  {ml['top100_ret'].mean()*100:+.4f}%")
        print(f"    ML Bottom20:{ml['bot20_ret'].mean()*100:+.4f}%")
        print(f"    随机20只:   {ml['rand20_ret'].mean()*100:+.4f}%")
        print(f"    全市场:     {ml['market_ret'].mean()*100:+.4f}%")

        print(f"\n  胜率统计:")
        top_beat_market = ml['top20_win'].mean()
        top_beat_bot = ml['top20_beat_bot'].mean()
        print(f"    Top20跑赢全市场: {top_beat_market*100:.1f}% ({ml['top20_win'].sum()}/{n})")
        print(f"    Top20跑赢Bottom20: {top_beat_bot*100:.1f}% ({ml['top20_beat_bot'].sum()}/{n})")

        print(f"\n  IC统计 (Spearman):")
        print(f"    均值IC: {ml['ic'].mean():.4f}")
        print(f"    IC标准差: {ml['ic'].std():.4f}")
        print(f"    ICIR: {ml['ic'].mean()/ml['ic'].std():.4f}")
        print(f"    IC>0天数: {(ml['ic']>0).sum()}/{n} ({(ml['ic']>0).mean()*100:.1f}%)")

        print(f"\n  Top20 vs Bottom20 收益差分布:")
        diff = (ml['top20_ret'] - ml['bot20_ret']) * 100
        print(f"    均值: {diff.mean():+.4f}%")
        print(f"    中位数: {diff.median():+.4f}%")
        print(f"    正差天数: {(diff>0).sum()}/{n} ({(diff>0).mean()*100:.1f}%)")
        print(f"    最大正差: {diff.max():+.3f}%")
        print(f"    最大负差: {diff.min():+.3f}%")

        # 按月统计
        ml['month'] = ml['date'].str[:7]
        print(f"\n  按月Top20平均次日收益:")
        for month, grp in ml.groupby('month'):
            n_days = len(grp)
            avg_top = grp['top20_ret'].mean() * 100
            avg_mkt = grp['market_ret'].mean() * 100
            beat = grp['top20_win'].mean() * 100
            print(f"    {month}: Top20={avg_top:+.3f}% 市场={avg_mkt:+.3f}% 跑赢率={beat:.0f}% ({n_days}天)")

    # 因子部分
    if factor_results:
        fa = pd.DataFrame(factor_results)
        n = len(fa)
        print(f"\n{'─'*70}")
        print(f"  二、IC因子筛选力 ({n}天)")
        print(f"{'─'*70}")

        print(f"\n  次日平均收益:")
        print(f"    因子Top20:   {fa['top20_ret'].mean()*100:+.4f}%  (中位数: {fa['top20_ret'].median()*100:+.4f}%)")
        print(f"    因子Top50:   {fa['top50_ret'].mean()*100:+.4f}%")
        print(f"    因子Top100:  {fa['top100_ret'].mean()*100:+.4f}%")
        print(f"    因子Bottom20:{fa['bot20_ret'].mean()*100:+.4f}%")
        print(f"    随机20只:    {fa['rand20_ret'].mean()*100:+.4f}%")
        print(f"    全市场:      {fa['market_ret'].mean()*100:+.4f}%")

        print(f"\n  胜率统计:")
        print(f"    Top20跑赢全市场: {fa['top20_win'].mean()*100:.1f}% ({fa['top20_win'].sum()}/{n})")
        print(f"    Top20跑赢Bottom20: {fa['top20_beat_bot'].mean()*100:.1f}% ({fa['top20_beat_bot'].sum()}/{n})")

        print(f"\n  Top20 vs Bottom20 收益差:")
        diff = (fa['top20_ret'] - fa['bot20_ret']) * 100
        print(f"    均值: {diff.mean():+.4f}%")
        print(f"    正差天数: {(diff>0).sum()}/{n} ({(diff>0).mean()*100:.1f}%)")

        fa['month'] = fa['date'].str[:7]
        print(f"\n  按月因子Top20平均次日收益:")
        for month, grp in fa.groupby('month'):
            n_days = len(grp)
            avg_top = grp['top20_ret'].mean() * 100
            avg_mkt = grp['market_ret'].mean() * 100
            beat = grp['top20_win'].mean() * 100
            print(f"    {month}: Top20={avg_top:+.3f}% 市场={avg_mkt:+.3f}% 跑赢率={beat:.0f}% ({n_days}天)")

    # ML vs 因子直接对比
    if ml_results and factor_results:
        ml = pd.DataFrame(ml_results)
        fa = pd.DataFrame(factor_results)
        # 按日期对齐
        common = set(ml['date']) & set(fa['date'])
        ml_c = ml[ml['date'].isin(common)].set_index('date').sort_index()
        fa_c = fa[fa['date'].isin(common)].set_index('date').sort_index()

        print(f"\n{'─'*70}")
        print(f"  三、ML vs IC因子 直接对比 ({len(common)}天)")
        print(f"{'─'*70}")

        ml_top_avg = ml_c['top20_ret'].mean() * 100
        fa_top_avg = fa_c['top20_ret'].mean() * 100
        mkt_avg = ml_c['market_ret'].mean() * 100

        print(f"  次日平均收益:")
        print(f"    ML Top20:    {ml_top_avg:+.4f}%")
        print(f"    因子 Top20:  {fa_top_avg:+.4f}%")
        print(f"    全市场:      {mkt_avg:+.4f}%")

        # 哪个Top20更好
        ml_wins = (ml_c['top20_ret'] > fa_c['top20_ret']).sum()
        fa_wins = (fa_c['top20_ret'] > ml_c['top20_ret']).sum()
        print(f"\n  ML Top20 > 因子Top20: {ml_wins}/{len(common)}天 ({ml_wins/len(common)*100:.1f}%)")
        print(f"  因子Top20 > ML Top20: {fa_wins}/{len(common)}天 ({fa_wins/len(common)*100:.1f}%)")

        # Top20合并: ML + 因子都选中的票
        print(f"\n  ML vs 因子 选股重叠度:")
        # 用Top50算重叠(更有统计意义)
        # 这里简化: 比较两者跑赢市场的概率
        ml_always = ml_c['top20_win'].mean() * 100
        fa_always = fa_c['top20_win'].mean() * 100
        print(f"    ML Top20跑赢市场: {ml_always:.1f}%")
        print(f"    因子Top20跑赢市场: {fa_always:.1f}%")


if __name__ == '__main__':
    t0 = time.time()

    merged, by_date, sorted_dates, feature_cols = load_data()
    ml_results = ml_daily_stats(by_date, sorted_dates, feature_cols)
    factor_results = factor_daily_stats(by_date, sorted_dates, feature_cols)
    print_summary(ml_results, factor_results)

    # 保存原始数据
    os.makedirs('output/ml', exist_ok=True)
    with open('output/ml/full_ml_vs_factor_stats.json', 'w') as f:
        json.dump({'ml': ml_results, 'factor': factor_results}, f, ensure_ascii=False, indent=2)
    print(f"\n原始数据已保存: output/ml/full_ml_vs_factor_stats.json")

    print(f"\n总耗时: {time.time()-t0:.0f}秒")
