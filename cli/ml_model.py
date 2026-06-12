"""ML模型训练与回测CLI

用法:
    uv run python -m cli.ml_model train          # 训练模型并回测
    uv run python -m cli.ml_model train --top 10  # 选TOP 10
    uv run python -m cli.ml_model predict         # 用最新模型预测今日选股
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, ".")


def _ranked_eligible_indices(latest, scores, names: dict, top_n: int) -> list[int]:
    """Return the highest-scoring eligible A-share rows after filtering."""
    selected = []
    for idx in sorted(range(len(scores)), key=lambda i: scores[i], reverse=True):
        code = str(latest.iloc[idx]["stock_code"])
        name = names.get(code, "")
        if len(code) != 6 or not code.isdigit():
            continue
        if code.startswith(("200", "688", "689", "8", "9")):
            continue
        if "ST" in name or "退" in name:
            continue
        selected.append(idx)
        if len(selected) >= top_n:
            break
    return selected


def cmd_train(args):
    """训练LightGBM模型并运行Walk-Forward回测"""
    import warnings
    warnings.filterwarnings("ignore")
    import pandas as pd
    from rich.console import Console
    from rich.table import Table

    from src.ml.labeler import build_labels
    from src.ml.feature_pipeline import FeaturePipeline
    from src.ml.walk_forward import WalkForwardBacktest

    console = Console()
    db_path = args.db

    # 1. 构建标签
    console.print("[bold cyan]Step 1/3: 构建标签...[/]")
    labels = build_labels(db_path=db_path)
    console.print(f"  标签数据: {len(labels):,} 行")
    console.print(f"  日期范围: {labels.trade_date.min()} ~ {labels.trade_date.max()}")

    # 2. 构建特征
    console.print("[bold cyan]Step 2/3: 构建Alpha158特征...[/]")
    pipe = FeaturePipeline(db_path=db_path)
    features = pipe.build_features()
    feature_cols = [c for c in features.columns
                    if c not in ["stock_code", "trade_date", "open", "high", "low",
                                 "close", "volume", "amount", "turnover_rate", "pre_close"]]
    console.print(f"  特征矩阵: {len(features):,} 行 x {len(feature_cols)} 个特征")

    # 3. Walk-Forward回测
    console.print("[bold cyan]Step 3/3: Walk-Forward回测...[/]")
    wf = WalkForwardBacktest(
        train_days=args.train_days,
        test_days=args.test_days,
        step_days=args.step_days,
        top_n=args.top,
        model_output_dir=args.output,
    )
    results = wf.run(features, labels)

    # 4. 输出结果
    m = results["metrics"]
    console.print()
    console.print("[bold green]=== 回测结果 ===[/]")

    table = Table(title="核心指标")
    table.add_column("指标", style="cyan")
    table.add_column("值", style="green")
    table.add_row("回测轮次", str(m["total_trades"]))
    table.add_row("胜率", f'{m["win_rate"]:.1%}')
    table.add_row("平均收益", f'{m["avg_return"]:.2%}')
    table.add_row("Sharpe比率", f'{m["sharpe"]:.3f}')
    table.add_row("最大回撤", f'{m["max_drawdown"]:.2%}')
    console.print(table)

    # Top因子
    console.print("\n[bold]Top 15 因子重要性:[/]")
    sorted_feats = sorted(results["feature_importance"].items(), key=lambda x: -x[1])
    ft_table = Table()
    ft_table.add_column("#", style="dim")
    ft_table.add_column("因子", style="cyan")
    ft_table.add_column("重要性", style="green")
    for i, (fname, imp) in enumerate(sorted_feats[:15], 1):
        ft_table.add_row(str(i), fname, f"{imp:.4f}")
    console.print(ft_table)

    # 保存结果
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    result_file = output_dir / "ml_backtest_results.json"
    save_data = {
        "metrics": {k: float(v) if isinstance(v, (int, float)) else v
                     for k, v in m.items()},
        "feature_importance": {k: float(v) for k, v in sorted_feats},
        "config": {
            "train_days": args.train_days,
            "test_days": args.test_days,
            "step_days": args.step_days,
            "top_n": args.top,
        },
    }
    with open(result_file, "w") as f:
        json.dump(save_data, f, indent=2, ensure_ascii=False)
    console.print(f"\n结果已保存到: {result_file}")

    # 保存最新模型
    if results.get("model_path"):
        console.print(f"模型已保存到: {results['model_path']}")

    return results


def cmd_predict(args):
    """用训练好的模型预测"""
    import warnings
    warnings.filterwarnings("ignore")
    import numpy as np
    import sqlite3
    import pandas as pd
    from rich.console import Console
    from rich.table import Table

    console = Console()

    model_path = Path(args.model)
    meta_path = model_path.parent / "latest_model_meta.json"
    if not model_path.exists():
        console.print(f"[red]模型文件不存在: {model_path}[/]")
        console.print("请先运行: uv run python -m cli.ml_model train")
        return

    import lightgbm as lgb
    from src.ml.feature_pipeline import FeaturePipeline

    # 加载模型
    model = lgb.Booster(model_file=str(model_path))
    feature_cols = None
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        feature_cols = meta.get("feature_cols")

    console.print("[bold cyan]构建特征...[/]")
    pipe = FeaturePipeline(db_path=args.db)
    features = pipe.build_features()

    # 取最新一天
    latest_date = features["trade_date"].max()
    latest = features[features["trade_date"] == latest_date].copy()

    if feature_cols:
        use_cols = feature_cols
    else:
        use_cols = [c for c in latest.columns
                    if c not in ["stock_code", "trade_date", "open", "high", "low",
                                 "close", "volume", "amount", "turnover_rate", "pre_close"]]

    # 缺失列补0
    for col in use_cols:
        if col not in latest.columns:
            latest[col] = 0.0

    X = latest[use_cols].values
    col_means = np.nanmean(X, axis=0)
    col_means = np.where(np.isnan(col_means), 0.0, col_means)
    X = np.where(np.isnan(X), col_means, X)

    scores = model.predict(X, num_iteration=model.best_iteration)

    # 名称映射 — 从多表聚合，确保覆盖全市场
    db_path = args.db
    names = {}
    industry = {}
    if Path(db_path).exists():
        conn = sqlite3.connect(db_path)
        # 1) 从涨停池/强势股/资金流聚合名字(覆盖面有限)
        for table, code_col, name_col in [
            ("zt_pool", "stock_code", "name"),
            ("strong_pool", "stock_code", "name"),
            ("fund_flow", "stock_code", "stock_name"),
        ]:
            try:
                df = pd.read_sql_query(
                    f"SELECT DISTINCT {code_col}, {name_col} FROM {table} WHERE {name_col} IS NOT NULL",
                    conn,
                )
                for _, row in df.iterrows():
                    names[row[code_col]] = row[name_col]
            except Exception:
                pass
        # 2) 用fund_flow的stock_name补充(fund_flow覆盖约5000+只)
        try:
            ff_df = pd.read_sql_query(
                "SELECT stock_code, stock_name FROM fund_flow WHERE stock_name IS NOT NULL AND stock_name != ''",
                conn,
            )
            for _, row in ff_df.iterrows():
                names.setdefault(row["stock_code"], row["stock_name"])
        except Exception:
            pass
        # 行业映射
        try:
            ind_df = pd.read_sql_query(
                "SELECT stock_code, concept_name FROM concept_mapping "
                "GROUP BY stock_code HAVING ROWID = MIN(ROWID)",
                conn,
            )
            industry = dict(zip(ind_df["stock_code"], ind_df["concept_name"]))
        except Exception:
            pass

        # 价格信息 — 同时作为名字的兜底源
        price_df = pd.read_sql_query(
            "SELECT stock_code, close, pre_close FROM daily_price WHERE trade_date = ?",
            conn, params=(latest_date,),
        )
        price_map = dict(zip(price_df["stock_code"], price_df["close"]))
        preclose_map = dict(zip(price_df["stock_code"], price_df["pre_close"]))
        conn.close()
    else:
        price_map = {}
        preclose_map = {}

    # 排序 — 过滤B股/科创板/北交所/ST后补足top_n
    top_n = args.top
    top_idx = _ranked_eligible_indices(latest, scores, names, top_n)
    result = []
    rank = 0
    for idx in top_idx:
        code = str(latest.iloc[idx]["stock_code"])
        name = names.get(code, "")
        sc = float(scores[idx])
        rank += 1
        cl = price_map.get(code, 0)
        pre = preclose_map.get(code) or 1
        chg = ((cl / pre) - 1) * 100 if pre and pre > 0 else 0
        result.append({
            "rank": rank, "code": code, "name": name,
            "industry": industry.get(code, ""), "score": round(sc, 6),
            "close": round(cl, 2), "change_pct": round(chg, 2),
        })
        if rank >= top_n:
            break

    # 输出表格
    table = Table(title=f"ML选股 TOP{top_n} ({latest_date})")
    table.add_column("#", style="dim")
    table.add_column("代码", style="cyan")
    table.add_column("名称", style="white")
    table.add_column("行业", style="dim")
    table.add_column("模型得分", style="green")
    table.add_column("收盘价", style="yellow")
    table.add_column("涨跌幅", style="green")
    for r in result:
        chg_str = f'{r["change_pct"]:+.2f}%'
        chg_style = "red" if r["change_pct"] > 0 else "green" if r["change_pct"] < 0 else "white"
        table.add_row(
            str(r["rank"]), r["code"], r["name"], r["industry"],
            f'{r["score"]:.4f}', f'{r["close"]:.2f}',
            f'[{chg_style}]{chg_str}[/]',
        )
    console.print(table)

    # 保存缓存文件（供Web UI使用）
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_path = output_dir / "latest_prediction.json"
    with open(cache_path, "w") as f:
        json.dump({
            "date": latest_date,
            "total_stocks": int(len(latest)),
            "top7": result[:7],
            "all_top": result,
        }, f, ensure_ascii=False, indent=2)
    console.print(f"\n[green]预测缓存已保存到: {cache_path}[/]")
    console.print(f"[dim]Web UI 将自动加载最新预测结果[/]")


def main():
    parser = argparse.ArgumentParser(description="Alpha Miner ML模型")
    parser.add_argument("--db", default="data/alpha_miner.db", help="数据库路径")
    sub = parser.add_subparsers(dest="command")

    # train
    train_p = sub.add_parser("train", help="训练模型+回测")
    train_p.add_argument("--train-days", type=int, default=60)
    train_p.add_argument("--test-days", type=int, default=20)
    train_p.add_argument("--step-days", type=int, default=20)
    train_p.add_argument("--top", type=int, default=20, help="每轮选股数")
    train_p.add_argument("--output", default="output/ml", help="输出目录")

    # predict
    pred_p = sub.add_parser("predict", help="预测选股")
    pred_p.add_argument("--model", default="output/ml/latest_model.txt")
    pred_p.add_argument("--top", type=int, default=20)
    pred_p.add_argument("--output", default="output/ml", help="输出目录")
    pred_p.add_argument("--db", default="data/alpha_miner.db")

    args = parser.parse_args()
    if args.command == "train":
        cmd_train(args)
    elif args.command == "predict":
        cmd_predict(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
