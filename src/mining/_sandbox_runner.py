"""沙箱运行脚本 — 由 Sandbox.execute 调用，不应直接使用。

执行因子 compute() 后，做滑动窗口回测计算真实 IC：
1. 取最近 ic_window 个交易日
2. 每天调用 compute() 得到因子截面值
3. 取次日 forward_return 作为 label
4. 计算每日 Spearman rank correlation → ic_mean, icir, win_rate
"""
import json
import sys
import os
import traceback
from importlib import util

def main():
    code_path = sys.argv[1]
    db_path = sys.argv[2]
    factor_name = sys.argv[3]

    try:
        # 读取并执行因子代码
        with open(code_path) as f:
            code = f.read()

        # 在受限命名空间执行，注入基础依赖
        import pandas as pd
        import numpy as np
        from datetime import datetime, timedelta
        from scipy.stats import spearmanr
        from src.data.storage import Storage

        ns = {
            "__name__": f"factor_{factor_name}",
            "pd": pd,
            "pandas": pd,
            "np": np,
            "numpy": np,
            "datetime": datetime,
            "timedelta": timedelta,
            "Storage": Storage,
        }
        exec(compile(code, code_path, "exec"), ns)

        if "compute" not in ns:
            print(json.dumps({"error": "代码中未定义 compute(universe, as_of, db) 函数"}))
            return

        # 构造测试参数
        db = Storage(db_path)

        # ── 多日回测计算真实 IC ─────────────────────────────
        ic_window = 20  # 回测窗口（交易日）
        ic_result = {
            "ic_mean": 0.0,
            "icir": 0.0,
            "win_rate": 0.0,
            "sample_size": 0,
            "ic_series": [],
        }

        # 1. 找到可用交易日列表
        end_date = datetime.now()
        # 先尝试取最近交易日
        trade_dates = []
        for i in range(90):  # 往回看90天
            d = end_date - timedelta(days=i)
            price_df = db.query("daily_price", d, where="trade_date = ?", params=(d.strftime("%Y-%m-%d"),))
            if not price_df.empty:
                trade_dates.append(d)
                if len(trade_dates) >= ic_window + 5:  # 多取几天给 forward return
                    break

        if len(trade_dates) < 5:
            # 数据不足，回退到单日测试模式
            # 至少确保 compute 能跑通
            as_of = datetime.now()
            for i in range(1, 60):
                as_of = datetime.now() - timedelta(days=i)
                price_df = db.query("daily_price", as_of)
                if not price_df.empty:
                    break

            universe = sorted(price_df["stock_code"].unique().tolist())[:100] if not price_df.empty else ["000001"]
            values = ns["compute"](universe, as_of, db)

            if values is not None and isinstance(values, pd.Series):
                valid = values.dropna()
                ic_result["sample_size"] = len(valid)
                ic_result["values_mean"] = float(valid.mean()) if len(valid) > 0 else 0.0
                ic_result["values_std"] = float(valid.std()) if len(valid) > 1 else 0.0
                ic_result["warning"] = "数据不足，无法计算真实IC，仅返回因子统计"
            else:
                ic_result["warning"] = "compute 返回 None 或非 Series"

            result = {"ic_result": ic_result, "factor_name": factor_name, "universe_size": len(universe)}
            print(json.dumps(result, ensure_ascii=False))
            return

        trade_dates.sort()  # 从早到晚

        # 2. 滑动窗口计算 IC
        daily_ics = []
        daily_sample_sizes = []

        for i in range(len(trade_dates) - 1):
            as_of = trade_dates[i]
            next_day = trade_dates[i + 1]
            date_str = as_of.strftime("%Y-%m-%d")
            next_date_str = next_day.strftime("%Y-%m-%d")

            # 获取 universe
            price_today = db.query("daily_price", as_of,
                                   where="trade_date = ?", params=(date_str,))
            if price_today.empty:
                continue

            universe = sorted(price_today["stock_code"].unique().tolist())[:100]

            # 调用 compute
            try:
                factor_values = ns["compute"](universe, as_of, db)
            except Exception as e:
                continue

            if factor_values is None or not isinstance(factor_values, pd.Series):
                continue

            factor_values = factor_values.dropna()
            if len(factor_values) < 5:
                continue

            # 获取次日收益率（forward return）
            price_next = db.query("daily_price", next_day,
                                  where="trade_date = ?", params=(next_date_str,))
            if price_next.empty:
                continue

            # 计算 forward return
            if "close" in price_today.columns and "close" in price_next.columns:
                ret_today = price_today.set_index("stock_code")["close"]
                ret_next = price_next.set_index("stock_code")["close"]

                # 只保留两天都有数据的股票
                common = factor_values.index.intersection(ret_today.index).intersection(ret_next.index)
                if len(common) < 5:
                    continue

                fwd_ret = (ret_next[common] / ret_today[common] - 1).replace([np.inf, -np.inf], np.nan).dropna()
                fv_aligned = factor_values[common].reindex(fwd_ret.index).dropna()

                # 对齐后再检查
                common2 = fv_aligned.index.intersection(fwd_ret.index)
                fv_aligned = fv_aligned[common2]
                fwd_ret = fwd_ret[common2]

                if len(fv_aligned) < 5:
                    continue

                # Spearman IC
                try:
                    ic, pval = spearmanr(fv_aligned, fwd_ret)
                    if not np.isnan(ic):
                        daily_ics.append(ic)
                        daily_sample_sizes.append(len(fv_aligned))
                except Exception:
                    continue

        # 3. 汇总 IC 统计
        if daily_ics:
            ic_arr = np.array(daily_ics)
            ic_result["ic_mean"] = float(np.mean(ic_arr))
            ic_result["ic_std"] = float(np.std(ic_arr))
            ic_result["icir"] = float(np.mean(ic_arr) / np.std(ic_arr)) if np.std(ic_arr) > 0 else 0.0
            ic_result["win_rate"] = float(np.mean(ic_arr > 0))
            ic_result["sample_size"] = int(np.mean(daily_sample_sizes)) if daily_sample_sizes else 0
            ic_result["num_days"] = len(daily_ics)
            ic_result["ic_series"] = [round(x, 4) for x in ic_arr.tolist()]
        else:
            ic_result["warning"] = "无有效 IC 计算（数据不足或因子输出异常）"

        result = {
            "ic_result": ic_result,
            "factor_name": factor_name,
            "universe_size": 100,
            "num_test_days": len(trade_dates) - 1,
            "num_valid_days": len(daily_ics),
        }
        print(json.dumps(result, ensure_ascii=False))

    except Exception as e:
        print(json.dumps({"error": traceback.format_exc()[-500:]}))

if __name__ == "__main__":
    main()
