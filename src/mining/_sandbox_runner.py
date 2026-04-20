"""沙箱运行脚本 — 由 Sandbox.execute 调用，不应直接使用。"""
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
        from datetime import datetime, timedelta
        from src.data.storage import Storage

        ns = {
            "__name__": f"factor_{factor_name}",
            "pd": pd,
            "pandas": pd,
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

        # 用最近的交易日做测试
        as_of = datetime.now()
        price_df = db.query("daily_price", as_of)
        if price_df.empty:
            # 回退：用更早的日期
            for i in range(1, 30):
                as_of = datetime.now() - timedelta(days=i)
                price_df = db.query("daily_price", as_of)
                if not price_df.empty:
                    break

        universe = sorted(price_df["stock_code"].unique().tolist())[:50] if not price_df.empty else ["000001"]

        # 执行 compute
        values = ns["compute"](universe, as_of, db)

        # 计算 IC（如果有足够数据）
        ic_result = {"ic_mean": 0.0, "icir": 0.0, "win_rate": 0.0, "sample_size": 0}

        if values is not None and len(values) > 0:
            import pandas as pd
            if isinstance(values, pd.Series):
                valid = values.dropna()
                ic_result["sample_size"] = len(valid)
                # 简单的 IC 测试：因子值与次日收益的 Spearman 相关
                # 这里只返回样本数，实际 IC 计算需要回测
                if len(valid) > 0:
                    ic_result["values_mean"] = float(valid.mean())
                    ic_result["values_std"] = float(valid.std()) if len(valid) > 1 else 0.0

        result = {
            "ic_result": ic_result,
            "factor_name": factor_name,
            "universe_size": len(universe),
        }
        print(json.dumps(result, ensure_ascii=False))

    except Exception as e:
        print(json.dumps({"error": traceback.format_exc()[-500:]}))

if __name__ == "__main__":
    main()
