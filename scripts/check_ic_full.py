"""IC验证脚本 — 全量因子质量检查"""

import sqlite3
import numpy as np
from scipy.stats import spearmanr

DB = "data/alpha_miner.db"

def main():
    conn = sqlite3.connect(DB)

    factors = [r[0] for r in conn.execute(
        "SELECT DISTINCT factor_name FROM factor_values ORDER BY factor_name"
    ).fetchall()]

    # 预加载价格
    prices = conn.execute("SELECT stock_code, trade_date, close FROM daily_price").fetchall()
    price_dict = {}
    for code, date, close in prices:
        price_dict[(code, date)] = close

    # 预加载因子值
    all_fv = conn.execute(
        "SELECT factor_name, stock_code, trade_date, factor_value FROM factor_values"
    ).fetchall()
    fv_by_factor = {}
    for fname, code, date, val in all_fv:
        fv_by_factor.setdefault(fname, []).append((code, date, val))

    all_dates = sorted(set(r[1] for r in prices))
    conn.close()

    print(f"{'因子':25s} {'IC均值':>8s} {'ICIR':>8s} {'胜率':>8s} {'天数':>6s}")
    print("-" * 60)

    for fname in factors:
        fv_list = fv_by_factor.get(fname, [])
        if not fv_list:
            continue
        trade_dates = sorted(set(r[1] for r in fv_list))
        trade_dates = [d for d in trade_dates if d in all_dates]

        fv_by_date = {}
        for code, date, val in fv_list:
            fv_by_date.setdefault(date, {})[code] = val

        ics = []
        for i in range(len(trade_dates) - 1):
            d0 = trade_dates[i]
            d1 = trade_dates[i + 1]

            factor_vals = fv_by_date.get(d0, {})
            if len(factor_vals) < 20:
                continue

            returns = {}
            for code in factor_vals:
                c0 = price_dict.get((code, d0))
                c1 = price_dict.get((code, d1))
                if c0 and c1 and c0 > 0:
                    returns[code] = c1 / c0 - 1

            common = set(factor_vals) & set(returns)
            if len(common) < 20:
                continue

            fvals = [factor_vals[c] for c in common]
            rets = [returns[c] for c in common]

            ic, _ = spearmanr(fvals, rets)
            if not np.isnan(ic):
                ics.append(ic)

        if ics:
            ic_arr = np.array(ics)
            ic_mean = np.mean(ic_arr)
            ic_std = np.std(ic_arr)
            icir = ic_mean / ic_std if ic_std > 0 else 0
            win_rate = (ic_arr > 0).sum() / len(ic_arr)
            print(f"{fname:25s} {ic_mean:>8.4f} {icir:>8.4f} {win_rate:>8.2%} {len(ics):>6d}")
        else:
            print(f"{fname:25s}    N/A")


if __name__ == "__main__":
    main()
