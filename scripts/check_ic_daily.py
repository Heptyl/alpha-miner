"""逐日IC检查 — theme_crowding"""

import sqlite3
import numpy as np
from scipy.stats import spearmanr

DB = "data/alpha_miner.db"

def main():
    conn = sqlite3.connect(DB)

    prices = conn.execute("SELECT stock_code, trade_date, close FROM daily_price").fetchall()
    price_dict = {}
    for code, date, close in prices:
        price_dict[(code, date)] = close

    all_fv = conn.execute(
        "SELECT stock_code, trade_date, factor_value FROM factor_values "
        "WHERE factor_name='theme_crowding'"
    ).fetchall()
    fv_by_date = {}
    for code, date, val in all_fv:
        fv_by_date.setdefault(date, {})[code] = val

    dates = sorted(fv_by_date.keys())
    conn.close()

    print("theme_crowding 逐日IC:")
    for i in range(len(dates) - 1):
        d0, d1 = dates[i], dates[i + 1]
        fv = fv_by_date[d0]
        rets = {}
        for code in fv:
            c0 = price_dict.get((code, d0))
            c1 = price_dict.get((code, d1))
            if c0 and c1 and c0 > 0:
                rets[code] = c1 / c0 - 1
        common = set(fv) & set(rets)
        if len(common) >= 5:
            fvals = [fv[c] for c in common]
            rv = [rets[c] for c in common]
            ic, _ = spearmanr(fvals, rv)
            flag = "***" if abs(ic) > 0.05 else ""
            print(f"  {d0}->{d1}: {len(common):>4}只, IC={ic:>7.4f} {flag}")
        else:
            print(f"  {d0}->{d1}: {len(common):>4}只 (太少)")


if __name__ == "__main__":
    main()
