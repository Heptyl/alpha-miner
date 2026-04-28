"""全因子 IC 验证 — 扩展版."""
import sqlite3
import numpy as np
from scipy.stats import spearmanr

conn = sqlite3.connect('data/alpha_miner.db', timeout=30)
conn.execute('PRAGMA journal_mode=WAL')

factors = ['turnover_rank', 'lhb_institution', 'leader_clarity', 'theme_crowding',
           'consecutive_board', 'main_flow_intensity', 'theme_lifecycle',
           'narrative_velocity', 'zt_dt_ratio']

dates = [r[0] for r in conn.execute('SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date').fetchall()]

print(f'日期范围: {dates[0]} ~ {dates[-1]}, {len(dates)} 天')
print()
print(f'{"因子":<22} {"IC均值":>8} {"ICIR":>8} {"胜率":>6} {"天数":>5}')
print('-' * 58)

for fname in factors:
    ics = []
    for i, dt in enumerate(dates):
        if i >= len(dates) - 1:
            break
        next_dt = dates[i + 1]
        fv = dict(conn.execute(
            'SELECT stock_code, factor_value FROM factor_values WHERE factor_name=? AND trade_date=?',
            (fname, dt)
        ).fetchall())
        cur = dict(conn.execute(
            'SELECT stock_code, close FROM daily_price WHERE trade_date=?', (dt,)
        ).fetchall())
        nxt = dict(conn.execute(
            'SELECT stock_code, close FROM daily_price WHERE trade_date=?', (next_dt,)
        ).fetchall())
        common = set(fv) & set(cur) & set(nxt)
        if len(common) < 10:
            continue
        vals = np.array([fv[c] for c in common])
        rets = np.array([(nxt[c] - cur[c]) / cur[c] for c in common])
        if np.std(vals) < 1e-10:
            continue
        valid = np.isfinite(vals) & np.isfinite(rets)
        if valid.sum() < 10:
            continue
        ic, _ = spearmanr(vals[valid], rets[valid])
        if np.isfinite(ic):
            ics.append(ic)

    if ics:
        a = np.array(ics)
        ic_mean = np.mean(a)
        ic_std = np.std(a)
        icir = ic_mean / ic_std if ic_std > 0 else 0
        win = (a > 0).mean()
        marker = ' **' if abs(ic_mean) > 0.03 and abs(icir) > 0.5 else ''
        print(f'{fname:<22} {ic_mean:>+8.4f} {icir:>+8.4f} {win:>5.0%} {len(ics):>5}{marker}')
    else:
        print(f'{fname:<22} 无有效IC')

conn.close()
