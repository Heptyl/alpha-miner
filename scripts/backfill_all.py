"""综合数据回填 — 一站式补齐所有数据源。

策略:
1. 先用东财指数获取交易日历
2. 回填 daily_price（从 zt/strong/lhb/fund_flow 汇总股票列表，逐只拉K线）
3. 回填 zt_pool / zb_pool / strong_pool / lhb_detail
4. 扩充 concept_mapping

用法: PYTHONUNBUFFERED=1 python scripts/backfill_all.py [--days 30]
"""

import sqlite3
import sys
import time
from datetime import datetime

import akshare as ak
import pandas as pd

DB_PATH = "data/alpha_miner.db"


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    return conn


def get_trade_dates(n_days=30):
    """获取最近N个交易日。"""
    print("[INFO] 获取交易日历...")
    for attempt in range(3):
        try:
            df = ak.stock_zh_index_daily(symbol="sh000001")
            dates = sorted(df["date"].astype(str).tolist(), reverse=True)
            dates = dates[:n_days]
            print(f"  {len(dates)} 个交易日: {dates[-1]} ~ {dates[0]}")
            return dates
        except Exception as e:
            print(f"  失败({attempt+1}/3): {e}")
            time.sleep(5)
    return []


# ═══════════════════════════════════════
# daily_price
# ═══════════════════════════════════════

def get_all_stock_codes(conn):
    """从所有表汇总股票代码。"""
    codes = set()
    for table in ["daily_price", "zt_pool", "zb_pool", "strong_pool", "lhb_detail", "fund_flow"]:
        try:
            rows = conn.execute(f"SELECT DISTINCT stock_code FROM {table}").fetchall()
            for r in rows:
                code = str(r[0]).strip()
                if code and len(code) == 6 and code.isdigit():
                    codes.add(code)
        except Exception:
            pass
    return sorted(codes)


def backfill_daily_price(conn, codes, n_days=30):
    """逐只拉日K线。"""
    print(f"\n[1/4] daily_price: {len(codes)} 只股票, 最近{n_days}天")

    # 已有记录
    existing = set()
    rows = conn.execute("SELECT stock_code, trade_date FROM daily_price").fetchall()
    for code, date in rows:
        existing.add((code, date))
    print(f"  已有 {len(existing)} 条")

    ok, fail, new_rows = 0, 0, 0
    snap_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start = time.time()

    for i, code in enumerate(codes):
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
            if df is not None and not df.empty:
                df = df.tail(n_days)
                batch = []
                for _, row in df.iterrows():
                    td = pd.to_datetime(row["日期"]).strftime("%Y-%m-%d")
                    key = (code, td)
                    if key not in existing:
                        batch.append({
                            "stock_code": code,
                            "trade_date": td,
                            "open": float(row["开盘"]),
                            "high": float(row["最高"]),
                            "low": float(row["最低"]),
                            "close": float(row["收盘"]),
                            "volume": float(row["成交量"]),
                            "amount": float(row["成交额"]) if "成交额" in df.columns else 0,
                            "turnover": float(row["换手率"]) if "换手率" in df.columns else 0,
                            "snapshot_time": snap_time,
                        })
                        existing.add(key)
                if batch:
                    pd.DataFrame(batch).to_sql("daily_price", conn, if_exists="append", index=False)
                    new_rows += len(batch)
                ok += 1
            else:
                fail += 1
        except Exception:
            fail += 1

        if (i + 1) % 50 == 0:
            elapsed = time.time() - start
            eta = elapsed / (i + 1) * (len(codes) - i - 1)
            print(f"  [{i+1}/{len(codes)}] ok={ok} fail={fail} new={new_rows} eta={int(eta)}s")
        time.sleep(0.3)

    conn.commit()
    elapsed = time.time() - start
    print(f"  完成: ok={ok} fail={fail} new={new_rows} {int(elapsed)}s")


# ═══════════════════════════════════════
# pools + lhb
# ═══════════════════════════════════════

def safe_num(df, col, default=0.0):
    if col not in df.columns:
        return pd.Series([default] * len(df))
    return pd.to_numeric(df[col], errors="coerce").fillna(default)

def safe_s(df, col, default=""):
    if col not in df.columns:
        return pd.Series([default] * len(df))
    return df[col].astype(str).fillna(default)


def backfill_pools(conn, trade_dates):
    """回填 zt_pool / zb_pool / strong_pool。"""
    print(f"\n[2/4] pools 回填: {len(trade_dates)} 天")

    # 已有日期
    existing = {}
    for table in ["zt_pool", "zb_pool", "strong_pool"]:
        rows = conn.execute(f"SELECT DISTINCT trade_date FROM {table}").fetchall()
        existing[table] = set(r[0] for r in rows)

    stats = {"zt": [0, 0, 0], "zb": [0, 0, 0], "strong": [0, 0, 0]}  # ok, fail, rows

    for i, date in enumerate(trade_dates):
        # zt_pool
        if date not in existing["zt_pool"]:
            try:
                df = ak.stock_zt_pool_em(date=date.replace("-", ""))
                if df is not None and not df.empty:
                    result = pd.DataFrame({
                        "stock_code": df["代码"].values,
                        "name": safe_s(df, "名称").values,
                        "trade_date": date,
                        "consecutive_zt": safe_num(df, "连板数", 1).astype(int).values,
                        "amount": safe_num(df, "成交额").values,
                        "industry": safe_s(df, "所属行业").values,
                        "circulation_mv": safe_num(df, "流通市值").values,
                        "open_count": safe_num(df, "炸板次数", 0).astype(int).values,
                        "zt_stats": safe_s(df, "涨停统计").values,
                        "snapshot_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    })
                    result.to_sql("zt_pool", conn, if_exists="append", index=False)
                    stats["zt"][0] += 1
                    stats["zt"][2] += len(result)
            except Exception:
                stats["zt"][1] += 1
            time.sleep(1.5)

        # zb_pool
        if date not in existing["zb_pool"]:
            try:
                df = ak.stock_zt_pool_zbgc_em(date=date.replace("-", ""))
                if df is not None and not df.empty:
                    result = pd.DataFrame({
                        "stock_code": df["代码"].values,
                        "trade_date": date,
                        "amount": safe_num(df, "成交额").values,
                        "open_count": safe_num(df, "炸板次数", 0).astype(int).values,
                        "snapshot_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    })
                    result.to_sql("zb_pool", conn, if_exists="append", index=False)
                    stats["zb"][0] += 1
                    stats["zb"][2] += len(result)
            except Exception as e:
                if "30" in str(e):
                    pass  # 炸板池只能30天
                else:
                    stats["zb"][1] += 1
            time.sleep(1.5)

        # strong_pool
        if date not in existing["strong_pool"]:
            try:
                df = ak.stock_zt_pool_strong_em(date=date.replace("-", ""))
                if df is not None and not df.empty:
                    result = pd.DataFrame({
                        "stock_code": df["代码"].values,
                        "name": safe_s(df, "名称").values,
                        "trade_date": date,
                        "amount": safe_num(df, "成交额").values,
                        "reason": safe_s(df, "入选理由").values,
                        "industry": safe_s(df, "所属行业").values,
                        "snapshot_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    })
                    result.to_sql("strong_pool", conn, if_exists="append", index=False)
                    stats["strong"][0] += 1
                    stats["strong"][2] += len(result)
            except Exception:
                stats["strong"][1] += 1
            time.sleep(1.5)

        if (i + 1) % 10 == 0:
            print(f"  [{i+1}/{len(trade_dates)}] zt:{stats['zt']} zb:{stats['zb']} strong:{stats['strong']}")

    conn.commit()
    for name in ["zt", "zb", "strong"]:
        print(f"  {name}: ok={stats[name][0]} fail={stats[name][1]} rows={stats[name][2]}")


def backfill_lhb(conn, trade_dates):
    """回填龙虎榜。"""
    print(f"\n[3/4] lhb 回填: {len(trade_dates)} 天")

    existing = set(r[0] for r in conn.execute("SELECT DISTINCT trade_date FROM lhb_detail").fetchall())
    ok, fail, rows = 0, 0, 0

    for i, date in enumerate(trade_dates):
        if date in existing:
            continue
        try:
            df = ak.stock_lhb_detail_em(start_date=date.replace("-", ""), end_date=date.replace("-", ""))
            if df is not None and not df.empty:
                result = pd.DataFrame({
                    "stock_code": df["代码"].values if "代码" in df.columns else [],
                    "trade_date": date,
                    "buy_amount": pd.to_numeric(df.get("龙虎榜买入额", df.get("买入额", 0)), errors="coerce").fillna(0).values,
                    "sell_amount": pd.to_numeric(df.get("龙虎榜卖出额", df.get("卖出额", 0)), errors="coerce").fillna(0).values,
                    "net_amount": pd.to_numeric(df.get("龙虎榜净买额", df.get("净买入额", 0)), errors="coerce").fillna(0).values,
                    "reason": df.get("上榜原因", pd.Series([""] * len(df))).astype(str).values,
                    "buy_depart": "",
                    "sell_depart": "",
                    "snapshot_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
                result["_row_idx"] = range(len(result))
                result.to_sql("lhb_detail", conn, if_exists="append", index=False)
                ok += 1
                rows += len(result)
        except Exception:
            fail += 1
        time.sleep(1.5)

        if (i + 1) % 10 == 0:
            print(f"  [{i+1}/{len(trade_dates)}] ok={ok} fail={fail} rows={rows}")

    conn.commit()
    print(f"  完成: ok={ok} fail={fail} rows={rows}")


# ═══════════════════════════════════════
# concept_mapping
# ═══════════════════════════════════════

def backfill_concepts(conn):
    """扩充概念映射 — 用东财概念成分股。"""
    print("\n[4/4] concept_mapping 扩充...")

    # 方案A: 从 zt_pool + strong_pool 的 industry 字段提取
    mappings = {}
    for table in ["zt_pool", "strong_pool"]:
        try:
            rows = conn.execute(
                f"SELECT DISTINCT stock_code, industry FROM {table} WHERE industry != ''"
            ).fetchall()
            for code, industry in rows:
                if industry and str(industry).strip():
                    mappings[(code, str(industry).strip())] = True
        except Exception:
            pass

    print(f"  从 industry 字段提取 {len(mappings)} 条")

    # 方案B: 尝试东财概念成分股
    try:
        # 先获取概念列表
        concepts_df = ak.stock_board_concept_name_em()
        if concepts_df is not None and not concepts_df.empty:
            print(f"  东财概念列表: {len(concepts_df)} 个")
            name_col = "板块名称" if "板块名称" in concepts_df.columns else concepts_df.columns[0]

            for i, (_, concept_row) in enumerate(concepts_df.head(50).iterrows()):
                concept_name = str(concept_row[name_col])
                try:
                    cons_df = ak.stock_board_concept_cons_em(symbol=concept_name)
                    if cons_df is not None and not cons_df.empty:
                        code_col = "代码" if "代码" in cons_df.columns else cons_df.columns[0]
                        for _, r in cons_df.iterrows():
                            code = str(r[code_col]).strip()
                            if len(code) == 6:
                                mappings[(code, concept_name)] = True
                    if (i + 1) % 10 == 0:
                        print(f"    [{i+1}/50] {concept_name}: 累计 {len(mappings)} 条")
                except Exception:
                    pass
                time.sleep(0.8)
    except Exception as e:
        print(f"  东财概念接口失败: {e}")

    # 写入
    if mappings:
        df = pd.DataFrame([
            {"stock_code": k[0], "concept_name": k[1]}
            for k in mappings.keys()
        ])
        conn.execute("DELETE FROM concept_mapping")
        conn.commit()
        df["snapshot_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df.to_sql("concept_mapping", conn, if_exists="append", index=False)
        conn.commit()

    final = conn.execute("SELECT count(*), count(DISTINCT stock_code), count(DISTINCT concept_name) FROM concept_mapping").fetchone()
    print(f"  完成: {final[0]} 条, {final[1]} 只股票, {final[2]} 个概念")


# ═══════════════════════════════════════
# main
# ═══════════════════════════════════════

def main():
    n_days = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    print(f"=== 综合回填 === 目标: {n_days} 个交易日")

    trade_dates = get_trade_dates(n_days)
    if not trade_dates:
        print("[ERROR] 无法获取交易日历")
        return

    conn = get_conn()

    # Step 1: daily_price
    codes = get_all_stock_codes(conn)
    backfill_daily_price(conn, codes, n_days)

    # Step 2: pools
    backfill_pools(conn, trade_dates)

    # Step 3: lhb
    backfill_lhb(conn, trade_dates)

    # Step 4: concept_mapping
    backfill_concepts(conn)

    # 汇总
    print("\n=== 最终数据统计 ===")
    for t in ["daily_price", "zt_pool", "zb_pool", "strong_pool", "fund_flow", "lhb_detail"]:
        r = conn.execute(f"SELECT count(*), count(DISTINCT trade_date), min(trade_date), max(trade_date) FROM {t}").fetchone()
        print(f"  {t:20s}: {r[0]:>6} rows, {r[1]:>3} days, {r[2]} ~ {r[3]}")
    r = conn.execute("SELECT count(*), count(DISTINCT stock_code), count(DISTINCT concept_name) FROM concept_mapping").fetchone()
    print(f"  concept_mapping:     {r[0]:>6} rows, {r[1]:>4} stocks, {r[2]:>3} concepts")

    conn.close()
    print("\n全部完成!")


if __name__ == "__main__":
    main()
