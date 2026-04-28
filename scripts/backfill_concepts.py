"""扩充 concept_mapping — 用同花顺概念成分股。

当前 concept_mapping 仅 474 条（从行业字段提取），远不够。
这个脚本拉取同花顺热门概念的成分股，大幅扩充映射关系。

用法: python scripts/backfill_concepts.py
"""

import sqlite3
import sys
import time

import akshare as ak
import pandas as pd

DB_PATH = "data/alpha_miner.db"


def main():
    conn = sqlite3.connect(DB_PATH)
    existing = conn.execute("SELECT count(*) FROM concept_mapping").fetchone()[0]
    print(f"当前 concept_mapping: {existing} 条")

    # 1. 获取概念列表
    print("\n[1/2] 拉取同花顺概念列表...")
    concepts = []
    for attempt in range(3):
        try:
            df = ak.stock_board_concept_name_ths()
            if df is not None and not df.empty:
                concepts = df.to_dict("records")
                print(f"  获取 {len(concepts)} 个概念")
                break
        except Exception as e:
            print(f"  尝试 {attempt+1}/3 失败: {e}")
            time.sleep(3)

    if not concepts:
        print("[ERROR] 无法获取概念列表")
        conn.close()
        return

    # 2. 逐个拉取成分股
    print(f"\n[2/2] 拉取成分股（最多200个概念）...")
    all_mappings = []
    fail_count = 0

    for i, concept in enumerate(concepts[:200]):
        concept_name = concept.get("概念名称", concept.get("name", ""))
        concept_code = concept.get("代码", concept.get("code", ""))

        if not concept_name:
            continue

        try:
            # 同花顺概念成分股
            df = ak.stock_board_concept_cons_ths(symbol=concept_name)
            if df is not None and not df.empty:
                code_col = "代码" if "代码" in df.columns else "stock_code"
                for _, row in df.iterrows():
                    all_mappings.append({
                        "stock_code": str(row[code_col]),
                        "concept_name": concept_name,
                    })
                if (i + 1) % 20 == 0:
                    print(f"  [{i+1}/{min(len(concepts),200)}] {concept_name}: +{len(df)} 只, 累计 {len(all_mappings)}")
            else:
                fail_count += 1
        except Exception as e:
            fail_count += 1
            if fail_count <= 3:
                print(f"  [{i+1}] {concept_name} 失败: {e}")

        time.sleep(0.8)  # 限速

    # 去重
    df_mappings = pd.DataFrame(all_mappings).drop_duplicates(subset=["stock_code", "concept_name"])
    print(f"\n总计 {len(df_mappings)} 条映射（去重后）")

    # 合并已有数据（保留旧的 + 新增）
    if not df_mappings.empty:
        # 清空旧数据重新写入
        conn.execute("DELETE FROM concept_mapping")
        conn.commit()

        # 添加 snapshot_time
        df_mappings["snapshot_time"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        df_mappings.to_sql("concept_mapping", conn, if_exists="append", index=False)
        conn.commit()

    final = conn.execute("SELECT count(*) FROM concept_mapping").fetchone()[0]
    stocks = conn.execute("SELECT count(DISTINCT stock_code) FROM concept_mapping").fetchone()[0]
    concepts_count = conn.execute("SELECT count(DISTINCT concept_name) FROM concept_mapping").fetchone()[0]

    conn.close()
    print(f"\n完成: {final} 条映射, {stocks} 只股票, {concepts_count} 个概念")


if __name__ == "__main__":
    main()
