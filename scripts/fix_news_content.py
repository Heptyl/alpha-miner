"""补全历史新闻正文v2 — 标题匹配"""
import sqlite3, time, pandas as pd

DB_PATH = "data/alpha_miner.db"

def main():
    conn = sqlite3.connect(DB_PATH)

    total = conn.execute("SELECT COUNT(*) FROM news").fetchone()[0]
    empty = conn.execute("SELECT COUNT(*) FROM news WHERE content IS NULL OR length(content) < 50").fetchone()[0]
    print(f"新闻总量: {total}, 正文缺失: {empty} ({empty*100//max(total,1)}%)")

    if empty == 0:
        print("无需补全")
        return

    # 看看缺失新闻的stock_code分布
    codes_with = conn.execute("""
        SELECT stock_code, COUNT(*) FROM news
        WHERE (content IS NULL OR length(content) < 50) AND stock_code IS NOT NULL AND stock_code != ''
        GROUP BY stock_code ORDER BY COUNT(*) DESC LIMIT 20
    """).fetchall()
    no_code = conn.execute("""
        SELECT COUNT(*) FROM news
        WHERE (content IS NULL OR length(content) < 50) AND (stock_code IS NULL OR stock_code = '')
    """).fetchone()[0]

    print(f"无代码缺失: {no_code}")
    print(f"有代码缺失: {empty - no_code}")
    for c in codes_with[:5]:
        print(f"  {c[0]}: {c[1]}条")

    # 对有stock_code的新闻按代码批量拉取
    needs = conn.execute("""
        SELECT news_id, stock_code, title FROM news
        WHERE (content IS NULL OR length(content) < 50)
        AND stock_code IS NOT NULL AND stock_code != ''
        ORDER BY publish_time DESC
        LIMIT 1000
    """).fetchall()
    print(f"\n有代码待补: {len(needs)}条")

    if not needs:
        print("无可补全的新闻")
        return

    # 构建标题→news_id映射
    title_map = {}
    for nid, code, title in needs:
        title_map[title.strip()] = nid

    codes = list(set(r[1] for r in needs))
    print(f"涉及{len(codes)}只股票")

    fixed = 0
    for i, code in enumerate(codes):
        if i > 0 and i % 5 == 0:
            print(f"  进度: {i}/{len(codes)} fixed={fixed}")
            conn.commit()
            time.sleep(0.5)

        try:
            import akshare as ak
            with pd.option_context("future.infer_string", False):
                df = ak.stock_news_em(symbol=code)
            if df is None or df.empty:
                continue
            for _, row in df.iterrows():
                title = str(row.get("新闻标题", "")).strip()
                content = str(row.get("新闻内容", ""))
                if title in title_map and len(content) >= 30:
                    conn.execute("UPDATE news SET content=? WHERE news_id=?", (content, title_map[title]))
                    fixed += 1
        except Exception as e:
            if i < 3:
                print(f"  {code}失败: {str(e)[:50]}")

    conn.commit()
    empty_after = conn.execute("SELECT COUNT(*) FROM news WHERE content IS NULL OR length(content) < 50").fetchone()[0]
    print(f"\n结果: fixed={fixed}, 剩余缺失={empty_after}/{total}")

if __name__ == "__main__":
    main()
