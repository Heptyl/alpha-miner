#!/usr/bin/env python3
"""今日市场行情分析"""
import sqlite3
import pandas as pd
import numpy as np

conn = sqlite3.connect("data/alpha_miner.db")

print("=" * 60)
print("   A股市场行情分析 — 2026-04-24 (今日)")
print("=" * 60)

# 1. 市场情绪总览
me = pd.read_sql("SELECT * FROM market_emotion ORDER BY rowid DESC LIMIT 1", conn)
print("\n【一、市场情绪总览】")
print(f"  涨停数: {me.iloc[0]['zt_count']}     跌停数: {me.iloc[0]['dt_count']}")
print(f"  上涨家数: {me.iloc[0]['up_count']}   下跌家数: {me.iloc[0]['down_count']}")
print(f"  最高连板: {me.iloc[0]['highest_board']}板")
print(f"  情绪等级: {me.iloc[0]['sentiment_level']}")
print(f"  市场活跃度: {me.iloc[0]['activity']}")
zt = me.iloc[0]['zt_count']
dt = me.iloc[0]['dt_count']
ratio = zt / (zt + dt) * 100 if (zt + dt) > 0 else 0
print(f"  涨停/(涨停+跌停): {ratio:.1f}%")

# 2. 涨停板块分布
zt_pool = pd.read_sql("SELECT * FROM zt_pool WHERE snapshot_time LIKE '2026-04-24%'", conn)
print(f"\n【二、涨停板块分布】 (共{len(zt_pool)}只涨停)")
if 'industry' in zt_pool.columns:
    ind = zt_pool['industry'].value_counts().head(15)
    for name, cnt in ind.items():
        print(f"  {name:12s} {cnt:>3d} 只  {'█' * cnt}")

# 3. 连板梯队
print(f"\n【三、连板梯队】")
for board in sorted(zt_pool['consecutive_zt'].unique(), reverse=True):
    stocks = zt_pool[zt_pool['consecutive_zt'] == board]
    names = stocks['name'].tolist()[:10]
    print(f"  {board}板 ({len(stocks)}只): {', '.join(names)}")

# 4. 资金流向 TOP
ff = pd.read_sql("SELECT * FROM fund_flow WHERE snapshot_time LIKE '2026-04-24%'", conn)
print(f"\n【四、主力资金净流入 TOP15】 (万元)")
if 'main_net' in ff.columns:
    ff['mn'] = pd.to_numeric(ff['main_net'], errors='coerce')
    top_in = ff.nlargest(15, 'mn')
    for _, r in top_in.iterrows():
        name = str(r.get('stock_name', r.get('stock_code', '')))
        print(f"  {name:12s}  净流入: {r['mn']:>12,.0f} 万  涨跌: {r.get('pct_change', 'N/A')}%")
    
    print(f"\n【五、主力资金净流出 TOP15】 (万元)")
    top_out = ff.nsmallest(15, 'mn')
    for _, r in top_out.iterrows():
        name = str(r.get('stock_name', r.get('stock_code', '')))
        print(f"  {name:12s}  净流出: {r['mn']:>12,.0f} 万  涨跌: {r.get('pct_change', 'N/A')}%")

# 6. 炸板分析
zb = pd.read_sql("SELECT * FROM zb_pool WHERE snapshot_time LIKE '2026-04-24%'", conn)
print(f"\n【六、炸板池】 (共{len(zb)}只)")
if 'industry' in zb.columns and len(zb) > 0:
    zb_ind = zb['industry'].value_counts().head(10)
    for name, cnt in zb_ind.items():
        print(f"  {name:12s} {cnt:>3d} 只")

# 7. 新闻热点
news = pd.read_sql(
    "SELECT title, sentiment_score, news_type FROM news WHERE snapshot_time LIKE '2026-04-24%' ORDER BY sentiment_score DESC LIMIT 20",
    conn
)
print(f"\n【七、今日新闻热点 TOP20 (按情绪)】")
for _, r in news.iterrows():
    s = r['sentiment_score']
    emoji = "+" if s > 0.6 else ("-" if s < 0.4 else "=")
    print(f"  [{emoji}] [{s:.2f}] {r['title'][:50]}")

# 8. 概念板块
cd = pd.read_sql("SELECT * FROM concept_daily WHERE snapshot_time LIKE '2026-04-24%'", conn)
print(f"\n【八、概念板块聚合】")
print(cd.to_string())

conn.close()
print("\n" + "=" * 60)
print("   分析完成")
print("=" * 60)
