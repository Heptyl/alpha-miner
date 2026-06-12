import json

with open('data/news_hotspot.json', 'r') as f:
    data = json.load(f)

hotspot = data.get('hotspot', {})

# Top themes
themes = hotspot.get('top_themes', [])
print(f'\n=== 热点主题排名 ({len(themes)} 个) ===')
for i, t in enumerate(themes, 1):
    print(f'  {i}. {t["name"]}  相关新闻数={t["count"]}')

# Top concepts
concepts = hotspot.get('top_concepts', [])
print(f'\n=== 关联板块排名 ({len(concepts)} 个) ===')
for i, c in enumerate(concepts, 1):
    print(f'  {i}. {c["name"]}  相关新闻数={c["count"]}')

# Top tags
tags = hotspot.get('top_tags', [])
print(f'\n=== 新闻标签排名 ({len(tags)} 个) ===')
for i, t in enumerate(tags, 1):
    print(f'  {i}. {t["name"]}  相关新闻数={t["count"]}')

# ZT stocks
zt = data.get('zt_stocks', [])
print(f'\n=== 涨停池股票 ({len(zt)} 只) ===')
if isinstance(zt, list):
    for s in zt[:10]:
        if isinstance(s, dict):
            print(f'  {s.get("code","")} {s.get("name","")} 涨停时间={s.get("first_zt_time","")} 封板={s.get("board_count","")}连板')
        else:
            print(f'  {s}')
    if len(zt) > 10:
        print(f'  ... 共 {len(zt)} 只')

print(f'\n更新时间: {data.get("updated", "")}')
