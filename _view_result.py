import json

with open('data/news_hotspot.json', 'r') as f:
    data = json.load(f)

print('=== 采集概况 ===')
if 'stats' in data:
    print(json.dumps(data['stats'], ensure_ascii=False, indent=2))

if 'hotspots' in data:
    print(f'\n=== 热点主题 ({len(data["hotspots"])} 个) ===')
    for i, h in enumerate(data['hotspots'][:15], 1):
        name = h.get('concept_name', h.get('name', ''))
        count = h.get('news_count', h.get('count', 0))
        score = h.get('hot_score', h.get('score', ''))
        print(f'  {i}. {name}  新闻数={count}  热度={score}')
elif 'themes' in data:
    print(f'\n=== 热点主题 ({len(data["themes"])} 个) ===')
    for i, t in enumerate(data['themes'][:15], 1):
        print(f'  {i}. {json.dumps(t, ensure_ascii=False)}')
else:
    print('\n=== 数据结构键 ===')
    print(list(data.keys()))
    print('\n=== 前1000字符 ===')
    print(json.dumps(data, ensure_ascii=False)[:1000])
