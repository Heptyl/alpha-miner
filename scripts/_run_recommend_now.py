#!/usr/bin/env python3
"""手动跑推荐并输出推送文本"""
import sys, json
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

trade_date = '2026-05-06'
as_of = datetime.now()

print(f'[4/5] 生成推荐 {trade_date}...')

from src.data.storage import Storage
from src.strategy.recommend import RecommendEngine

db = Storage('data/alpha_miner.db')
engine = RecommendEngine(db)
result = engine.recommend(as_of=as_of, report_date=trade_date)

recs = result.stocks if hasattr(result, 'stocks') else []
print(f'  推荐 {len(recs)} 只')
for i, r in enumerate(recs, 1):
    d = r.to_dict() if hasattr(r, 'to_dict') else r
    name = d.get('name', d.get('code', '?'))
    code = d.get('code', '')
    score = d.get('score', 0)
    reason = d.get('reason', '')[:80]
    print(f'  {i}. {name}({code}) score={score:.3f} {reason}')

# 保存
rec_path = Path(__file__).parent.parent / 'recommendations'
rec_path.mkdir(exist_ok=True)
recs_data = [r.to_dict() if hasattr(r, 'to_dict') else r for r in recs]
out_file = rec_path / f'{trade_date}_recommend.json'
with open(out_file, 'w', encoding='utf-8') as f:
    json.dump({'date': trade_date, 'stocks': recs_data, 'generated_at': datetime.now().isoformat()}, f, ensure_ascii=False, indent=2)
print(f'[5/5] 已保存: {out_file}')

# 推送文本
print('===PUSH_START===')
text = f'Alpha Miner 晚间推荐 {trade_date}\n\n'
for i, r in enumerate(recs, 1):
    d = r.to_dict() if hasattr(r, 'to_dict') else r
    name = d.get('name', '?')
    code = d.get('code', '')
    score = d.get('score', 0)
    reason = d.get('reason', '')
    text += f'{i}. {name}({code}) 综合{score:.2f}\n'
    if reason:
        text += f'   {reason}\n'
    text += '\n'
print(text)
print('===PUSH_END===')
