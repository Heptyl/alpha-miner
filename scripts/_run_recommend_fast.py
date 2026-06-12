#!/usr/bin/env python3
"""快速跑推荐并输出"""
import sys, json
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

trade_date = '2026-05-06'
as_of = datetime.now()

print('[1] 生成推荐...')
from src.data.storage import Storage
from src.strategy.recommend import RecommendEngine

db = Storage('data/alpha_miner.db')
engine = RecommendEngine(db)

# 直接跳过耗时步骤
result = engine.recommend(as_of=as_of, report_date=trade_date)

print(f'推荐 {len(result.stocks)} 只')
for i, r in enumerate(result.stocks, 1):
    d = r.to_dict()
    cp = d.get('buy_zone_high', 0)  # 校验后 buy_zone_high 约等于收盘价
    print(f"{i}. {d['stock_name']}({d['stock_code']}) 综合{d['composite_score']:.2f} {d['signal_level']}级")
    print(f"   今日收{cp} | 竞价{d['auction_price']} | 低吸{d['dip_price']} | 止损{d['stop_loss']} | 目标{d['target_price']}")

# 保存
rec_path = Path(__file__).parent.parent / 'recommendations'
rec_path.mkdir(exist_ok=True)
recs_data = [r.to_dict() for r in result.stocks]
out_file = rec_path / f'{trade_date}_recommend.json'
with open(out_file, 'w', encoding='utf-8') as f:
    json.dump({'date': trade_date, 'stocks': recs_data, 'generated_at': datetime.now().isoformat()}, f, ensure_ascii=False, indent=2)
print(f'\n已保存: {out_file}')
