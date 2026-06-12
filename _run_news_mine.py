import sys
sys.path.insert(0, ".")
from src.data.sources.news_miner import run_full_mine
result = run_full_mine()
print(f'采集完成: {result["stats"]}')
