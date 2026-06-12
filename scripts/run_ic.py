"""运行IC管线并输出结果。"""
import warnings
warnings.filterwarnings("ignore")

from src.data.storage import Storage
from src.pipeline.runner import run_ic_pipeline

db = Storage("data/alpha_miner.db")
db.init_db()
results = run_ic_pipeline(db)
for fn, info in results.items():
    ic_str = "%.4f" % info["avg_ic"] if info["avg_ic"] == info["avg_ic"] else "N/A"
    print(f"  {fn}: {info['valid_ic']}/{info['dates']} valid IC, avg={ic_str}")
