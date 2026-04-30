"""快速尝试用akshare拉今日数据"""
try:
    import akshare as ak
    print("akshare version:", ak.__version__)
    
    # Try different approach
    df = ak.stock_zh_a_daily(symbol="sh000001", start_date="20260428", end_date="20260429", adjust="qfq")
    print(f"Index stock - Got {len(df)} rows")
    print(df.tail())
except Exception as e:
    print(f"Error with index: {e}")

try:
    import akshare as ak
    # Try individual stock
    df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20260425", end_date="20260429", adjust="qfq")
    print(f"\nHist - Got {len(df)} rows")
    print(df.tail())
except Exception as e:
    print(f"Error with hist: {e}")
