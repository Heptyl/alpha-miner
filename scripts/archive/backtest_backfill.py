"""
延长回测到3年 — 策略B「涨停次日低吸」

核心发现: 我们之前已经验证过从daily_price反推涨停(涨幅>=9.5%)是可行的。
所以不需要补采zt_pool, 只需要补采daily_price。

但3年daily_price ≈ 750天 × 5500只 = 412万条, baostock串行需要62小时。
实际可行方案: 只补采关键数据(每天涨停股的次日表现)。

方案:
1. 从akshare的stock_zh_a_spot_em获取全量股票列表
2. 用akshare的stock_zh_a_hist批量拉取每只股票的历史日K
3. 筛选出涨停股(涨幅>=9.5%)
4. 匹配次日表现

或者更快: 直接用已有的218天数据, 不补采, 但加入以下替代验证:
1. 用akshare的涨停池历史接口(stock_zt_pool_em)试试能拉多少
2. 用东财的数据接口拉涨停历史
"""
import sqlite3
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def check_akshare_zt_history():
    """检查akshare涨停池历史数据可用性"""
    import akshare as ak
    
    # 尝试拉取历史涨停池
    print("测试akshare涨停池历史接口...")
    try:
        # stock_zt_pool_em 只支持当天
        df = ak.stock_zt_pool_em(date="20240101")
        print(f"  stock_zt_pool_em(20240101): {len(df)}条")
    except Exception as e:
        print(f"  stock_zt_pool_em(20240101): 失败 - {e}")
    
    try:
        # stock_zt_pool_previous_em 涨停池(昨日涨停)
        df = ak.stock_zt_pool_previous_em(date="20240102")
        print(f"  stock_zt_pool_previous_em(20240102): {len(df)}条")
    except Exception as e:
        print(f"  stock_zt_pool_previous_em(20240102): 失败 - {e}")
    
    # 尝试用stock_zh_a_hist拉单只股票历史
    print("\n测试单只股票历史K线...")
    try:
        df = ak.stock_zh_a_hist(symbol="000001", period="daily", 
                                start_date="20220101", end_date="20241231",
                                adjust="qfq")
        print(f"  000001 平安银行 2022-2024: {len(df)}条")
        print(f"  列: {list(df.columns)}")
        print(f"  前3行:\n{df.head(3)}")
    except Exception as e:
        print(f"  stock_zh_a_hist 失败: {e}")


def check_backfill_feasibility():
    """评估补采可行性"""
    import akshare as ak
    
    print("\n=== 补采可行性评估 ===")
    
    # 方案A: 逐只拉取stock_zh_a_hist
    # 约5500只 × 1秒/只 ≈ 1.5小时 (一次拉3年)
    # 然后从数据中筛选涨停股
    print("方案A: stock_zh_a_hist 逐只拉3年")
    print("  预计: ~5500只 × ~1s = ~1.5小时")
    print("  优点: 一次拉完3年, 有完整K线数据")
    print("  缺点: 数据量大(~2000万条), 需要反推涨停")
    
    # 方案B: baostock逐天拉全量
    print("\n方案B: baostock 逐天全量拉取")  
    print("  预计: 750天 × 5min = ~62小时")
    print("  优点: 每天全量5490只, 有pre_close")
    print("  缺点: 太慢")

    # 方案C: 只拉涨停股的K线
    print("\n方案C: 先从东财获取历史涨停列表,再拉对应K线")
    print("  预计: ~2万只涨停记录 × 1s = ~6小时")
    print("  优点: 数据量小, 精准")
    print("  缺点: 需要东财历史涨停列表接口")
    
    print("\n推荐: 方案A (先拉500只大盘股验证, 再全量)")


if __name__ == '__main__':
    check_akshare_zt_history()
    check_backfill_feasibility()
