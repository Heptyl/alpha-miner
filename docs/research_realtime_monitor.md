# A股实时盯盘系统调研报告

## 一、数据源调研

### 1. 腾讯实时行情 (qt.gtimg.cn) ⭐⭐⭐⭐⭐ (推荐)

- **数据内容**: 最新价/开/高/低/收/量/额 + 买卖五档盘口 + 换手率/PE
- **延迟**: 实时(约3秒)
- **成本**: 免费
- **WSL2可行性**: 通过Windows curl绕行，稳定可靠
- **限制**: 单次最多约50只股票
- **编码**: GBK，需decode('gbk')

```python
# 获取单只股票实时行情
import subprocess
r = subprocess.run(['/mnt/c/Windows/System32/curl.exe', '-s', '--max-time', '5',
    'http://qt.gtimg.cn/q=sz300059'], capture_output=True, timeout=10)
data = r.stdout.decode('gbk')
# 解析: 最新价=parts[3], 涨跌幅=parts[32], 五档=parts[9~29]
```

### 2. 东财分钟K线 (akshare) ⭐⭐⭐

- **数据内容**: 1min/5min/15min K线(OHLCV)
- **延迟**: 历史数据(非实时)
- **WSL2可行性**: 不稳定，经常被IP封锁
- **备选方案**: 通过Windows curl直接调东财API

### 3. pytdx (通达信) ⭐⭐⭐⭐

- **GitHub**: rainx/pytdx (1k+ stars)
- **数据内容**: 实时行情+分钟K线+日K线+Tick数据
- **延迟**: 实时(约3秒)
- **优点**: 数据稳定，通达信服务器遍布全国
- **缺点**: 需要连接通达信服务器，有时不稳定
- **安装**: pip install pytdx

### 4. 新浪实时行情 ⭐⭐

- **WSL2可行性**: 不可用(超时)
- **需通过Windows curl绕行**

## 二、GitHub参考项目

### 实时监控相关

1. **pytdx** (rainx/pytdx) - 1k stars
   通达信数据接口，支持实时行情+分钟线

2. **easyquotation** (shidenggui/easyquotation) - 2k stars
   实时获取A股行情(新浪/腾讯/QQ), 简单易用
   安装: pip install easyquotation

3. **dfhq** (kershuaimo/dfhq) - 少stars但功能精准
   东方财富分时图数据获取

4. **stock\_monitor** (多个同名项目)
   A股实时监控+价格提醒+微信推送

### 日内交易信号

5. **vnpy** (vnpy/vnpy) - 24k stars
   全栈量化，CTA策略模块有日内交易信号
   参考: vnpy/trader/indicator.py 的技术指标实现

6. **backtrader** (mementum/backtrader) - 13k stars  
   经典回测框架，indicator模块可参考日内指标计算

### 实时可视化

7. **streamlit-autorefresh** 
   Streamlit页面自动刷新组件
   安装: pip install streamlit-autorefresh

8. **plotly** 
   交互式图表，适合分时图展示

## 三、推荐技术方案

### 架构

```
数据采集层:
  腾讯API (Windows curl) → 实时行情 (每10秒刷新)
  东财API (akshare) → 5分钟K线 (每5分钟刷新)

信号计算层:
  技术指标引擎 → MACD/KDJ/RSI/布林/MA (实时计算)
  量价异动检测 → 放量/急涨/急跌/大单 (实时检测)

展示层:
  Streamlit Web UI → 分时图+信号+盘口 (每30秒自动刷新)
  推送通知 → 关键信号推送 (Telegram/微信)
```

### 实现步骤

1. **实时数据采集器** (src/trader/realtime_quote.py)
   - 腾讯API封装，支持批量获取
   - 10秒轮询，返回标准化数据

2. **分钟级技术指标** (src/trader/intraday_signal.py)  
   - 基于实时价格计算MACD/KDJ/RSI
   - 信号触发条件:
     - MACD金叉/死叉 → 买入/卖出信号
     - KDJ超卖(J<20) + 价格在支撑位 → 买入
     - KDJ超买(J>80) + 价格到压力位 → 卖出
     - 急涨(5分钟涨>2%) + 放量 → 追涨信号
     - 急跌(5分钟跌>2%) + 放量 → 止损信号

3. **Web UI实时面板** (web/pages/15_realtime_monitor.py)
   - 分时图 (plotly交互式图表)
   - 实时盘口 (买卖五档)
   - 信号闪烁 (买入/卖出/异动)
   - 30秒自动刷新

4. **推送通知**
   - 关键信号实时推送
   - "东方电气 现价38.50 突破压力位38.00，建议减仓"
   - "科新发展 RSI=25 超卖反弹，可关注"

### 关键技术点

1. **Streamlit自动刷新**: 使用streamlit-autorefresh或st.empty()+time.sleep()
2. **分钟K线图**: 使用plotly.graph_objects.Figure，支持缩放拖拽
3. **实时信号**: 计算5分钟K线的技术指标，检测信号变化
4. **WSL2网络**: 所有外部请求通过Windows curl绕行
