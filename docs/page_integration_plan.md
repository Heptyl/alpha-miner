# Alpha Miner 页面联动优化方案

## 一、当前问题

### 1.1 页面孤岛
15个页面各自独立，数据不流动：
- 市场总览看完涨停 → 无法跳转到对应股票的行情回溯
- 9维选股选出高分股 → 无法加入自选/交易计划
- ML选股TOP7 → 实时盯盘需要手动复制代码
- 每日推荐 → 不能一键生成交易计划

### 1.2 功能重叠
- 2尾盘选股 / 10每日推荐 / 11 ML选股 → 都是选股，结果不融合
- 3持仓管理 / 15实时盯盘 → 都管持仓，数据源不同
- 14信号监控 / 15实时盯盘 → 都出信号，标准不同

### 1.3 决策链断裂
用户操作流程：看推荐(10) → 查K线(6) → 看持仓(3) → 做交易计划(12)
4个页面之间需要手动记忆和传递信息。

### 1.4 低价值页面
- 4因子看板：纯展示，没有决策输出
- 5复盘日志 / 7推荐跟踪：功能重叠，孤立运行
- 6行情回溯：没有联动入口，必须手动输入股票代码

## 二、设计原则 (参考Qlib/vnpy/Streamlit最佳实践)

### 原则1: 决策漏斗
```
发现(市场/新闻) → 选股(9维/ML) → 验证(行情/因子) → 决策(交易计划) → 执行(盯盘)
```
每一层是上一层的输出，用户沿漏斗推进。

### 原则2: 股票上下文传递
点击任何股票代码 → 带上下文跳转 → 目标页面自动加载该股票的完整信息。

### 原则3: 统一信号流
所有信号统一格式：`{code, direction, strength, reason, source}`
不同来源的信号在一个面板融合展示。

### 原则4: session_state 共享状态
```python
st.session_state["selected_stock"] = "300059"    # 当前选中股票
st.session_state["portfolio"] = {...}            # 持仓数据
st.session_state["watchlist"] = [...]             # 自选股
st.session_state["available_cash"] = 10189        # 可用资金
st.session_state["signals"] = [...]               # 统一信号池
```
所有页面读写同一个state → 自动联动。

## 三、优化后的页面架构

### 第一层：市场发现 (2个页面)
| 页面 | 功能 | 联动输出 |
|------|------|----------|
| 1 市场总览 | 大盘/涨停/板块 | 点击股票 → 跳转6行情回溯 |
| 8 新闻热点 | 全球/新闻/情绪 | 点击概念 → 跳转9九维选股 |

### 第二层：选股引擎 (3个页面，合并后)
| 页面 | 功能 | 联动输出 |
|------|------|----------|
| 9 九维选股 | 多策略融合 | 点击股票 → 加入自选/生成交易计划 |
| 11 ML选股 | ML预测+回测 | TOP7自动流入15实时盯盘 |
| 2 尾盘选股 | 尾盘策略+交易计划 | 合并10每日推荐，统一推荐源 |

### 第三层：验证工具 (2个页面)
| 页面 | 功能 | 联动入口 |
|------|------|----------|
| 6 行情回溯 | 个股深度分析 | 从任何页面点击股票代码进入 |
| 4 因子看板 | 因子有效性 | 增加决策输出：标记失效因子 |

### 第四层：决策执行 (2个页面)
| 页面 | 功能 | 联动 |
|------|------|------|
| 12 交易计划 | 买卖指令 | 自动接收选股结果+持仓状态+资金 |
| 15 实时盯盘 | 盘中实时 | 整合持仓+ML候选+信号+换仓联动 |

### 第五层：复盘验证 (3个页面)
| 页面 | 功能 | 联动 |
|------|------|------|
| 3 持仓管理 | 仓位/止损/盈亏 | 与15实时盯盘共享session_state |
| 13 模拟盘 | 策略回测 | 接收12交易计划的信号 |
| 5 复盘日志 | 合并7推荐跟踪 | 自动关联历史推荐表现 |

## 四、具体联动实现

### 4.1 全局状态服务 (web/state.py)
```python
import streamlit as st

def init_state():
    """每个页面开头调用，初始化全局状态"""
    defaults = {
        "selected_stock": None,        # 当前选中股票
        "selected_stock_name": None,   # 股票名称
        "watchlist": [],               # 自选股列表
        "portfolio": {...},            # 持仓数据
        "available_cash": 10189,       # 可用资金
        "ml_candidates": [],           # ML候选
        "signals": [],                 # 统一信号池
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

def select_stock(code, name=None):
    """选中股票 — 所有页面响应"""
    st.session_state.selected_stock = code
    st.session_state.selected_stock_name = name

def add_to_watchlist(code, name):
    """加入自选 — 选股页面调用"""
    if code not in [w["code"] for w in st.session_state.watchlist]:
        st.session_state.watchlist.append({"code": code, "name": name})

def remove_from_watchlist(code):
    """移除自选"""
    st.session_state.watchlist = [w for w in st.session_state.watchlist if w["code"] != code]
```

### 4.2 股票链接组件 (web/components.py)
```python
def stock_link(code, name):
    """可点击的股票链接 — 跳转到行情回溯"""
    import streamlit as st
    from web.state import select_stock
    if st.button(f"📊 {code} {name}", key=f"link_{code}"):
        select_stock(code, name)
        st.switch_page("pages/6_history.py")

def add_to_plan_button(code, name, price):
    """加入交易计划按钮"""
    import streamlit as st
    if st.button(f"📋 加入交易计划", key=f"plan_{code}"):
        # 写入trade plan
        ...
```

### 4.3 侧边栏全局面板
在app.py侧边栏底部增加全局信息面板：
```python
with st.sidebar:
    st.markdown("---")
    # 当前选中股票
    if st.session_state.get("selected_stock"):
        st.info(f"📊 {st.session_state.selected_stock} {st.session_state.selected_stock_name}")
        if st.button("查看行情"):
            st.switch_page("pages/6_history.py")

    # 自选股列表
    if st.session_state.get("watchlist"):
        st.markdown("**自选股:**")
        for w in st.session_state.watchlist:
            st.caption(f"• {w['code']} {w['name']}")
```

### 4.4 页面改造优先级

**P0 — 立刻做 (核心联动)**
1. 创建 web/state.py + web/components.py
2. 修改 app.py 侧边栏加入全局面板
3. 15实时盯盘读取session_state.portfolio (不再硬编码)
4. 6行情回溯自动读取session_state.selected_stock
5. 9九维/11ML选股 → 加入自选/加入交易计划按钮

**P1 — 本周做 (选股融合)**
6. 合并10每日推荐到2尾盘选股
7. 12交易计划自动接收选股结果
8. 5复盘日志合并7推荐跟踪
9. 1市场总览 → 点击涨停股跳转6行情

**P2 — 后续优化**
10. 3持仓管理与15实时盯盘共享session_state
11. 13模拟盘接收12交易计划信号
12. 4因子看板增加决策输出(失效因子告警)

## 五、预期效果

### 用户操作流程 (优化前 vs 优化后)

**优化前**: 看推荐 → 手动记代码 → 切到行情回溯输入代码 → 看K线 → 切到持仓看有没有钱 → 切到交易计划输入 → 切到盯盘看信号
= 5次页面切换 + 3次手动输入

**优化后**: 市场总览点涨停股 → 自动跳行情回溯 → 点"加入自选" → 交易计划自动出现 → 实时盯盘自动跟踪
= 3次点击，零手动输入

### 每个页面的独特价值 (优化后)
1. 市场总览: 发现阶段，涨停/板块异动入口
2. 尾盘选股: 统一推荐源 + 交易计划生成
3. 持仓管理: 仓位调整 + 止损管理 (与15共享状态)
4. 因子看板: 因子质量监控 + 失效告警
5. 复盘日志: 合并推荐跟踪，自动关联历史表现
6. 行情回溯: 个股深度分析中心 (从任何页面跳入)
7. (删除，功能合并到5)
8. 新闻热点: 全球市场 + 情绪 + 概念联动
9. 九维选股: 多策略选股 + 加入自选/交易计划
10. (删除，功能合并到2)
11. ML选股: ML预测 + 模型管理 + 候选流入盯盘
12. 交易计划: 自动接收选股结果 + 资金计算
13. 模拟盘: 策略验证 (接收交易计划信号)
14. 信号监控: T+1信号 + 与15实时信号互补
15. 实时盯盘: 盘中决策中心 (持仓+ML+换仓)

最终: 15页 → 13页(合并2组)，每个页面都是决策漏斗的一环
