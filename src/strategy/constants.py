"""
策略常量 — 所有策略属性的单一来源(Single Source of Truth)


其他模块引用策略属性时, 只从这个文件取, 不允许硬编码。

用法:
  from src.strategy.constants import STRATEGIES, get_strategy_by_signal

  # 获取策略ID
  strategy_id = STRATEGIES["A"]["id"]

  # 通过信号类型反查策略
  strategy_id = get_strategy_by_signal("趋势牛股(策略C)")  # → "C"
"""

# 策略信号类型 → 策略ID 的映射(支持多种匹配方式)
SIGNAL_MAP = {
    # 策略A的信号类型
    "首阴日内": "A",
    "龙头首阴": "A",
    "策略A": "A",
    # 策略B的信号类型
    "回踩低吸": "B",
    "低开反弹": "B",
    "暴跌日狙击": "B",        # v2: 暴跌日好公司狙击
    "暴跌日狙击(策略B)": "B",
    "策略B": "B",
    # 策略C的信号类型(趋势牛股)
    "趋势牛股": "C",
    "趋势牛股(策略C)": "C",
    "策略C": "C",
    # 兼容旧信号(数据库中可能还有策略D标记)
    "缩量反包": "C",
    "缩量反包(策略C)": "C",
    "趋势牛股(策略D)": "C",  # DB中旧数据兼容
    "策略D": "C",  # DB中旧数据兼容
}

# 策略完整配置
STRATEGIES = {
    "A": {
        "id": "A",
        "name": "龙头首阴反包",
        "signal_type": "首阴日内",
        "initial_capital": 30_000,
        "max_positions": 3,
        "position_ratio": 0.33,
        "stop_loss_pct": -0.05,
        "max_hold_days": 3,        # 持2-3天(跌破首阴低止损)
    },
    "B": {
        "id": "B",
        "name": "暴跌日好公司狙击",
        "signal_type": "暴跌日狙击(策略B)",
        "enabled": True,
        "initial_capital": 10_000,
        "max_positions": 1,
        "position_ratio": 1.0,
        "stop_loss_pct": -0.06,  # v2: 止损-6%
        "max_hold_days": 7,      # v2: 持7天
    },
    "C": {
        "id": "C",
        "name": "趋势牛股",
        "signal_type": "趋势牛股",
        "initial_capital": 50_000,
        "max_positions": 3,
        "position_ratio": 0.33,
        "stop_loss_pct": -0.10,      # v4: hold5+stop-10% PF=1.09(旧-6%时PF=0.94)
        "max_hold_days": 5,          # v4: 高量比短期事件, hold5 PF=1.09(旧20天PF=0.94)
    },
}


def get_strategy_by_signal(signal_type: str) -> str:
    """通过信号类型字符串反查策略ID

    优先精确匹配, 回退到包含匹配。

    >>> get_strategy_by_signal("趋势牛股(策略C)")
    'C'
    >>> get_strategy_by_signal("首阴日内")
    'A'
    >>> get_strategy_by_signal("未知信号")
    ''
    """
    if not signal_type:
        return ""

    # 精确匹配
    if signal_type in SIGNAL_MAP:
        return SIGNAL_MAP[signal_type]

    # 包含匹配(信号类型可能带后缀, 如"趋势牛股(策略C)")
    for key, strategy_id in SIGNAL_MAP.items():
        if key in signal_type or signal_type in key:
            return strategy_id

    return ""


def get_strategy_name(strategy_id: str) -> str:
    """获取策略中文名"""
    return STRATEGIES.get(strategy_id, {}).get("name", "未知策略")
