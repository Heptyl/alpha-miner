"""trading_daemon 常量配置 — 独立模块, 供 daemon 本体和外部页面/测试共用

所有纯常量定义从此模块导出; 运行时可变状态仍留在 trading_daemon.py。
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# ---------------------------------------------------------------------------
# 路径
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"
PRED_PATH = PROJECT_ROOT / "output" / "ml" / "latest_prediction.json"
LOG_DIR = PROJECT_ROOT / "output" / "trader" / "daemon_logs"

# ---------------------------------------------------------------------------
# 运行模式
# ---------------------------------------------------------------------------
# paper: 模拟盘采样优先。风控只防脏数据和明显异常, 不因正常A股波动停机。
# live_conservative/live_normal: 实盘模式, 可逐步启用更强阻断式风控。
RISK_MODE = "paper"

# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# 策略参数 — A:3万/C:5万/B:1万, 总9万/7只
# ---------------------------------------------------------------------------
CURRENT_PERIOD = 3

# === 策略A: 龙头首阴反包 (3万/3只) ===
A_INITIAL_CAPITAL = 30_000.0
A_MAX_POSITIONS = 3
A_POSITION_RATIO = 0.33           # 每只33%≈1万

# === 策略B: 暴跌日好公司狙击 (1万/1只) ===
# v2: 观察期减半, PF=1.29统计不显著, 待实盘验证后恢复
B_INITIAL_CAPITAL = 10_000.0
B_MAX_POSITIONS = 1
B_POSITION_RATIO = 1.0
B_STOP_LOSS_PCT = -0.06           # 止损-6%

# === 策略C: 趋势牛股 (5万/3只) ===
C_INITIAL_CAPITAL = 50_000.0
C_MAX_POSITIONS = 3
C_POSITION_RATIO = 0.33
C_STOP_LOSS_PCT = -0.08           # 止损-8%(ATR动态止损基线)

# === 兼容旧代码 ===
INITIAL_CAPITAL = 90_000.0        # A:3万 + B:1万 + C:5万
MAX_POSITIONS = A_MAX_POSITIONS + B_MAX_POSITIONS + C_MAX_POSITIONS  # 7只
MAX_AB_POSITIONS = B_MAX_POSITIONS   # 兼容
AB_POSITION_RATIO = B_POSITION_RATIO # 兼容
C_POSITION_RATIO_OLD = 0.10       # 旧参数,不再使用
MIN_CASH_RATIO = 0.10             # 最低保留10%现金


# === 极端退潮冷却期 ===
# 极端退潮清仓后, 至少等30分钟才能重新买入
# 5-20教训: 09:32极端退潮清仓6只, 09:33就买入泰和新材 — 清仓和买入没联动
EBB_COOLDOWN_MINUTES = 30
MAX_SINGLE_RATIO = 0.25           # 高价股超配上限
STOP_LOSS_PCT = B_STOP_LOSS_PCT  # 全局止损跟随B
COMMISSION_RATE = 0.00025         # 手续费万2.5
STAMP_DUTY_RATE = 0.0005          # 印花税万5(2023.8.28降为0.05%,卖出收取)

# 止损延迟(grace period) — 4452笔回测: 净效果+0.52%/笔
# 开盘30分钟(09:30-10:00)止损只标记不执行, 等波动消化后再判断
# 硬止损(-15%)即使在grace period也执行(重大利空)
GRACE_PERIOD_ENABLED = True
GRACE_PERIOD_MINUTES = 30         # 开盘后30分钟
STRATEGY_HARD_STOP_PCT = -0.08    # 策略级硬止损-8%: 技术过滤也保不住,必须止损(策略A/B卖出函数用)
HARD_STOP_PCT = -0.10             # Grace Period硬止损-10%: 开盘30分钟内也强制执行(比策略硬止损更宽松,因为grace period本身就是延迟保护)

# 集中度风控 — 同行业最多N只, 防止单一板块黑天鹅
MAX_SAME_INDUSTRY = 2              # 同行业最多2只
SLIPPAGE = 0.001                  # 滑点0.1%
MIN_COMMISSION = 5.0              # 最低手续费
DAILY_LOSS_LIMIT = -1800.0        # 日限亏1800元(9万的2%)
MIN_ML_SCORE = 0.005              # ML最低门槛(放宽,让基本面过滤来筛)

# 模拟盘风控阈值 — 用于日志/监控, 默认不阻断交易系统继续采样
PAPER_DAILY_LOSS_LIMIT_PCT = -0.03     # 模拟盘日亏3%才视为严重异常
PAPER_WEEKLY_LOSS_LIMIT_PCT = -0.08    # 模拟盘周亏8%才视为严重异常
PAPER_CONSECUTIVE_LOSSES = 5           # 模拟盘连亏5笔才提示降频

# === 策略C: 趋势牛股策略(5万独立资金) ===
STRATEGY_C_CONFIG = {
    'enabled': True,   # v3: AI赛道+基本面+技术择时
    'position_size': 16700,       # 每只~1.67万(5万/3只)
    'max_positions': 3,           # 最多3只
    'stop_loss_pct': -0.10,       # 止损-10%(回测验证: hold5+stop-10% PF=1.09 vs stop-6% PF=0.94)
    'sell_at_close': False,       # 不强制尾盘清仓
    'max_hold_days': 5,           # 最长持仓5天(高量比是短期事件, hold5 PF=1.09 vs hold20 PF=0.94)
    'mode': 'fundamental',       # 基本面驱动模式
    # v3 T+1回测(次日开盘价买入): PF=3.85, 胜率67.6%, 收益+14.15%, 回撤1.99%
    'min_score': 55,              # 基本面评分最低55分
    'target_profit': 0.10,        # 目标收益+10%(T+1回测: 10% PF=3.85 vs 12% PF=2.13)
    'vol_ratio_min': 3.0,         # 量比>=3
    'ai_only': True,              # AI赛道限定
}

# === 策略B追高过滤 (2026-05-15 P0-1修订) ===
# === 策略B低吸阈值 (2026-05-17 改版: 追涨→低吸) ===
# 回测依据: 218天/10437条涨停股次日数据
# 涨停股次日高开追入(>2%): 均亏-2.52%, 胜率27.3% → 废弃
# 涨停股次日低开买入(<-2%): 均赚+1.06%, 胜率55.1%, PF=1.86, 夏普3.78
# 外部验证: 雪球57%/1.2%, 学术1-2%, 三专家审核通过
# 龙虎榜过滤: LHB净卖出胜率51.7%(排除), LHB净买入胜率68.1%(优选)
OPEN_CHG_FILTER = {
    '退潮': 2.0,    # 退潮市场高开>2%即过滤 (回测: <2%总亏¥-25,284)
    '冰点': 3.0,    # 冰点市场高开>3%过滤
    '正常': 2.0,    # 正常市场高开>2%过滤 (回测: <2%最优)
    # 注: 情绪体系v3(涨跌比为主)不产生"高潮"阶段, get_market_driven_buy_params保留处理但不会触发
}

# === 策略B: 暴跌日好公司狙击 v2 ===
# 2026-05-30: 涨停回踩PF=1.06已废弃, 改为暴跌日反弹+基本面过滤
# T+1回测(4年156笔): PF=1.29, 胜率51.3%, 均收益+0.93%
# ROE分层实证: ROE>10%的PF=2.39 vs 无过滤PF=1.82
B_ENABLED = True

# 策略B v2参数
STRATEGY_B_CONFIG = {
    'enabled': True,
    'stop_loss_pct': -0.06,      # 止损-6%
    'sell_at_close': False,
    'max_hold_days': 7,          # 持7天
    'trailing_pct': 0.05,        # trailing 5%
    'crash_market_threshold': -0.02,  # 暴跌日: 全市场均跌>2%
    'crash_stock_drop': -0.05,        # 个股跌>5%
    'roe_min': 10,                    # ROE>10%
    'max_positions': 1,               # 观察期减半: 最多1只
}

# === 策略差异化卖出参数 (2026-05-17 回测优化) ===
# 回测依据: 218天/758笔全量回测 + 参数敏感性分析(6维扫描)
# 2026-05-17回测发现:
#   策略B旧版(涨停追涨): 520笔巨亏-40923元(笔均-1.67%, 夏普-4.61) — 已废弃
#   → 根因: trailing 5%太宽(52%的"止盈"实际亏损), 止损-8%太宽
#   → 参数扫描: trailing 3%+止损-5%+高开<2% 最优(少亏60%)
#   策略C(趋势牛股): 均线多头+量能+MACD金叉
#   → 三段交叉验证: 2/3时间段盈利, 中段(10-1月)微利
#
# 优化决策(全部基于数据):
#   B止损: -8% → -5% (总亏¥-28,785 vs ¥-29,480, 减少单笔亏损)
#   B trailing: 5% → 3% (总亏¥-26,239 vs ¥-29,480, 减少"假止盈")
#   B退潮trailing: 3% → 2% (同步收紧)
#   B冰点trailing: 2% → 1.5% (同步收紧)

# === ATR动态止损参数 ===
# 外部验证: ATR×N优于固定百分比(Freqtrade/学术/量化社区一致推荐)
# 原理: 高波动股自动放宽止损, 低波动股收紧, 减少噪声止损
# 安全设计: clamp到[FLOOR, CAP]范围, ATR为None时fallback到固定%
# === ATR动态止损开关 ===
# 回测验证: 日K线下ATR不如固定%(A:PF1.16 vs 1.22, C:更差)
# ATR优势是盘中动态, 日K回测无法体现. 分时数据接入后开启
USE_ATR_STOP = False
ATR_STOP_FLOOR_PCT = -0.02          # ATR止损最紧-2%(不低于此值,防过紧)
ATR_STOP_CAP_PCT = -0.15            # ATR止损最宽-15%(不超过此值,防过宽)

SELL_PARAMS = {
    "A": {
        # 龙头首阴反包: 7来源调研共识 — 次日确认买入, 持2-3天
        # 止损=跌破首阴最低价(非固定百分比), 反包后可持2-3天
        "is_intraday": False,          # 非日内: 次日确认买入, 持2-3天
        "stop_loss_pct": -0.05,        # 兜底止损-5%(ATR为None时fallback)
        "atr_multiplier": 2.5,         # ATR×2.5 (涨停股高波动, 日均4%×2.5=10%)
        "use_yin_low_stop": True,      # 止损用首阴最低价(从candidate的_stop_loss取)
        "confirm_high_open_pct": 0.02, # 次日确认: 高开>=2%
        "trailing_stop_pct": 0.03,     # trailing 3%(回测验证: 精选候选紧trail更优, PF=1.22 vs 5%时1.07)
        "trailing_ebb_pct": 0.02,
        "trailing_frost_pct": 0.015,
        "time_stop_days": 3,           # 3天不涨就卖
        "time_stop_threshold": 0.01,
        "max_hold_days": 3,            # 最长持3天
    },
    "B": {
        # v2: 暴跌日好公司狙击, T+1回测PF=1.29/胜率51.3%
        "is_intraday": False,
        "stop_loss_pct": -0.06,        # 兜底止损-6%(ATR为None时fallback)
        "atr_multiplier": 2.0,         # ATR×2.0 (标准2σ, 暴跌日反弹)
        "trailing_stop_pct": 0.05,     # trailing 5%
        "trailing_ebb_pct": 0.03,      # 退潮收紧到3%
        "trailing_frost_pct": 0.02,    # 冰点收紧到2%
        "time_stop_days": 5,           # 5天不涨就评估
        "time_stop_threshold": 0.02,
        "max_hold_days": 7,            # 最长持7天
    },
    "C": {
        # v3 T+1回测验证(次日开盘价): PF=3.85, 胜率67.6%, 回撤1.99%
        "is_intraday": False,
        "stop_loss_pct": -0.10,        # 兜底止损-10%(ATR为None时fallback)
        "atr_multiplier": 3.0,         # ATR×3.0 (趋势股持股期长, 需更宽止损)
        "trailing_stop_pct": 0.10,     # trailing 10%(hold5模式放宽)
        "trailing_ebb_pct": 0.06,      # 退潮收紧到6%
        "trailing_frost_pct": 0.03,    # 冰点收紧到3%
        "time_stop_days": 4,           # 4天不涨评估(hold5, 留1天余量)
        "time_stop_threshold": 0.03,   # 4天涨幅<3%就评估
        "max_hold_days": 5,            # 最长持5天(高量比短期事件, hold5 PF=1.09 vs hold20 PF=0.94)
    },
}

# 守护进程
POLL_INTERVAL = 60                # 盘后轮询间隔(秒)
POLL_INTERVAL_TRADING = 15        # 盘中轮询间隔(秒) — 实时回踩检测需要快速扫描
MARKET_OPEN_AM = (9, 30)          # 上午开盘
MARKET_CLOSE_AM = (11, 30)        # 上午收盘
MARKET_OPEN_PM = (13, 0)          # 下午开盘
MARKET_CLOSE_PM = (15, 0)         # 下午收盘

# 操作预告配置
SIGNAL_DIR = PROJECT_ROOT / "output" / "trader" / "signals"
SIGNAL_PENDING = SIGNAL_DIR / "pending_signals.json"  # 待执行预告
SIGNAL_DELAY_SEC = 120            # 预告延迟2分钟执行
SIGNAL_URGENT_DELAY_SEC = 60      # 紧急信号(止损/最长持有)仅1分钟
SIGNAL_NOTIFY_SCRIPT = PROJECT_ROOT / "scripts" / "notify_trade.py"  # 成交通知脚本

# 买入信号参数
BREAKOUT_MIN_CHG = 2.0            # 突破买点: 最小涨幅%
BREAKOUT_MAX_CHG = 8.0            # 突破买点: 最大涨幅%(涨停前都可以)
BREAKOUT_VOL_RATIO = 1.5          # 突破买点: 量比阈值(盘中缩放后)
PULLBACK_MA_DIST = 0.02           # 回踩买点: 距均线距离<2%
PULLBACK_VOL_RATIO = 0.8          # 回踩买点: 缩量(量比<0.8)
OVERSOLD_MIN_DROP = -5.0          # 超跌买点: 最大跌幅
OVERSOLD_MAX_DROP = -3.0          # 超跌买点: 最小跌幅
OVERSOLD_RSI = 30                 # 超跌买点: RSI阈值

# === Webhook通知(钉钉/飞书) — 可选, 不配就不发 ===
DINGTALK_WEBHOOK_URL = ""          # 钉钉机器人Webhook URL(留空=不启用)
DINGTALK_KEYWORD = "alpha-miner"   # 钉钉自定义关键词(安全设置用)
FEISHU_WEBHOOK_URL = ""            # 飞书机器人Webhook URL(留空=不启用)

# === 辩论式信号融合(Bull/Bear/Judge) — 可选, 验证后再开启 ===
DEBATE_ENABLED = False             # 默认关闭, 等IC验证后开启
DEBATE_MIN_CONFIDENCE = 50         # confidence<50直接过滤

# === 策略研究运行状态 ===
# shadow: 只记录假设信号和后验收益, 不占用模拟盘资金
# paper: 允许进入模拟盘
# pause: 完全暂停该策略买入, 通常用于明显负期望或数据异常
STRATEGY_VERSION_MAP = {
    "A": "A_v1",
    "B": "B_crash_v2_shadow_20260610",
    "C": "C_fundamental_v3",
    "C1": "C1_attention_momentum_v1",
    "C2": "C2_panic_reversal_v1",
}

STRATEGY_RUN_MODES = {
    "A": "paper",
    "B": "shadow",
    "C": "shadow",
    "C1": "shadow",
    "C2": "shadow",
}

STRATEGY_ENTRY_RULES = {
    "A": "A_first_yin_reversal_v1",
    "B": "B_crash_good_company_v2",
    "C": "C_fundamental_timing_v3",
    "C1": "C1_volume_attention_momentum_v1",
    "C2": "C2_panic_volume_reversal_v1",
}

STRATEGY_EXIT_RULES = {
    "A": "A_sell_params_v1",
    "B": "B_sell_params_v2",
    "C": "C_sell_params_v3",
    "C1": "C1_shadow_t3_review_v1",
    "C2": "C2_shadow_t3_review_v1",
}
