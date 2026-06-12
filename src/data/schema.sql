-- Alpha Miner 数据库 Schema
-- 所有表都有 snapshot_time 列用于时间隔离

CREATE TABLE IF NOT EXISTS daily_price (
    stock_code   TEXT NOT NULL,
    trade_date   TEXT NOT NULL,
    open         REAL,
    high         REAL,
    low          REAL,
    close        REAL,
    pre_close    REAL,
    volume       REAL,
    amount       REAL,
    turnover_rate REAL,
    snapshot_time TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (stock_code, trade_date, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_daily_price_date ON daily_price(trade_date);
CREATE INDEX IF NOT EXISTS idx_daily_price_code ON daily_price(stock_code);
CREATE INDEX IF NOT EXISTS idx_dp_date_code ON daily_price(trade_date, stock_code);

CREATE TABLE IF NOT EXISTS zt_pool (
    stock_code      TEXT NOT NULL,
    trade_date      TEXT NOT NULL,
    name            TEXT DEFAULT '',
    consecutive_zt  INTEGER DEFAULT 1,
    amount          REAL,
    industry        TEXT DEFAULT '',
    circulation_mv  REAL,
    open_count      INTEGER DEFAULT 0,
    zt_stats        TEXT,
    snapshot_time   TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (stock_code, trade_date, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_zt_pool_date ON zt_pool(trade_date);

CREATE TABLE IF NOT EXISTS zb_pool (
    stock_code    TEXT NOT NULL,
    trade_date    TEXT NOT NULL,
    amount        REAL,
    open_count    INTEGER DEFAULT 0,
    snapshot_time TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (stock_code, trade_date, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_zb_pool_date ON zb_pool(trade_date);

CREATE TABLE IF NOT EXISTS strong_pool (
    stock_code    TEXT NOT NULL,
    trade_date    TEXT NOT NULL,
    name          TEXT DEFAULT '',
    amount        REAL,
    reason        TEXT,
    industry      TEXT DEFAULT '',
    snapshot_time TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (stock_code, trade_date, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_strong_pool_date ON strong_pool(trade_date);

CREATE TABLE IF NOT EXISTS lhb_detail (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code    TEXT NOT NULL,
    trade_date    TEXT NOT NULL,
    buy_amount    REAL,
    sell_amount   REAL,
    net_amount    REAL,
    buy_depart    TEXT,
    sell_depart   TEXT,
    reason        TEXT,
    snapshot_time TEXT DEFAULT (datetime('now')),
    UNIQUE(stock_code, trade_date, buy_depart, sell_depart)
);

CREATE INDEX IF NOT EXISTS idx_lhb_detail_date ON lhb_detail(trade_date);

CREATE TABLE IF NOT EXISTS fund_flow (
    stock_code       TEXT NOT NULL,
    trade_date       TEXT NOT NULL,
    super_large_net  REAL,
    large_net        REAL,
    medium_net       REAL,
    small_net        REAL,
    main_net         REAL,
    snapshot_time    TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (stock_code, trade_date, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_fund_flow_date ON fund_flow(trade_date);
CREATE INDEX IF NOT EXISTS idx_fund_flow_code ON fund_flow(stock_code);

CREATE TABLE IF NOT EXISTS concept_mapping (
    stock_code    TEXT NOT NULL,
    concept_name  TEXT NOT NULL,
    snapshot_time TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (stock_code, concept_name, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_concept_mapping_code ON concept_mapping(stock_code);
CREATE INDEX IF NOT EXISTS idx_concept_mapping_concept ON concept_mapping(concept_name);

CREATE TABLE IF NOT EXISTS concept_daily (
    concept_name      TEXT NOT NULL,
    trade_date        TEXT NOT NULL,
    zt_count          INTEGER DEFAULT 0,
    leader_code       TEXT,
    leader_consecutive INTEGER DEFAULT 0,
    snapshot_time     TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (concept_name, trade_date, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_concept_daily_date ON concept_daily(trade_date);

CREATE TABLE IF NOT EXISTS news (
    news_id          TEXT NOT NULL,
    stock_code       TEXT,
    title            TEXT,
    publish_time     TEXT,
    content          TEXT,
    sentiment_score  REAL,
    snapshot_time    TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (news_id, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_news_code ON news(stock_code);
CREATE INDEX IF NOT EXISTS idx_news_time ON news(publish_time);

CREATE TABLE IF NOT EXISTS market_emotion (
    trade_date     TEXT NOT NULL,
    zt_count       INTEGER DEFAULT 0,
    dt_count       INTEGER DEFAULT 0,
    highest_board  INTEGER DEFAULT 0,
    sentiment_level TEXT,
    snapshot_time  TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (trade_date, snapshot_time)
);

CREATE TABLE IF NOT EXISTS factor_values (
    factor_name    TEXT NOT NULL,
    stock_code     TEXT,
    trade_date     TEXT NOT NULL,
    factor_value   REAL,
    snapshot_time  TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (factor_name, stock_code, trade_date, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_factor_values_name ON factor_values(factor_name);
CREATE INDEX IF NOT EXISTS idx_factor_values_date ON factor_values(trade_date);
CREATE INDEX IF NOT EXISTS idx_fv_name_date ON factor_values(factor_name, trade_date);
CREATE INDEX IF NOT EXISTS idx_fv_name_date_code ON factor_values(factor_name, trade_date, stock_code);

CREATE TABLE IF NOT EXISTS ic_series (
    factor_name    TEXT NOT NULL,
    trade_date     TEXT NOT NULL,
    ic_value       REAL,
    forward_days   INTEGER DEFAULT 1,
    snapshot_time  TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (factor_name, trade_date, forward_days, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_ic_series_name ON ic_series(factor_name);
CREATE INDEX IF NOT EXISTS idx_ic_name_fwd ON ic_series(factor_name, forward_days);

CREATE TABLE IF NOT EXISTS drift_events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    factor_name    TEXT NOT NULL,
    event_date     TEXT NOT NULL,
    event_type     TEXT,
    description    TEXT,
    snapshot_time  TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_drift_events_name ON drift_events(factor_name);

CREATE TABLE IF NOT EXISTS regime_state (
    trade_date     TEXT NOT NULL,
    regime_type    TEXT NOT NULL,
    confidence     REAL DEFAULT 0.0,
    snapshot_time  TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (trade_date, snapshot_time)
);

CREATE TABLE IF NOT EXISTS mining_log (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    factor_name    TEXT,
    generation     INTEGER,
    parent         TEXT,
    mutation_type  TEXT,
    theory_source  TEXT,
    hypothesis     TEXT,
    ic_mean        REAL,
    icir           REAL,
    win_rate       REAL,
    pnl_ratio      REAL,
    accepted       INTEGER DEFAULT 0,
    failure_mode   TEXT,
    code           TEXT,
    snapshot_time  TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_mining_log_name ON mining_log(factor_name);

CREATE TABLE IF NOT EXISTS market_scripts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    script_title TEXT,
    script_narrative TEXT,
    theme_verdicts TEXT,
    tomorrow_playbook TEXT,
    risk_alerts TEXT,
    raw_snapshot TEXT,
    replay_result TEXT,
    snapshot_time TEXT DEFAULT (datetime('now')),
    UNIQUE(trade_date)
);

CREATE INDEX IF NOT EXISTS idx_market_scripts_date ON market_scripts(trade_date);

CREATE TABLE IF NOT EXISTS replay_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date       TEXT UNIQUE NOT NULL,
    regime_match     INTEGER DEFAULT 0,
    playbook_hits    TEXT DEFAULT '[]',
    playbook_misses  TEXT DEFAULT '[]',
    surprise_events  TEXT DEFAULT '[]',
    narrative        TEXT DEFAULT '',
    lessons          TEXT DEFAULT '[]',
    adjustment_suggestions TEXT DEFAULT '[]',
    snapshot_time    TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_replay_log_date ON replay_log(trade_date);

-- ═══════════════════════════════════════════════════════════
-- 策略系统表
-- ═══════════════════════════════════════════════════════════

-- 策略定义（持久化 Strategy 对象）
CREATE TABLE IF NOT EXISTS strategy_defs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT UNIQUE NOT NULL,
    description    TEXT DEFAULT '',
    yaml_body      TEXT NOT NULL,          -- 完整 YAML 序列化
    parent         TEXT,                   -- 进化来源
    version        INTEGER DEFAULT 1,
    source         TEXT DEFAULT 'manual',  -- manual / evolver / knowledge_base
    tags           TEXT DEFAULT '[]',      -- JSON array
    created_at     TEXT DEFAULT (datetime('now')),
    snapshot_time  TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_strategy_defs_name ON strategy_defs(name);

-- 回测报告
CREATE TABLE IF NOT EXISTS strategy_reports (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_name    TEXT NOT NULL,
    backtest_start   TEXT NOT NULL,
    backtest_end     TEXT NOT NULL,
    total_trades     INTEGER DEFAULT 0,
    win_rate         REAL DEFAULT 0,
    total_return_pct REAL DEFAULT 0,
    sharpe_ratio     REAL DEFAULT 0,
    max_drawdown_pct REAL DEFAULT 0,
    profit_loss_ratio REAL DEFAULT 0,
    report_yaml      TEXT,                 -- 完整报告 YAML
    snapshot_time    TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (strategy_name) REFERENCES strategy_defs(name)
);

CREATE INDEX IF NOT EXISTS idx_strategy_reports_name ON strategy_reports(strategy_name);

-- 交易记录
CREATE TABLE IF NOT EXISTS strategy_trades (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_name  TEXT NOT NULL,
    stock_code     TEXT NOT NULL,
    entry_date     TEXT NOT NULL,
    entry_price    REAL,
    exit_date      TEXT,
    exit_price     REAL,
    return_pct     REAL,
    hold_days      INTEGER DEFAULT 0,
    exit_reason    TEXT,
    regime_at_entry TEXT DEFAULT '',
    snapshot_time  TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (strategy_name) REFERENCES strategy_defs(name)
);

CREATE INDEX IF NOT EXISTS idx_strategy_trades_name ON strategy_trades(strategy_name);
CREATE INDEX IF NOT EXISTS idx_strategy_trades_date ON strategy_trades(entry_date);

-- 现金流量表
CREATE TABLE IF NOT EXISTS cash_flow_stmt (
    stock_code       TEXT NOT NULL,
    report_date      TEXT NOT NULL,
    operate_cash_flow REAL,
    invest_cash_flow  REAL,
    finance_cash_flow REAL,
    free_cash_flow    REAL,
    cash_change       REAL,
    PRIMARY KEY (stock_code, report_date)
);

-- 资产负债表
CREATE TABLE IF NOT EXISTS balance_sheet (
    stock_code         TEXT NOT NULL,
    report_date        TEXT NOT NULL,
    total_assets       REAL,
    total_liabilities  REAL,
    total_equity       REAL,
    current_assets     REAL,
    current_liabilities REAL,
    cash_and_equiv     REAL,
    accounts_receivable REAL,
    inventory          REAL,
    goodwill           REAL,
    PRIMARY KEY (stock_code, report_date)
);

-- 行业每日景气度
CREATE TABLE IF NOT EXISTS industry_daily (
    industry_code TEXT NOT NULL,
    industry_name TEXT NOT NULL,
    trade_date    TEXT NOT NULL,
    avg_pe        REAL,
    avg_pb        REAL,
    avg_roe       REAL,
    member_count  INTEGER,
    up_count      INTEGER,
    down_count    INTEGER,
    total_volume  REAL,
    total_amount  REAL,
    PRIMARY KEY (industry_code, trade_date)
);

-- 个股-行业映射
CREATE TABLE IF NOT EXISTS stock_industry_mapping (
    stock_code   TEXT PRIMARY KEY,
    industry_code TEXT NOT NULL,
    industry_name TEXT NOT NULL,
    update_date  TEXT NOT NULL
);

-- 北向资金每日净流入(亿元)
CREATE TABLE IF NOT EXISTS northbound_flow (
    trade_date   TEXT PRIMARY KEY,
    hgt_net      REAL,          -- 沪股通净流入(亿元)
    sgt_net      REAL,          -- 深股通净流入(亿元)
    total_net    REAL,          -- 北向合计净流入(亿元)
    hgt_buy      REAL,          -- 沪股通买入成交额(亿元)
    hgt_sell     REAL,          -- 沪股通卖出成交额(亿元)
    sgt_buy      REAL,          -- 深股通买入成交额(亿元)
    sgt_sell     REAL,          -- 深股通卖出成交额(亿元)
    snapshot_time TEXT DEFAULT (datetime('now'))
);

-- 解禁日历(个股级别)
CREATE TABLE IF NOT EXISTS lockup_calendar (
    stock_code       TEXT NOT NULL,
    stock_name       TEXT DEFAULT '',
    free_date        TEXT NOT NULL,    -- 解禁日期
    free_shares      REAL,            -- 解禁数量(万股)
    lift_market_cap  REAL,            -- 解禁市值(万元)
    free_ratio       REAL,            -- 占流通股比
    total_ratio      REAL,            -- 占总股本比
    free_type        TEXT DEFAULT '', -- 解禁类型(定增/首发原股东等)
    snapshot_time    TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (stock_code, free_date, free_type)
);

-- 交易记忆 — 盘后复盘归因
CREATE TABLE IF NOT EXISTS trade_memory (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id        INTEGER NOT NULL, -- 关联daemon_trades.id
    code            TEXT NOT NULL,
    name            TEXT DEFAULT '',
    strategy        TEXT NOT NULL,    -- 策略A/B/C
    action          TEXT NOT NULL,    -- buy/sell
    pnl_pct         REAL DEFAULT 0,  -- 盈亏%
    hold_days       INTEGER DEFAULT 0,
    market_phase    TEXT DEFAULT '',  -- 买入时市场情绪(正常/退潮/冰点)
    industry        TEXT DEFAULT '',  -- 行业
    entry_signal    TEXT DEFAULT '',  -- 买入信号类型
    exit_reason     TEXT DEFAULT '',  -- 卖出原因
    attribution     TEXT DEFAULT '',  -- LLM归因分析
    lessons         TEXT DEFAULT '',  -- 经验教训(一句话)
    similar_win_rate REAL DEFAULT 0, -- 历史相似交易胜率
    created_at      TEXT DEFAULT (datetime('now'))
);

-- 因子动态权重(IC/ICIR驱动, 每周更新)
CREATE TABLE IF NOT EXISTS factor_weights (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy     TEXT NOT NULL,
    factor_name  TEXT NOT NULL,
    ic_mean      REAL DEFAULT 0,
    ic_std       REAL DEFAULT 0,
    icir         REAL DEFAULT 0,
    ic_win_rate  REAL DEFAULT 0,
    weight       REAL DEFAULT 0,
    method       TEXT DEFAULT 'empirical',  -- ic_driven / empirical
    updated_at   TEXT DEFAULT (datetime('now')),
    UNIQUE(strategy, factor_name)
);

-- 龙虎榜席位明细(买入/卖出TOP5营业部)
CREATE TABLE IF NOT EXISTS lhb_seats (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code    TEXT NOT NULL,
    trade_date    TEXT NOT NULL,
    seat_name     TEXT NOT NULL,        -- 营业部名称
    direction     TEXT NOT NULL,        -- 买入/卖出
    buy_amount    REAL DEFAULT 0,       -- 买入金额
    sell_amount   REAL DEFAULT 0,       -- 卖出金额
    net_amount    REAL DEFAULT 0,       -- 净额
    seat_type     TEXT DEFAULT '',      -- 机构/游资/散户/普通
    reason        TEXT DEFAULT '',      -- 上榜原因
    snapshot_time TEXT DEFAULT (datetime('now')),
    UNIQUE(stock_code, trade_date, seat_name, direction)
);

CREATE INDEX IF NOT EXISTS idx_lhb_seats_code ON lhb_seats(stock_code);
CREATE INDEX IF NOT EXISTS idx_lhb_seats_date ON lhb_seats(trade_date);

-- LLM调用统计
CREATE TABLE IF NOT EXISTS llm_usage (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date    TEXT NOT NULL,
    caller        TEXT DEFAULT '',
    provider      TEXT DEFAULT '',
    model         TEXT DEFAULT '',
    success       INTEGER DEFAULT 0,
    latency_ms    INTEGER DEFAULT 0,
    input_tokens  INTEGER DEFAULT 0,
    output_tokens INTEGER DEFAULT 0,
    cost          REAL DEFAULT 0,
    error         TEXT DEFAULT '',
    created_at    TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_llm_usage_date ON llm_usage(trade_date);
CREATE INDEX IF NOT EXISTS idx_llm_usage_caller ON llm_usage(caller);

-- 盘后采集日志
CREATE TABLE IF NOT EXISTS collect_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date  TEXT NOT NULL,
    total       INTEGER DEFAULT 0,
    success     INTEGER DEFAULT 0,
    failed      INTEGER DEFAULT 0,
    duration_s  REAL DEFAULT 0,
    created_at  TEXT DEFAULT (datetime('now')),
    UNIQUE(trade_date)
);

-- 辩论式信号融合结果(Bull/Bear/Judge)
CREATE TABLE IF NOT EXISTS debate_results (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_code   TEXT NOT NULL,
    stock_name   TEXT DEFAULT '',
    strategy     TEXT DEFAULT '',
    verdict      TEXT DEFAULT '',       -- bull/bear/neutral
    confidence   INTEGER DEFAULT 50,   -- 0-100
    reasoning    TEXT DEFAULT '',
    key_risk     TEXT DEFAULT '',
    key_catalyst TEXT DEFAULT '',
    bull_points  TEXT DEFAULT '[]',     -- JSON array
    bear_points  TEXT DEFAULT '[]',     -- JSON array
    created_at   TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_debate_code ON debate_results(stock_code);
CREATE INDEX IF NOT EXISTS idx_debate_date ON debate_results(created_at);
