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
