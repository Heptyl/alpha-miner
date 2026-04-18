-- Alpha Miner 数据库 Schema
-- 所有表都有 snapshot_time 列用于时间隔离

CREATE TABLE IF NOT EXISTS daily_price (
    stock_code   TEXT NOT NULL,
    trade_date   TEXT NOT NULL,
    open         REAL,
    high         REAL,
    low          REAL,
    close        REAL,
    volume       REAL,
    amount       REAL,
    turnover_rate REAL,
    snapshot_time TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (stock_code, trade_date, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_daily_price_date ON daily_price(trade_date);
CREATE INDEX IF NOT EXISTS idx_daily_price_code ON daily_price(stock_code);

CREATE TABLE IF NOT EXISTS zt_pool (
    stock_code      TEXT NOT NULL,
    trade_date      TEXT NOT NULL,
    consecutive_zt  INTEGER DEFAULT 1,
    amount          REAL,
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
    amount        REAL,
    reason        TEXT,
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

CREATE TABLE IF NOT EXISTS ic_series (
    factor_name    TEXT NOT NULL,
    trade_date     TEXT NOT NULL,
    ic_value       REAL,
    forward_days   INTEGER DEFAULT 1,
    snapshot_time  TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (factor_name, trade_date, forward_days, snapshot_time)
);

CREATE INDEX IF NOT EXISTS idx_ic_series_name ON ic_series(factor_name);

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
