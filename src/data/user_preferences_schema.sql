PRAGMA foreign_keys = ON;
PRAGMA application_id = 1095585874;

CREATE TABLE IF NOT EXISTS watchlist (
    stock_code TEXT PRIMARY KEY CHECK(length(stock_code) = 6 AND stock_code NOT GLOB '*[^0-9]*'),
    added_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS watchlist_capture_status (
    stock_code TEXT PRIMARY KEY REFERENCES watchlist(stock_code) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK(status IN ('SUCCESS', 'ERROR', 'CONFLICT')),
    attempts INTEGER NOT NULL DEFAULT 0,
    bars_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT NOT NULL,
    last_error TEXT
);
