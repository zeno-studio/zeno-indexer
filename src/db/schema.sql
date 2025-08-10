CREATE TABLE blocks (
    block_number BIGINT PRIMARY KEY,
    block_data JSONB NOT NULL
);

CREATE TABLE transactions (
    tx_hash TEXT PRIMARY KEY,
    tx_data JSONB NOT NULL
);

CREATE TABLE events (
    event_id TEXT PRIMARY KEY, -- 事件唯一 ID（如 log_index + tx_hash）
    event_data JSONB NOT NULL
);