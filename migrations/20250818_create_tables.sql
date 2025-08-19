CREATE TABLE IF NOT EXISTS chains (
    chainid BIGINT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS metadata (
    id SERIAL PRIMARY KEY,
    tokenid TEXT,
    nftid TEXT,
    token_type TEXT,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    decimals BIGINT,
    homepage TEXT,
    image TEXT,
    chainid BIGINT NOT NULL,
    address TEXT NOT NULL,
    is_verified BOOLEAN,
    notices JSONB,
    description TEXT,
    risk_level TEXT,
    abi JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    UNIQUE(address, chainid)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_metadata_symbol ON metadata(symbol);
CREATE INDEX IF NOT EXISTS idx_metadata_token_type ON metadata(token_type);


CREATE TABLE IF NOT EXISTS tokenmap (
    id SERIAL PRIMARY KEY,
    token_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    chainid BIGINT NOT NULL,
    address TEXT NOT NULL,
    UNIQUE(address, chainid)
);

CREATE TABLE IF NOT EXISTS nftmap (
    id SERIAL PRIMARY KEY,
    nftid TEXT NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    chainid BIGINT NOT NULL,
    address TEXT NOT NULL,
    UNIQUE(address, chainid)
);

CREATE TABLE IF NOT EXISTS marketdata (
    id SERIAL PRIMARY KEY,
    token_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    image TEXT,
    market_cap DOUBLE PRECISION,
    market_cap_rank BIGINT,
    fully_diluted_valuation DOUBLE PRECISION,
    price_change_24h DOUBLE PRECISION,
    price_change_percentage_24h DOUBLE PRECISION,
    circulating_supply DOUBLE PRECISION,
    total_supply DOUBLE PRECISION,
    max_supply DOUBLE PRECISION,
    ath DOUBLE PRECISION,
    ath_date TEXT,
    atl DOUBLE PRECISION,
    atl_date TEXT,
    last_updated TEXT
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_marketdata_symbol ON marketdata(symbol);
CREATE INDEX IF NOT EXISTS idx_marketdata_token_id ON marketdata(token_id);


CREATE TABLE IF NOT EXISTS daily_price (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_daily_price_symbol ON daily_price(symbol);


CREATE TABLE IF NOT EXISTS fifteen_minute_price (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE,
    prices JSONB NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_fifteen_price_symbol ON fifteen_minute_price(symbol);
