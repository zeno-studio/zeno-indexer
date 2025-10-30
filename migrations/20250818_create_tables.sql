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


CREATE INDEX IF NOT EXISTS idx_metadata_symbol ON metadata(symbol);
CREATE INDEX IF NOT EXISTS idx_metadata_token_type ON metadata(token_type);


CREATE TABLE IF NOT EXISTS tokenmap (
    id SERIAL PRIMARY KEY,
    tokenid TEXT NOT NULL,
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
    tokenid TEXT NOT NULL,
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


CREATE INDEX IF NOT EXISTS idx_marketdata_symbol ON marketdata(symbol);
CREATE INDEX IF NOT EXISTS idx_marketdata_tokenid ON marketdata(tokenid);

CREATE TABLE IF NOT EXISTS forex_rates (
    created_at TIMESTAMP DEFAULT NOW(),
    data JSONB NOT NULL
);

