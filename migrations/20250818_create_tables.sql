CREATE TABLE IF NOT EXISTS chains (
                chainid BIGINT PRIMARY KEY,
                name TEXT NOT NULL
            );

CREATE TABLE IF NOT EXISTS metadata (
                id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                tokenid TEXT,
                nftid TEXT,
                dappid TEXT,
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
                UNIQUE(address, chainid),
                INDEX (symbol),
                INDEX (token_type)
            );

 CREATE TABLE IF NOT EXISTS tokenmap (
            id BIGSERIAL PRIMARY KEY,
            token_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            chainid BIGINT NOT NULL,
            address TEXT NOT NULL,
            UNIQUE(address, chainid)
        );

 CREATE TABLE IF NOT EXISTS nftmap (
            id BIGSERIAL PRIMARY KEY,
            nftid TEXT NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            chainid BIGINT NOT NULL,
            address TEXT NOT NULL,
            UNIQUE(address, chainid)
        )


CREATE TABLE IF NOT EXISTS hot_tokens (
    token_id TEXT NOT NULL PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    image TEXT,
    market_cap BIGINT,
    market_cap_rank BIGINT,
    total_supply BIGINT,
    max_supply BIGINT,
    ath BIGINT,
    ath_date TEXT,
    atl BIGINT,
    atl_date TEXT,
    last_updated TEXT,
    INDEX (symbol),
);