use crate::config::Config;
use serde::Deserialize;
use std::collections::HashMap;
use anyhow::Result;
use sqlx::Row;
use axum::{extract::State, http::StatusCode};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::time::{sleep, sleep_until, Instant};

#[derive(Debug, Deserialize)]
pub struct CoinGeckoToken {
    pub id: String,
    pub symbol: String,
    pub name: String,
    pub platforms: HashMap<String, String>,
}

pub async fn sync_tokenmap(config: &Config) -> Result<()> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS tokenmap (
            id BIGSERIAL PRIMARY KEY,
            token_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            chainid BIGINT NOT NULL,
            address TEXT NOT NULL,
            UNIQUE(address, chainid)
        )
        "#
    )
    .execute(&config.postgres_db.pool)
    .await?;

    let chain_map: HashMap<String, i64> = {
        let rows = sqlx::query("SELECT name, chainid FROM chains")
            .fetch_all(&config.postgres_db.pool)
            .await?;
        rows.into_iter()
            .map(|row| (row.get::<String, _>(0), row.get::<i64, _>(1)))
            .collect()
    };

    let url = "https://api.coingecko.com/api/v3/coins/list";
    let tokens: Vec<CoinGeckoToken> = config
        .http_client
        .get(url)
        .header("x-cg-demo-api-key", &config.coingecko_key)
        .header("Accept", "application/json")
        .query(&[("include_platform", "true")])
        .send()
        .await?
        .json()
        .await?;

    for token in tokens {
        for (platform, address) in token.platforms {
            if let Some(&chainid) = chain_map.get(&platform) {
                if !address.is_empty() {
                    sqlx::query(
                        r#"
                        INSERT INTO tokenmap (token_id, symbol, name, chainid, address)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (address, chainid) DO NOTHING
                        "#
                    )
                    .bind(&token.id)
                    .bind(&token.symbol)
                    .bind(&token.name)
                    .bind(chainid)
                    .bind(address.to_lowercase())
                    .execute(&config.postgres_db.pool)
                    .await?;
                }
            }
        }
    }

    println!("✅ sync_tokenmap completed.");
    Ok(())
}


#[derive(Debug, Deserialize)]
struct NftItem {
    id: String,
    contract_address: String,
    name: String,
    asset_platform_id: String,
    symbol: String,
}

pub async fn sync_nftmap(config: &Config) -> Result<(), anyhow::Error> {
    let pool = &config.postgres_db.pool;
    let client = &config.http_client;
    let api_key = &config.coingecko_key;

    // 创建表
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS nftmap (
            id BIGSERIAL PRIMARY KEY,
            nftid TEXT NOT NULL,
            symbol TEXT NOT NULL,
            name TEXT NOT NULL,
            chainid BIGINT NOT NULL,
            address TEXT NOT NULL,
            UNIQUE(address, chainid)
        )
        "#
    )
    .execute(pool)
    .await?;

    // 读取chains表，建立 name -> chainid 的映射
    let chains: Vec<(String, i64)> = sqlx::query_as("SELECT name, chainid FROM chains")
        .fetch_all(pool)
        .await?;

    let chain_map: std::collections::HashMap<String, i64> =
        chains.into_iter().collect();

    let mut page = 1;
    loop {
        let url = format!(
            "https://api.coingecko.com/api/v3/nfts/list?per_page=250&page={}",
            page
        );

        let resp = client
            .get(&url)
            .header("x-cg-demo-api-key", api_key)
            .header("Accept", "application/json")
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<NftItem>>()
            .await?;

        if resp.is_empty() {
            println!("✅ sync_nftmap finished at page {}", page - 1);
            break;
        }

        for nft in resp {
            if let Some(&chainid) = chain_map.get(&nft.asset_platform_id) {
                sqlx::query(
                    r#"
                    INSERT INTO nftmap (nftid, symbol, name, chainid, address)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (address, chainid) DO NOTHING
                    "#
                )
                .bind(&nft.id)
                .bind(&nft.symbol)
                .bind(&nft.name)
                .bind(chainid)
                .bind(nft.contract_address.to_lowercase())
                .execute(pool)
                .await?;
            }
        }

        println!("📦 sync_nftmap: page {} processed", page);
        page += 1;
    }

    Ok(())
}


pub async fn fetch_token_metadata(State(config): State<Arc<Config>>) -> Result<StatusCode, StatusCode> {
      
    let chains: Vec<(String, i64)> = sqlx::query_as("SELECT name, chainid FROM chains")
        .fetch_all(&config.postgres_db.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let tokenmap: Vec<(String, String, i64, String)> = sqlx::query_as(
        "SELECT tokenid, name, chainid, address FROM tokenmap"
    )
    .fetch_all(&config.postgres_db.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for (token_id, _name, chainid, address) in tokenmap {
        if config.postgres_db.token_exists(token_id.as_str(), chainid).await.unwrap() {
        println!("Token {} on chain {} already exists, skipping", token_id, chainid);
        return Ok(StatusCode::OK);
    }
        let url = format!("https://api.coingecko.com/api/v3/coins/{token_id}");
        let resp: Value = config.http_client
            .get(&url)
            .header("x-api-key", &config.coingecko_key)
            .query(&[
                ("localization", "false"),
                ("tickers", "false"),
                ("market_data", "false"),
                ("developer_data", "false"),
                ("sparkline", "false")
            ])
            .send()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?
            .json()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;

        if let Some(platform_id) = resp.get("asset_platform_id").and_then(|v| v.as_str()) {
            if let Some((_chain_name, _chain_id)) = chains.iter().find(|(name, _)| name == platform_id) {
                let decimal = resp.pointer("/detail_platforms/ethereum/decimal_place").and_then(|v| v.as_i64()).unwrap_or(18);
                let homepage = resp.pointer("/links/homepage/0").and_then(|v| v.as_str()).unwrap_or("");
                let image = resp.pointer("/image/large").and_then(|v| v.as_str()).unwrap_or("");

                sqlx::query(
                    "INSERT INTO metadata (tokenid, name, symbol, chainid, address, decimal, homepage, image)
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                     ON CONFLICT (tokenid, chainid) DO NOTHING"
                )
                .bind(token_id.clone())
                .bind(resp.get("name").and_then(|v| v.as_str()).unwrap_or(""))
                .bind(resp.get("symbol").and_then(|v| v.as_str()).unwrap_or(""))
                .bind(chainid)
                .bind(address.clone())
                .bind(decimal)
                .bind(homepage)
                .bind(image)
                .execute(&config.postgres_db.pool)
                .await
                .ok();
            }
        }

        sleep(Duration::from_millis(300)).await; // API 频率限制
    }

    Ok(StatusCode::OK)
}

// ================== NFT Metadata ==================
pub async fn fetch_nft_metadata(State(config): State<Arc<Config>>) -> Result<StatusCode, StatusCode> {
    let chains: Vec<(String, i64)> = sqlx::query_as("SELECT name, chainid FROM chains")
        .fetch_all(&config.postgres_db.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let nftmap: Vec<(String, String, i64, String)> = sqlx::query_as(
        "SELECT nftid, name, chainid, address FROM nftmap"
    )
    .fetch_all(&config.postgres_db.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for (nft_id, _name, chainid, address) in nftmap {
        if config.postgres_db.nft_exists(nft_id.as_str(), chainid).await.unwrap() {
        println!("NFT {} on chain {} already exists, skipping", nft_id, chainid);
        return Ok(StatusCode::OK);
    }
        let url = format!("https://api.coingecko.com/api/v3/nfts/{nft_id}");
        let resp: Value = config.http_client
            .get(&url)
            .header("x-api-key", &config.coingecko_key)
            .send()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?
            .json()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;

        if let Some(platform_id) = resp.get("asset_platform_id").and_then(|v| v.as_str()) {
            if let Some((_chain_name, _chain_id)) = chains.iter().find(|(name, _)| name == platform_id) {
                let homepage = resp.pointer("/links/homepage").and_then(|v| v.as_str()).unwrap_or("");
                let image = resp.pointer("/image/small").and_then(|v| v.as_str()).unwrap_or("");

                sqlx::query(
                    "INSERT INTO nft_metadata (nftid, name, symbol, chainid, address, homepage, image)
                     VALUES ($1,$2,$3,$4,$5,$6,$7)
                     ON CONFLICT (nftid, chainid) DO NOTHING"
                )
                .bind(nft_id.clone())
                .bind(resp.get("name").and_then(|v| v.as_str()).unwrap_or(""))
                .bind(resp.get("symbol").and_then(|v| v.as_str()).unwrap_or(""))
                .bind(chainid)
                .bind(address.clone())
                .bind(homepage)
                .bind(image)
                .execute(&config.postgres_db.pool)
                .await
                .ok();
            }
        }

        sleep(Duration::from_millis(300)).await; // API 频率限制
    }

    Ok(StatusCode::OK)
}

// ================== 自动任务（24小时随机延迟） ==================
pub async fn start_metadata_auto_tasks(config: Arc<Config>) {
    tokio::spawn(async move {
        // 随机延迟启动
        let delay =  3600; // 0-3600 秒
        println!("Metadata auto task will start after {delay} seconds...");
        sleep(Duration::from_secs(delay)).await;
            if let Err(e) = fetch_token_metadata(State(config.clone())).await {
                eprintln!("Token metadata update failed: {:?}", e);
            }
            if let Err(e) = fetch_nft_metadata(State(config.clone())).await {
                eprintln!("NFT metadata update failed: {:?}", e);
            }
    });
}

