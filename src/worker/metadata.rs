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


pub async fn fetch_token_metadata(config: &Config) -> Result<StatusCode, StatusCode> {
    // Fetch chains from the database
    let chains: Vec<(String, i64)> = sqlx::query_as("SELECT name, chainid FROM chains")
        .fetch_all(&config.postgres_db.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Fetch tokens from tokenmap
    let tokenmap: Vec<(String, String, i64, String)> = sqlx::query_as(
        "SELECT tokenid, name, chainid, address FROM tokenmap"
    )
    .fetch_all(&config.postgres_db.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for (token_id, _name, chainid, address) in tokenmap {
        // Check if token exists by address and chainid to avoid unnecessary API calls
        if config.postgres_db.contract_exists(&address, chainid).await.unwrap_or(false) {
            println!("Token at address {} on chain {} already exists, skipping", address, chainid);
            continue; // Skip to next token in the loop
        }

        // Fetch from CoinGecko
        let url = format!("https://api.coingecko.com/api/v3/coins/{}", token_id);
        let resp: Value = config.http_client
            .get(&url)
            .header("x-api-key", &config.coingecko_key)
            .query(&[
                ("localization", "false"),
                ("tickers", "false"),
                ("market_data", "false"),
                ("developer_data", "false"),
                ("sparkline", "false"),
            ])
            .send()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?
            .json()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;

        // Find matching platform in CoinGecko's platforms or detail_platforms
        let platforms = resp.get("platforms").and_then(|v| v.as_object());
        let detail_platforms = resp.get("detail_platforms").and_then(|v| v.as_object());
        let mut platform_matched = false;

        if let Some(platforms) = platforms {
            for (platform_id, contract_address) in platforms {
                // Skip if platform_id is not in chains table or chainid doesn't match
                if let Some((_, platform_chainid)) = chains.iter().find(|(name, _)| name == platform_id) {
                    if *platform_chainid != chainid {
                        continue;
                    }

                    // Check if contract address matches
                    if contract_address.as_str().map(|addr| addr.to_lowercase() == address.to_lowercase()).unwrap_or(false) {
                        platform_matched = true;

                        // Extract fields
                        let symbol = resp.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
                        let name = resp.get("name").and_then(|v| v.as_str()).unwrap_or("");
                        let homepage = resp.pointer("/links/homepage/0").and_then(|v| v.as_str());
                        let image = resp.pointer("/image/large").and_then(|v| v.as_str());
                        let description = resp.pointer("/description/en").and_then(|v| v.as_str());
                        let notices = resp.get("additional_notices").cloned().unwrap_or_else(|| serde_json::json!([]));
                        let decimals = detail_platforms
                            .and_then(|dp| dp.get(platform_id))
                            .and_then(|dp| dp.get("decimal_place"))
                            .and_then(|v| v.as_i64());

                        // Validate NOT NULL fields
                        if symbol.is_empty() || name.is_empty() {
                            println!("Token {} (address: {}) has empty symbol or name, skipping", token_id, address);
                            continue;
                        }

                        // Insert into metadata table
                        sqlx::query(
                            r#"
                            INSERT INTO metadata (
                                tokenid, symbol, name, chainid, address, 
                                decimals, homepage, image, description, notices, created_at
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP)
                            ON CONFLICT (address, chainid) DO NOTHING
                            "#,
                        )
                        .bind(&token_id)
                        .bind(symbol)
                        .bind(name)
                        .bind(chainid)
                        .bind(&address)
                        .bind(decimals)
                        .bind(homepage)
                        .bind(image)
                        .bind(description)
                        .bind(notices)
                        .execute(&config.postgres_db.pool)
                        .await
                        .map_err(|e| {
                            println!("Failed to insert token {} (address: {}): {}", token_id, address, e);
                            StatusCode::INTERNAL_SERVER_ERROR
                        })?;

                        break; // Found matching platform, no need to check others
                    }
                }
            }
        }

        if !platform_matched {
            println!("Token {} (address: {}) on chain {} has no matching platform or address, skipping", token_id, address, chainid);
        }

        sleep(Duration::from_millis(300)).await; // Respect API rate limits
    }

    Ok(StatusCode::OK)
}

// ================== NFT Metadata ==================
pub async fn fetch_nft_metadata(config: &Config) -> Result<StatusCode, StatusCode> {
    // Fetch chains from the database
    let chains: Vec<(String, i64)> = sqlx::query_as("SELECT name, chainid FROM chains")
        .fetch_all(&config.postgres_db.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Fetch NFTs from nftmap
    let nftmap: Vec<(String, String, i64, String)> = sqlx::query_as(
        "SELECT nftid, name, chainid, address FROM nftmap"
    )
    .fetch_all(&config.postgres_db.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    for (nft_id, _name, chainid, address) in nftmap {
        // Check if NFT exists by address and chainid to avoid unnecessary API calls
        if config.postgres_db.contract_exists(&address, chainid).await.unwrap_or(false) {
            println!("NFT at address {} on chain {} already exists, skipping", address, chainid);
            continue; // Skip to next NFT in the loop
        }

        // Fetch from CoinGecko
        let url = format!("https://api.coingecko.com/api/v3/nfts/{}", nft_id);
        let resp: Value = config.http_client
            .get(&url)
            .header("x-api-key", &config.coingecko_key)
            .send()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?
            .json()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;

        // Check asset_platform_id and contract_address
        if let Some(platform_id) = resp.get("asset_platform_id").and_then(|v| v.as_str()) {
            if let Some((_, platform_chainid)) = chains.iter().find(|(name, _)| name == platform_id) {
                if *platform_chainid != chainid {
                    println!("NFT {} on chain {} has mismatched chainid {} for platform {}, skipping", nft_id, chainid, platform_chainid, platform_id);
                    continue;
                }

                // Verify contract address
                if let Some(contract_address) = resp.get("contract_address").and_then(|v| v.as_str()) {
                    if contract_address.to_lowercase() != address.to_lowercase() {
                        println!("NFT {} on chain {} has mismatched address {} (expected {}), skipping", nft_id, chainid, contract_address, address);
                        continue;
                    }

                    // Extract fields
                    let symbol = resp.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
                    let name = resp.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    let homepage = resp.pointer("/links/homepage").and_then(|v| v.as_str());
                    let image = resp.pointer("/image/small").and_then(|v| v.as_str());
                    let description = resp.pointer("/description").and_then(|v| v.as_str());

                    // Validate NOT NULL fields
                    if symbol.is_empty() || name.is_empty() {
                        println!("NFT {} (address: {}) has empty symbol or name, skipping", nft_id, address);
                        continue;
                    }

                    // Insert into metadata table
                    sqlx::query(
                        r#"
                        INSERT INTO metadata (
                            nftid, symbol, name, chainid, address, 
                            homepage, image, description, created_at
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP)
                        ON CONFLICT (address, chainid) DO NOTHING
                        "#,
                    )
                    .bind(&nft_id)
                    .bind(symbol)
                    .bind(name)
                    .bind(chainid)
                    .bind(&address)
                    .bind(homepage)
                    .bind(image)
                    .bind(description)
                    .execute(&config.postgres_db.pool)
                    .await
                    .map_err(|e| {
                        println!("Failed to insert NFT {} (address: {}): {}", nft_id, address, e);
                        StatusCode::INTERNAL_SERVER_ERROR
                    })?;
                } else {
                    println!("NFT {} on chain {} has no contract_address, skipping", nft_id, chainid);
                }
            } else {
                println!("NFT {} on chain {} has platform_id {} not in chains table, skipping", nft_id, chainid, platform_id);
            }
        } else {
            println!("NFT {} has no asset_platform_id, skipping", nft_id);
        }

        sleep(Duration::from_millis(300)).await; // Respect API rate limits
    }

    Ok(StatusCode::OK)
}
