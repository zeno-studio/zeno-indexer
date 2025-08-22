use crate::config::Config;
use anyhow::Result;
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::Value;
use sqlx::{Row, FromRow};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn, error};

const MAX_RETRY: u8 = 5;



// ================== 请求重试封装 ==================
async fn get_json_with_retry<T: serde::de::DeserializeOwned>(
    config: &Config,
    url: &str,
    headers: impl Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
) -> Result<T> {
    for attempt in 1..=MAX_RETRY {
        let req = headers(config.http_client.get(url));
        match req.send().await {
            Ok(resp) => match resp.error_for_status() {
                Ok(resp_ok) => return Ok(resp_ok.json::<T>().await?),
                Err(e) => warn!("HTTP error {} on {} (attempt {}/{})", e, url, attempt, MAX_RETRY),
            },
            Err(e) => warn!("Request error {} on {} (attempt {}/{})", e, url, attempt, MAX_RETRY),
        }
        sleep(Duration::from_millis(300 * attempt as u64)).await;
    }
    Err(anyhow::anyhow!("Failed to fetch {} after {} attempts", url, MAX_RETRY))
}

// ================== TokenMap 同步 ==================
#[derive(Debug, Deserialize)]
pub struct CoinGeckoToken {
    pub id: String,
    pub symbol: String,
    pub name: String,
    pub platforms: HashMap<String, String>,
}

pub async fn sync_tokenmap(config: &Config) -> Result<()> {
    info!("Task sync_tokenmap started");

    let chain_map: HashMap<String, i64> = sqlx::query("SELECT name, chainid FROM chains")
        .fetch_all(&config.postgres_db.pool)
        .await?
        .into_iter()
        .map(|row| (row.get::<String, _>(0), row.get::<i64, _>(1)))
        .collect();

    let url = "https://api.coingecko.com/api/v3/coins/list";
    let tokens: Vec<CoinGeckoToken> = get_json_with_retry(config, url, |r| {
        r.header("x-cg-demo-api-key", &config.coingecko_key)
            .header("Accept", "application/json")
            .query(&[("include_platform", "true")])
    }).await?;

    let mut inserted = 0;
    for token in tokens {
        for (platform, address) in token.platforms {
            if let Some(&chainid) = chain_map.get(&platform) {
                if !address.is_empty() {
                    if let Err(e) = sqlx::query(
                        r#"
                        INSERT INTO tokenmap (token_id, symbol, name, chainid, address)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (address, chainid) DO NOTHING
                        "#,
                    )
                    .bind(&token.id)
                    .bind(&token.symbol)
                    .bind(&token.name)
                    .bind(chainid)
                    .bind(address.to_lowercase())
                    .execute(&config.postgres_db.pool)
                    .await
                    {
                        warn!("Failed to insert token {}:{} -> {}", token.id, address, e);
                    } else {
                        inserted += 1;
                    }
                }
            }
        }
    }

    info!("Task sync_tokenmap completed, inserted {} rows", inserted);
    Ok(())
}

// ================== NFTMap 同步 ==================
#[derive(Debug, Deserialize)]
struct NftItem {
    id: String,
    contract_address: String,
    name: String,
    asset_platform_id: String,
    symbol: String,
}

pub async fn sync_nftmap(config: &Config) -> Result<()> {
    info!("Task sync_nftmap started");
    let pool = &config.postgres_db.pool;
    let api_key = &config.coingecko_key;

    let chains: HashMap<String, i64> = sqlx::query_as::<_, (String, i64)>("SELECT name, chainid FROM chains")
        .fetch_all(pool)
        .await?
        .into_iter()
        .collect();

    let mut page = 1;
    let mut inserted = 0;

    loop {
        let url = format!("https://api.coingecko.com/api/v3/nfts/list?per_page=250&page={}", page);
        let resp: Vec<NftItem> = get_json_with_retry(config, &url, |r| {
            r.header("x-cg-demo-api-key", api_key)
                .header("Accept", "application/json")
        })
        .await?;

        if resp.is_empty() {
            info!("sync_nftmap finished at page {}", page - 1);
            break;
        }

        for nft in &resp {
            if let Some(&chainid) = chains.get(&nft.asset_platform_id) {
                if let Err(e) = sqlx::query(
                    r#"
                    INSERT INTO nftmap (nftid, symbol, name, chainid, address)
                    VALUES ($1,$2,$3,$4,$5)
                    ON CONFLICT (address, chainid) DO NOTHING
                    "#,
                )
                .bind(&nft.id)
                .bind(&nft.symbol)
                .bind(&nft.name)
                .bind(chainid)
                .bind(nft.contract_address.to_lowercase())
                .execute(pool)
                .await
                {
                    warn!("Failed to insert NFT {}:{} -> {}", nft.id, nft.contract_address, e);
                } else {
                    inserted += 1;
                }
            }
        }

        info!("sync_nftmap page {} processed", page);
        page += 1;
    }

    info!("Task sync_nftmap completed, inserted {} rows", inserted);
    Ok(())
}

// ================== Token Metadata 同步 ==================
pub async fn fetch_token_metadata(config: &Config) -> Result<StatusCode, StatusCode> {
    info!("Task fetch_token_metadata started");

    let chains: Vec<(String, i64)> = sqlx::query_as("SELECT name, chainid FROM chains")
        .fetch_all(&config.postgres_db.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let tokenmap: Vec<(String, String, i64, String)> =
        sqlx::query_as("SELECT tokenid, name, chainid, address FROM tokenmap")
            .fetch_all(&config.postgres_db.pool)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut inserted = 0;

    for (token_id, _name, chainid, address) in tokenmap {
        // Skip if contract already exists
        if config
            .postgres_db
            .contract_exists(&address, chainid)
            .await
            .unwrap_or(false)
        {
            info!("Token {} on chain {} exists, skipping", address, chainid);
            continue;
        }

        let url = format!("https://api.coingecko.com/api/v3/coins/{}", token_id);
        let resp: Value = get_json_with_retry(config, &url, |r| {
            r.header("x-api-key", &config.coingecko_key)
                .query(&[
                    ("localization", "false"),
                    ("tickers", "false"),
                    ("market_data", "false"),
                    ("developer_data", "false"),
                    ("sparkline", "false"),
                ])
        })
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

        let platforms = resp.get("platforms").and_then(|v| v.as_object());
        let detail_platforms = resp.get("detail_platforms").and_then(|v| v.as_object());
        let mut platform_matched = false;

        if let Some(platforms) = platforms {
            for (platform_id, contract_address) in platforms {
                if let Some((_, platform_chainid)) =
                    chains.iter().find(|(name, _)| name == platform_id)
                {
                    if *platform_chainid != chainid {
                        continue;
                    }

                    if contract_address
                        .as_str()
                        .map(|addr| addr.to_lowercase() == address.to_lowercase())
                        .unwrap_or(false)
                    {
                        platform_matched = true;

                        let symbol = resp.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
                        let name = resp.get("name").and_then(|v| v.as_str()).unwrap_or("");
                        let homepage = resp.pointer("/links/homepage/0").and_then(|v| v.as_str());
                        let image = resp.pointer("/image/large").and_then(|v| v.as_str());
                        let description = resp.pointer("/description/en").and_then(|v| v.as_str());
                        let notices = resp
                            .get("additional_notices")
                            .cloned()
                            .unwrap_or_else(|| serde_json::json!([]));
                        let decimals = detail_platforms
                            .and_then(|dp| dp.get(platform_id))
                            .and_then(|dp| dp.get("decimal_place"))
                            .and_then(|v| v.as_i64());

                        if symbol.is_empty() || name.is_empty() {
                            warn!("Token {} has empty symbol or name, skipping", token_id);
                            continue;
                        }

                        if let Err(e) = sqlx::query(
                            r#"
                            INSERT INTO metadata (
                                tokenid, symbol, name, chainid, address, 
                                decimals, homepage, image, description, notices, created_at
                            )
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,CURRENT_TIMESTAMP)
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
                        {
                            warn!("Failed to insert token metadata {}: {}", token_id, e);
                        } else {
                            inserted += 1;
                        }

                        break;
                    }
                }
            }
        }

        if !platform_matched {
            warn!("Token {} on chain {} has no matching platform/address", token_id, chainid);
        }

        sleep(Duration::from_millis(300)).await;
    }

    info!("Task fetch_token_metadata completed, inserted {} rows", inserted);
    Ok(StatusCode::OK)
}

// ================== NFT Metadata 同步 ==================
pub async fn fetch_nft_metadata(config: &Config) -> Result<StatusCode, StatusCode> {
    info!("Task fetch_nft_metadata started");

    let chains: Vec<(String, i64)> = sqlx::query_as("SELECT name, chainid FROM chains")
        .fetch_all(&config.postgres_db.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let nftmap: Vec<(String, String, i64, String)> =
        sqlx::query_as("SELECT nftid, name, chainid, address FROM nftmap")
            .fetch_all(&config.postgres_db.pool)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut inserted = 0;

    for (nft_id, _name, chainid, address) in nftmap {
        if config
            .postgres_db
            .contract_exists(&address, chainid)
            .await
            .unwrap_or(false)
        {
            info!("NFT {} on chain {} exists, skipping", address, chainid);
            continue;
        }

        let url = format!("https://api.coingecko.com/api/v3/nfts/{}", nft_id);
        let resp: Value = get_json_with_retry(config, &url, |r| {
            r.header("x-api-key", &config.coingecko_key)
        })
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

        if let Some(platform_id) = resp.get("asset_platform_id").and_then(|v| v.as_str()) {
            if let Some((_, platform_chainid)) = chains.iter().find(|(name, _)| name == platform_id)
            {
                if *platform_chainid != chainid {
                    warn!("NFT {} on chain {} has mismatched chainid {}, skipping", nft_id, chainid, platform_chainid);
                    continue;
                }

                if let Some(contract_address) = resp.get("contract_address").and_then(|v| v.as_str()) {
                    if contract_address.to_lowercase() != address.to_lowercase() {
                        warn!("NFT {} on chain {} has mismatched address {}, skipping", nft_id, chainid, contract_address);
                        continue;
                    }

                    let symbol = resp.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
                    let name = resp.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    let homepage = resp.pointer("/links/homepage").and_then(|v| v.as_str());
                    let image = resp.pointer("/image/small").and_then(|v| v.as_str());
                    let description = resp.pointer("/description").and_then(|v| v.as_str());

                    if symbol.is_empty() || name.is_empty() {
                        warn!("NFT {} (address: {}) has empty symbol or name, skipping", nft_id, address);
                        continue;
                    }

                    if let Err(e) = sqlx::query(
                        r#"
                        INSERT INTO metadata (
                            nftid, symbol, name, chainid, address, 
                            homepage, image, description, created_at
                        )
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CURRENT_TIMESTAMP)
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
                    {
                        warn!("Failed to insert NFT metadata {}: {}", nft_id, e);
                    } else {
                        inserted += 1;
                    }
                }
            }
        }

        sleep(Duration::from_millis(300)).await;
    }

    info!("Task fetch_nft_metadata completed, inserted {} rows", inserted);
    Ok(StatusCode::OK)
}


#[derive(Debug, FromRow)]
struct MetadataPartial {
    id: i64,
    chainid: i64,
    address: String,
    token_type: Option<String>,
    is_verified: Option<bool>,
    risk_level: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BlockscoutResponse {
    is_contract: bool,
    is_verified: bool,
    is_scam: bool,
    token: Option<TokenInfo>,
}

#[derive(Debug, Deserialize)]
struct TokenInfo {
    #[serde(rename = "type")]
    token_type: Option<String>,
}

pub async fn update_metadata_from_blockscout(config: &Config) -> Result<()> {
    let pool= &config.postgres_db.pool;
    let client= &config.http_client;

    let rows: Vec<MetadataPartial> = sqlx::query_as::<_, MetadataPartial>(
        r#"
        SELECT id, chainid, address, token_type, is_verified, risk_level
        FROM metadata        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut updated_count = 0;
    let mut fail_count_by_chain: HashMap<i64, usize> = HashMap::new();

    for row in rows {
        if row.token_type.is_some() && row.is_verified.is_some() && row.risk_level.is_some() {
            continue;
        }

        // 从 config 里查找 API endpoint
        let base_url = match config.blockscout_endpoints.get(&row.chainid) {
            Some(url) => url,
            None => continue, // 没配置的链跳过
        };

        let api_url = format!("{}/{}", base_url, row.address);

        let resp = client
            .get(&api_url)
            .header("accept", "application/json")
            .send()
            .await;

        let resp = match resp {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                warn!(chainid=row.chainid, status=?r.status(), "API 请求失败");
                *fail_count_by_chain.entry(row.chainid).or_default() += 1;
                continue;
            }
            Err(e) => {
                warn!(chainid=row.chainid, error=?e, "HTTP 请求异常");
                *fail_count_by_chain.entry(row.chainid).or_default() += 1;
                continue;
            }
        };

        let data: BlockscoutResponse = match resp.json().await {
            Ok(d) => d,
            Err(e) => {
                warn!(chainid=row.chainid, error=?e, "JSON 解析失败");
                *fail_count_by_chain.entry(row.chainid).or_default() += 1;
                continue;
            }
        };

        if !data.is_contract {
            continue;
        }

        let risk_level = if data.is_scam {
            Some("scam".to_string())
        } else {
            None
        };

        let token_type = data.token.and_then(|t| t.token_type);

        let result = sqlx::query(
            r#"
            UPDATE metadata
            SET token_type = COALESCE($1, token_type),
                is_verified = COALESCE($2, is_verified),
                risk_level = COALESCE($3, risk_level),
                updated_at = NOW()
            WHERE id = $4
            "#,
        )
        .bind(token_type)
        .bind(Some(data.is_verified))
        .bind(risk_level)
        .bind(row.id)
        .execute(pool)
        .await;

        match result {
            Ok(_) => {
                updated_count += 1;
            }
            Err(e) => {
                error!(chainid=row.chainid, error=?e, "数据库更新失败");
                *fail_count_by_chain.entry(row.chainid).or_default() += 1;
            }
        }
    }

    info!(updated = updated_count, "本次更新完成");

    for (chainid, fails) in fail_count_by_chain {
        warn!(chainid, fails, "API 或更新失败统计");
    }

    Ok(())
}