use crate::config::Config;
use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use serde_json::Value;
use sqlx::{FromRow, Row};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

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
                Ok(resp_ok) => {
                    return resp_ok
                        .json::<T>()
                        .await
                        .context(format!("JSON parse failed for URL: {}", url));
                }
                Err(e) => warn!(
                    "HTTP error {} on {} (attempt {}/{})",
                    e, url, attempt, MAX_RETRY
                ),
            },
            Err(e) => warn!(
                "Request error {} on {} (attempt {}/{})",
                e, url, attempt, MAX_RETRY
            ),
        }
        sleep(Duration::from_millis(300 * attempt as u64)).await;
    }
    Err(anyhow!("Failed to fetch {} after {} attempts", url, MAX_RETRY))
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
        .await
        .context("Failed to fetch chain map")?
        .into_iter()
        .map(|row| (row.get::<String, _>(0), row.get::<i64, _>(1)))
        .collect();

    let url = "https://api.coingecko.com/api/v3/coins/list";
    let tokens: Vec<CoinGeckoToken> = get_json_with_retry(config, url, |r| {
        r.header("x-cg-demo-api-key", &config.coingecko_key)
            .header("Accept", "application/json")
            .query(&[("include_platform", "true")])
    })
    .await
    .context("Failed to fetch token list from CoinGecko")?;

    let mut inserted = 0usize;
    for token in tokens {
        for (platform, address) in token.platforms {
            if let Some(&chainid) = chain_map.get(&platform) {
                if address.is_empty() {
                    continue;
                }
                let address_lc = address.to_lowercase();
                let res = sqlx::query(
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
                .bind(&address_lc)
                .execute(&config.postgres_db.pool)
                .await;

                match res {
                    Ok(_) => inserted += 1,
                    Err(e) => warn!("Insert failed for token {}:{} => {}", token.id, address_lc, e),
                }
            }
        }
    }

    info!("✅ sync_tokenmap completed, inserted {} rows", inserted);
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

    let chains: HashMap<String, i64> = sqlx::query_as::<_, (String, i64)>("SELECT name, chainid FROM chains")
        .fetch_all(pool)
        .await
        .context("Failed to load chains")?
        .into_iter()
        .collect();

    let mut page = 1usize;
    let mut inserted = 0usize;

    loop {
        let url = format!("https://api.coingecko.com/api/v3/nfts/list?per_page=250&page={}", page);
        let resp: Vec<NftItem> = get_json_with_retry(config, &url, |r| {
            r.header("x-cg-demo-api-key", &config.coingecko_key)
                .header("Accept", "application/json")
        })
        .await
        .with_context(|| format!("Failed to fetch NFT list at page {}", page))?;

        if resp.is_empty() {
            break;
        }

        for nft in &resp {
            if let Some(&chainid) = chains.get(&nft.asset_platform_id) {
                let addr = nft.contract_address.to_lowercase();
                let res = sqlx::query(
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
                .bind(&addr)
                .execute(pool)
                .await;

                match res {
                    Ok(_) => inserted += 1,
                    Err(e) => warn!("Insert failed for NFT {}:{} -> {}", nft.id, addr, e),
                }
            }
        }

        info!("Processed page {}", page);
        page += 1;
    }

    info!("✅ sync_nftmap completed, inserted {} rows", inserted);
    Ok(())
}

// ================== Token Metadata 同步 ==================
pub async fn fetch_token_metadata(config: &Config) -> Result<()> {
    info!("Task fetch_token_metadata started");

    let chains: Vec<(String, i64)> = sqlx::query_as("SELECT name, chainid FROM chains")
        .fetch_all(&config.postgres_db.pool)
        .await
        .context("Failed to load chains for metadata")?;

    // tokenmap: (tokenid, name, chainid, address)
    let tokenmap: Vec<(String, String, i64, String)> =
        sqlx::query_as("SELECT tokenid, name, chainid, address FROM tokenmap")
            .fetch_all(&config.postgres_db.pool)
            .await
            .context("Failed to load tokenmap for metadata")?;

    let mut inserted = 0usize;

    for (token_id, _name, chainid, address) in tokenmap {
        // 如果 contract 已存在则跳过
        if config
            .postgres_db
            .contract_exists(&address, chainid)
            .await
            .unwrap_or(false)
        {
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
        .with_context(|| format!("Failed to fetch metadata for token {}", token_id))?;

        // 尝试从响应里抽取一些字段（保持宽松防止字段缺失）
        let symbol = resp.get("symbol").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let name = resp.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();

        if symbol.is_empty() || name.is_empty() {
            warn!("Skipping token {} with empty symbol/name", token_id);
            continue;
        }

        // 可选字段
        let homepage = resp.pointer("/links/homepage/0").and_then(|v| v.as_str());
        let image = resp.pointer("/image/large").and_then(|v| v.as_str());
        let description = resp.pointer("/description/en").and_then(|v| v.as_str());
        let notices = resp
            .get("additional_notices")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([]));

        let res = sqlx::query(
            r#"
            INSERT INTO metadata (
                tokenid, symbol, name, chainid, address, decimals, homepage, image, description, notices, created_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,CURRENT_TIMESTAMP)
            ON CONFLICT (address, chainid) DO NOTHING
            "#,
        )
        .bind(&token_id)
        .bind(&symbol)
        .bind(&name)
        .bind(chainid)
        .bind(&address)
        .bind(None::<i64>) // decimals unknown here; keep NULL
        .bind(homepage)
        .bind(image)
        .bind(description)
        .bind(notices)
        .execute(&config.postgres_db.pool)
        .await;

        match res {
            Ok(_) => inserted += 1,
            Err(e) => warn!("Insert failed for token metadata {}: {}", token_id, e),
        }

        sleep(Duration::from_millis(300)).await;
    }

    info!("✅ fetch_token_metadata completed, inserted {} rows", inserted);
    Ok(())
}

// ================== NFT Metadata 同步 ==================
pub async fn fetch_nft_metadata(config: &Config) -> Result<()> {
    info!("Task fetch_nft_metadata started");

    let chains: Vec<(String, i64)> = sqlx::query_as("SELECT name, chainid FROM chains")
        .fetch_all(&config.postgres_db.pool)
        .await
        .context("Failed to load chains for NFT metadata")?;

    let nftmap: Vec<(String, String, i64, String)> =
        sqlx::query_as("SELECT nftid, name, chainid, address FROM nftmap")
            .fetch_all(&config.postgres_db.pool)
            .await
            .context("Failed to load nftmap")?;

    let mut inserted = 0usize;

    for (nft_id, _name, chainid, address) in nftmap {
        if config
            .postgres_db
            .contract_exists(&address, chainid)
            .await
            .unwrap_or(false)
        {
            continue;
        }

        let url = format!("https://api.coingecko.com/api/v3/nfts/{}", nft_id);
        let resp: Value = get_json_with_retry(config, &url, |r| {
            r.header("x-api-key", &config.coingecko_key)
        })
        .await
        .with_context(|| format!("Failed to fetch NFT metadata {}", nft_id))?;

        let symbol = resp.get("symbol").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let name = resp.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();

        if symbol.is_empty() || name.is_empty() {
            warn!("NFT {} has empty symbol/name, skipping", nft_id);
            continue;
        }

        let homepage = resp.pointer("/links/homepage/0").and_then(|v| v.as_str());
        let image = resp.pointer("/image/small").and_then(|v| v.as_str());
        let description = resp.pointer("/description").and_then(|v| v.as_str());

        let res = sqlx::query(
            r#"
            INSERT INTO metadata (
                nftid, symbol, name, chainid, address, homepage, image, description, created_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CURRENT_TIMESTAMP)
            ON CONFLICT (address, chainid) DO NOTHING
            "#,
        )
        .bind(&nft_id)
        .bind(&symbol)
        .bind(&name)
        .bind(chainid)
        .bind(&address)
        .bind(homepage)
        .bind(image)
        .bind(description)
        .execute(&config.postgres_db.pool)
        .await;

        match res {
            Ok(_) => inserted += 1,
            Err(e) => warn!("Insert failed for NFT metadata {}: {}", nft_id, e),
        }

        sleep(Duration::from_millis(300)).await;
    }

    info!("✅ fetch_nft_metadata completed, inserted {} rows", inserted);
    Ok(())
}

// ================== Blockscout 更新 ==================
#[derive(Debug, sqlx::FromRow)]
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
    let pool = &config.postgres_db.pool;
    let client = &config.http_client;

    let rows: Vec<MetadataPartial> = sqlx::query_as::<_, MetadataPartial>(
        "SELECT id, chainid, address, token_type, is_verified, risk_level FROM metadata",
    )
    .fetch_all(pool)
    .await
    .context("Failed to fetch metadata rows")?;

    let mut updated_count = 0;
    let mut fail_count_by_chain = std::collections::HashMap::<i64, usize>::new();

    for row in rows {
        if row.token_type.is_some() && row.is_verified.is_some() && row.risk_level.is_some() {
            continue;
        }

        let base_url = match config.blockscout_endpoints.get(&row.chainid) {
            Some(url) => url,
            None => continue,
        };
        let api_url = format!("{}/{}", base_url, row.address);

        let resp = client.get(&api_url)
            .header("accept", "application/json")
            .send()
            .await;

        let resp = match resp {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                warn!(chainid=row.chainid, status=?r.status(), "API request failed");
                *fail_count_by_chain.entry(row.chainid).or_default() += 1;
                continue;
            }
            Err(e) => {
                warn!(chainid=row.chainid, error=?e, "HTTP request failed");
                *fail_count_by_chain.entry(row.chainid).or_default() += 1;
                continue;
            }
        };

        let data: BlockscoutResponse = resp.json()
            .await
            .context(format!("Failed to parse JSON for {}", row.address))?;

        if !data.is_contract {
            continue;
        }

        let risk_level = if data.is_scam { Some("scam".to_string()) } else { None };
        let token_type = data.token.and_then(|t| t.token_type);

        sqlx::query(
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
        .await
        .with_context(|| format!("Failed to update metadata id={}", row.id))?;

        updated_count += 1;
    }

    info!("✅ update_metadata_from_blockscout completed, updated {} rows", updated_count);
    for (chainid, fails) in fail_count_by_chain {
        warn!(chainid, fails, "API or update failures");
    }

    Ok(())
}