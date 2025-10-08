use crate::config::Config;
use crate::utils::{FetchResult, get_json_with_retry};
use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

// ================== TokenMap 同步 ==================
pub async fn sync_tokenmap(config: &Config) -> Result<()> {
    info!("🔄 Syncing tokenmap from Coingecko...");

    let pool = &config.postgres_db.pool;
    let mut inserted = 0usize;
    let mut skipped = 0usize;

    let url = "https://api.coingecko.com/api/v3/coins/list?include_platform=true";
    let result = get_json_with_retry::<Value>(
        config,
        url,
        |r| {
            r.header("x-cg-demo-api-key", &config.coingecko_key)
                .header("Accept", "application/json")
        },
        3,
    )
    .await;

    let tokens = match result {
        FetchResult::Success(resp) => resp.as_array().cloned().unwrap_or_default(),
        FetchResult::Empty => {
            warn!("⚠️ Token list response empty");
            return Ok(());
        }
        FetchResult::Failed(e) => {
            return Err(anyhow!("Failed to fetch token list: {}", e));
        }
    };

    let chains_map: HashMap<String, i64> = sqlx::query_as::<_, (String, i64)>(
        "SELECT name, chainid FROM chains",
    )
    .fetch_all(pool)
    .await
    .context("Failed to load chains")?
    .into_iter()
    .collect();

    for token in tokens {
        let tokenid = token.get("id").and_then(|v| v.as_str());
        let symbol = token.get("symbol").and_then(|v| v.as_str());
        let name = token.get("name").and_then(|v| v.as_str());
        let platforms = token.get("platforms").and_then(|v| v.as_object());

        if tokenid.is_none() || name.is_none() || symbol.is_none() || platforms.is_none() {
            skipped += 1;
            continue;
        }

        for (platform, address_val) in platforms.unwrap() {
            let address = address_val.as_str().unwrap_or("").to_lowercase();
            if address.is_empty() {
                skipped += 1;
                continue;
            }

            let Some(chainid) = chains_map.get(platform) else {
                continue;
            };

            let res = sqlx::query(
                r#"
                INSERT INTO tokenmap (tokenid, symbol, name, chainid, address)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (address, chainid) DO NOTHING
                "#,
            )
            .bind(tokenid)
            .bind(symbol)
            .bind(name)
            .bind(chainid)
            .bind(&address)
            .execute(pool)
            .await;

            match res {
                Ok(_) => inserted += 1,
                Err(e) => warn!(
                    "Insert failed for token {}:{} => {}",
                    tokenid.unwrap(),
                    address,
                    e
                ),
            }
        }
    }

    info!("✅ sync_tokenmap completed: inserted {}, skipped {}", inserted, skipped);
    Ok(())
}

// ================== NFTMap 同步 ==================
pub async fn sync_nftmap(config: &Config) -> Result<()> {
    info!("🔄 Syncing nftmap from Coingecko...");

    let pool = &config.postgres_db.pool;
    let mut inserted = 0usize;
    let mut skipped = 0usize;

    let chains_map: HashMap<String, i64> = sqlx::query_as::<_, (String, i64)>(
        "SELECT name, chainid FROM chains",
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .collect();

    let mut page = 1usize;

    loop {
        let url = format!(
            "https://api.coingecko.com/api/v3/nfts/list?per_page=250&page={}",
            page
        );

        let result = get_json_with_retry::<Value>(
            config,
            &url,
            |r| {
                r.header("x-cg-demo-api-key", &config.coingecko_key)
                    .header("Accept", "application/json")
            },
            3,
        )
        .await;

        let nfts = match result {
            FetchResult::Success(resp) => resp.as_array().cloned().unwrap_or_default(),
            FetchResult::Empty => {
                info!("Reached empty NFT list on page {}, stopping.", page);
                break;
            }
            FetchResult::Failed(e) => {
                return Err(anyhow!("Failed to fetch NFT list: {}", e));
            }
        };

        if nfts.is_empty() {
            break;
        }

        for nft in &nfts {
            let id = nft.get("id").and_then(|v| v.as_str());
            let name = nft.get("name").and_then(|v| v.as_str());
            let symbol = nft.get("symbol").and_then(|v| v.as_str());
            let addr = nft
                .get("contract_address")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_lowercase();
            let platform = nft
                .get("asset_platform_id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            if id.is_none() || name.is_none() || symbol.is_none() || addr.is_empty() || platform.is_empty() {
                skipped += 1;
                continue;
            }

            let Some(chainid) = chains_map.get(&platform) else {
                skipped += 1;
                continue;
            };

            let res = sqlx::query(
                r#"
                INSERT INTO nftmap (nftid, symbol, name, chainid, address)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (address, chainid) DO NOTHING
                "#,
            )
            .bind(id)
            .bind(symbol)
            .bind(name)
            .bind(chainid)
            .bind(&addr)
            .execute(pool)
            .await;

            if res.is_ok() {
                inserted += 1;
            } else {
                skipped += 1;
            }
        }

        info!("✅ Processed page {}, total inserted {}", page, inserted);
        page += 1;
        sleep(Duration::from_millis(300)).await;
    }

    info!("✅ sync_nftmap completed: inserted {}, skipped {}", inserted, skipped);
    Ok(())
}

// ======================= Token Metadata 同步 =======================
#[derive(Debug, Clone, Copy)]
pub enum SyncMode {
    Insert,
    Upsert,
}

// ======================= 通用 Metadata 结构体 =======================
#[derive(Debug)]
struct MetadataItem<'a> {
    tokenid: Option<&'a str>,
    nftid: Option<&'a str>,
    symbol: &'a str,
    name: &'a str,
    chainid: i64,
    address: &'a str,
    decimals: Option<i64>,
    homepage: Option<&'a str>,
    image: Option<&'a str>,
    description: Option<&'a str>,
    notices: Option<Value>,
}

// ======================= 通用插入与更新 =======================
async fn insert_metadata(pool: &PgPool, data: &MetadataItem<'_>) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO metadata (
            tokenid, nftid, symbol, name, chainid, address, decimals, homepage, image, description, notices, created_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
        ON CONFLICT (address, chainid) DO NOTHING
        "#,
    )
    .bind(data.tokenid)
    .bind(data.nftid)
    .bind(data.symbol)
    .bind(data.name)
    .bind(data.chainid)
    .bind(data.address)
    .bind(data.decimals)
    .bind(data.homepage)
    .bind(data.image)
    .bind(data.description)
    .bind(&data.notices)
    .execute(pool)
    .await?;
    Ok(())
}

async fn upsert_metadata(pool: &PgPool, data: &MetadataItem<'_>) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO metadata (
            tokenid, nftid, symbol, name, chainid, address, decimals, homepage, image, description, notices, created_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
        ON CONFLICT (address, chainid)
        DO UPDATE SET
            symbol = EXCLUDED.symbol,
            name = EXCLUDED.name,
            homepage = COALESCE(EXCLUDED.homepage, metadata.homepage),
            image = COALESCE(EXCLUDED.image, metadata.image),
            description = COALESCE(EXCLUDED.description, metadata.description),
            notices = COALESCE(EXCLUDED.notices, metadata.notices),
            updated_at = NOW()
        "#,
    )
    .bind(data.tokenid)
    .bind(data.nftid)
    .bind(data.symbol)
    .bind(data.name)
    .bind(data.chainid)
    .bind(data.address)
    .bind(data.decimals)
    .bind(data.homepage)
    .bind(data.image)
    .bind(data.description)
    .bind(&data.notices)
    .execute(pool)
    .await?;
    Ok(())
}

// ======================= Token Metadata 同步 =======================
pub async fn fetch_token_metadata(config: &Config, mode: SyncMode) -> Result<()> {
    let pool = &config.postgres_db.pool;

    let tokenmap: Vec<(String, String, i64, String)> =
        sqlx::query_as("SELECT tokenid, name, chainid, address FROM tokenmap")
            .fetch_all(pool)
            .await
            .context("Failed to load tokenmap for metadata")?;

    let mut inserted = 0usize;
    let mut updated = 0usize;
    let total = tokenmap.len();
    info!("🔍 Found {} tokens to sync", total);

    for (i, (token_id, _name, chainid, address)) in tokenmap.into_iter().enumerate() {
        
        if !config.is_initializing_metadata {
            if config
                .postgres_db
                .contract_exists(&address, chainid)
                .await
                .unwrap_or(false)
            {
                continue;
            }
        }

        let url = format!("https://api.coingecko.com/api/v3/coins/{}", token_id);
        let result = get_json_with_retry::<Value>(config, &url, |r| {
            r.header("x-cg-demo-api-key", &config.coingecko_key)
                .header("Accept", "application/json")
                .query(&[
                    ("localization", "false"),
                    ("tickers", "false"),
                    ("market_data", "false"),
                    ("developer_data", "false"),
                    ("sparkline", "false"),
                ])
        }, 3).await;

        match result {
            FetchResult::Success(resp) => {
                let tokenid = resp.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let symbol = resp.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
                let name = resp.get("name").and_then(|v| v.as_str()).unwrap_or("");
                if symbol.is_empty() || name.is_empty() {
                    warn!("Skipping token {} with empty symbol/name", token_id);
                    continue;
                }

                let homepage = resp.pointer("/links/homepage/0").and_then(|v| v.as_str());
                let image = resp.pointer("/image/large").and_then(|v| v.as_str());
                let description = resp.pointer("/description/en").and_then(|v| v.as_str());
                let notices = resp.get("additional_notices").cloned();
                
                let data = MetadataItem {
                    tokenid: Some(tokenid),
                    nftid: None,
                    symbol,
                    name,
                    chainid,
                    address: &address,
                    decimals: None,
                    homepage,
                    image,
                    description,
                    notices,
                };

                let res = match mode {
                    SyncMode::Insert => insert_metadata(pool, &data).await,
                    SyncMode::Upsert => upsert_metadata(pool, &data).await,
                };

                match res {
                    Ok(_) => {
                        if matches!(mode, SyncMode::Upsert) {
                            updated += 1;
                        } else {
                            inserted += 1;
                        }
                    }
                    Err(e) => warn!("Insert/Update failed for token {}: {}", token_id, e),
                }
            }

            FetchResult::Empty => {
                warn!("⚠️ Token {} returned empty response", token_id);
            }

            FetchResult::Failed(e) => {
                error!("❌ Failed to fetch token {} ({}/{}): {}", token_id, i + 1, total, e);
            }
        }

        sleep(Duration::from_millis(300)).await;
    }

    info!(
        "✅ fetch_token_metadata completed: inserted {}, updated {}",
        inserted, updated
    );
    Ok(())
}


// ======================= NFT Metadata 同步 =======================
pub async fn fetch_nft_metadata(config: &Config, mode: SyncMode) -> Result<()> {
    let pool = &config.postgres_db.pool;
    let nftmap: Vec<(String, String, i64, String)> =
        sqlx::query_as("SELECT nftid, name, chainid, address FROM nftmap")
            .fetch_all(pool)
            .await
            .context("Failed to load nftmap")?;

    let mut inserted = 0usize;
    let mut updated = 0usize;
    let total = nftmap.len();
    info!("🔍 Found {} NFTs to sync", total);

    for (i, (nft_id, _name, chainid, address)) in nftmap.into_iter().enumerate() {
        
        if !config.is_initializing_metadata {
            if config
                .postgres_db
                .contract_exists(&address, chainid)
                .await
                .unwrap_or(false)
            {
                continue;
            }
        }

        let url = format!("https://api.coingecko.com/api/v3/nfts/{}", nft_id);
        let result = get_json_with_retry::<Value>(config, &url, |r| {
            r.header("x-cg-demo-api-key", &config.coingecko_key)
                .header("Accept", "application/json")
        }, 3).await;

        match result {
            FetchResult::Success(resp) => {
                let symbol = resp.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
                let name = resp.get("name").and_then(|v| v.as_str()).unwrap_or("");
                if symbol.is_empty() || name.is_empty() {
                    warn!("NFT {} has empty symbol/name, skipping", nft_id);
                    continue;
                }

                let homepage = resp.pointer("/links/homepage/0").and_then(|v| v.as_str());
                let image = resp.pointer("/image/small").and_then(|v| v.as_str());
                let description = resp.pointer("/description").and_then(|v| v.as_str());

                let data = MetadataItem {
                    tokenid: None,
                    nftid: Some(&nft_id),
                    symbol,
                    name,
                    chainid,
                    address: &address,
                    decimals: None,
                    homepage,
                    image,
                    description,
                    notices: None,
                };

                let res = match mode {
                    SyncMode::Insert => insert_metadata(pool, &data).await,
                    SyncMode::Upsert => upsert_metadata(pool, &data).await,
                };

                match res {
                    Ok(_) => {
                        if matches!(mode, SyncMode::Upsert) {
                            updated += 1;
                        } else {
                            inserted += 1;
                        }
                    }
                    Err(e) => warn!("Insert/Update failed for NFT {}: {}", nft_id, e),
                }
            }

            FetchResult::Empty => {
                warn!("⚠️ NFT {} returned empty response", nft_id);
            }

            FetchResult::Failed(e) => {
                error!("❌ Failed to fetch NFT {} ({}/{}): {}", nft_id, i + 1, total, e);
            }
        }

        sleep(Duration::from_millis(300)).await;
    }

    info!(
        "✅ fetch_nft_metadata completed: inserted {}, updated {}",
        inserted, updated
    );
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
    #[serde(default)]
    is_contract: bool,
    #[serde(default)]
    is_verified: bool,
    #[serde(default)]
    is_scam: bool,
    #[serde(default)]
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

    // 1️⃣ 加载所有 metadata 行（仅加载需要字段）
    let rows: Vec<MetadataPartial> = sqlx::query_as::<_, MetadataPartial>(
        r#"SELECT id, chainid, address, token_type, is_verified, risk_level FROM metadata"#,
    )
    .fetch_all(pool)
    .await
    .context("Failed to fetch metadata rows")?;

    if rows.is_empty() {
        info!("⚠️ No metadata rows found, skipping Blockscout update");
        return Ok(());
    }

    let mut updated_count = 0usize;
    let mut skipped_count = 0usize;
    let mut fail_count_by_chain: HashMap<i64, usize> = HashMap::new();

    for (i, row) in rows.iter().enumerate() {
        // 已有数据齐全，跳过
        if row.token_type.is_some() && row.is_verified.is_some() && row.risk_level.is_some() {
            skipped_count += 1;
            continue;
        }

        let Some(base_url) = config.blockscout_endpoints.get(&row.chainid) else {
            warn!("⚠️ No Blockscout endpoint configured for chainid {}", row.chainid);
            skipped_count += 1;
            continue;
        };

        let api_url = format!("{}/{}", base_url.trim_end_matches('/'), row.address);

        // 2️⃣ 调用 Blockscout API（带重试）
        let mut resp_opt = None;
        for attempt in 1..=3 {
            match client
                .get(&api_url)
                .header("accept", "application/json")
                .send()
                .await
            {
                Ok(r) if r.status().is_success() => {
                    resp_opt = Some(r);
                    break;
                }
                Ok(r) => {
                    warn!(
                        "⚠️ [chainid={}] attempt {}/3: HTTP {} for {}",
                        row.chainid,
                        attempt,
                        r.status(),
                        row.address
                    );
                }
                Err(e) => {
                    warn!(
                        "⚠️ [chainid={}] attempt {}/3 failed for {}: {:?}",
                        row.chainid,
                        attempt,
                        row.address,
                        e
                    );
                }
            }
            sleep(Duration::from_millis(500)).await;
        }

        let Some(resp) = resp_opt else {
            *fail_count_by_chain.entry(row.chainid).or_default() += 1;
            continue;
        };

        // 3️⃣ 解析 JSON
        let data: BlockscoutResponse = match resp.json().await {
            Ok(json) => json,
            Err(e) => {
                warn!(
                    "⚠️ Failed to parse Blockscout JSON for {}: {:?}",
                    row.address, e
                );
                *fail_count_by_chain.entry(row.chainid).or_default() += 1;
                continue;
            }
        };

        // 非合约地址直接跳过
        if !data.is_contract {
            continue;
        }

        let risk_level = if data.is_scam {
            Some("scam".to_string())
        } else {
            None
        };
        let token_type = data.token.as_ref().and_then(|t| t.token_type.clone());
        let is_verified = Some(data.is_verified);

        // 若全为空，无需更新
        if token_type.is_none() && is_verified.is_none() && risk_level.is_none() {
            skipped_count += 1;
            continue;
        }

        // 4️⃣ 执行更新（仅更新非空字段）
        let res = sqlx::query(
            r#"
            UPDATE metadata
            SET
                token_type = COALESCE($1, token_type),
                is_verified = COALESCE($2, is_verified),
                risk_level = COALESCE($3, risk_level),
                updated_at = NOW()
            WHERE id = $4
            "#,
        )
        .bind(&token_type)
        .bind(&is_verified)
        .bind(&risk_level)
        .bind(row.id)
        .execute(pool)
        .await;

        match res {
            Ok(_) => {
                updated_count += 1;
                info!(
                    "✅ Updated metadata id={} (chainid={}, address={})",
                    row.id, row.chainid, row.address
                );
            }
            Err(e) => {
                warn!(
                    "❌ Failed to update metadata id={} ({}) -> {:?}",
                    row.id, row.address, e
                );
                *fail_count_by_chain.entry(row.chainid).or_default() += 1;
            }
        }

        if (i + 1) % 20 == 0 {
            info!(
                "Progress: {}/{} processed ({} updated, {} skipped)",
                i + 1,
                rows.len(),
                updated_count,
                skipped_count
            );
        }

        sleep(Duration::from_millis(200)).await;
    }

    // 5️⃣ 结果总结
    info!(
        "✅ Blockscout update finished: {} updated, {} skipped",
        updated_count, skipped_count
    );

    for (chainid, fails) in fail_count_by_chain {
        warn!("⚠️ Chain {}: {} failures", chainid, fails);
    }

    Ok(())
}
