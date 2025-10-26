use crate::config::Config;
use crate::utils::{FetchResult, get_json_with_retry};
use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use serde_json::Value;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

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
        5,
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

    let chains_map: HashMap<String, i64> =
        sqlx::query_as::<_, (String, i64)>("SELECT name, chainid FROM chains")
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

    info!(
        "✅ sync_tokenmap completed: inserted {}, skipped {}",
        inserted, skipped
    );
    Ok(())
}

// ================== NFTMap 同步 ==================
pub async fn sync_nftmap(config: &Config) -> Result<()> {
    info!("🔄 Syncing nftmap from Coingecko...");

    let pool = &config.postgres_db.pool;
    let mut inserted = 0usize;
    let mut skipped = 0usize;

    let chains_map: HashMap<String, i64> =
        sqlx::query_as::<_, (String, i64)>("SELECT name, chainid FROM chains")
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
            5,
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

            if id.is_none()
                || name.is_none()
                || symbol.is_none()
                || addr.is_empty()
                || platform.is_empty()
            {
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

    info!(
        "✅ sync_nftmap completed: inserted {}, skipped {}",
        inserted, skipped
    );
    Ok(())
}

// ======================= Metadata Structures =======================
/// Internal structure for metadata operations
/// Contains all fields needed for token/NFT metadata
#[derive(Debug)]
struct MetadataItem<'a> {
    /// Token ID from CoinGecko (for fungible tokens)
    tokenid: Option<&'a str>,
    /// NFT ID from CoinGecko (for NFTs)
    nftid: Option<&'a str>,
    /// Token symbol (e.g., "ETH", "USDT")
    symbol: &'a str,
    /// Token full name
    name: &'a str,
    /// Blockchain chain ID
    chainid: i64,
    /// Contract address (lowercase hex)
    address: &'a str,
    /// Token decimals (for fungible tokens only)
    decimals: Option<i64>,
    /// Project homepage URL
    homepage: Option<&'a str>,
    /// Token logo/image URL
    image: Option<&'a str>,
    /// Project description text
    description: Option<&'a str>,
    /// Additional notices/warnings in JSON format
    notices: Option<Value>,
}

// ======================= Database Operations =======================

/// Inserts new metadata record (skips if already exists)
///
/// Uses ON CONFLICT DO NOTHING to avoid updating existing records.
/// This is used for daily incremental sync where we only want to add new tokens.
///
/// # Arguments
/// * `pool` - Database connection pool
/// * `data` - Metadata to insert
///
/// # Returns
/// * `Ok(())` - Insert succeeded or was skipped due to conflict
/// * `Err` - Database error occurred
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
    .bind(data.notices.as_ref().map(sqlx::types::Json))
    .execute(pool)
    .await?;
    Ok(())
}

/// Force updates metadata record (updates all fields regardless of existing values)
///
/// Uses ON CONFLICT DO UPDATE to unconditionally replace all fields with new values.
/// This ensures complete data refresh during monthly comprehensive sync.
///
/// # Update Strategy
/// - Always update: symbol, name (core identifiers)
/// - Always update: homepage, image, description, notices (full refresh)
/// - COALESCE is used to preserve non-null old values when new value is NULL
///
/// # Arguments
/// * `pool` - Database connection pool
/// * `data` - New metadata values to apply
///
/// # Returns
/// * `Ok(())` - Insert or update succeeded
/// * `Err` - Database error occurred
///
/// # Note
/// Currently unused but kept for future monthly force update feature
#[allow(dead_code)]
async fn force_update_metadata(pool: &PgPool, data: &MetadataItem<'_>) -> Result<()> {
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
    .bind(data.notices.as_ref().map(sqlx::types::Json))
    .execute(pool)
    .await?;
    Ok(())
}

// ======================= Daily Incremental Sync =======================

/// Daily incremental sync of token metadata from CoinGecko
///
/// This function fetches metadata only for NEW tokens that don't exist in the metadata table yet.
/// It skips tokens that already have metadata to minimize API calls and respect rate limits.
///
/// # Workflow
/// 1. Load tokens from tokenmap (where id > last_update_id)
/// 2. For each token, check if metadata already exists
/// 3. If exists, skip (to save API calls)
/// 4. If not exists, fetch from CoinGecko API and insert
/// 5. Update config: set token_update_id to max_id on failure, 0 on success
///
/// # Arguments
/// * `config` - Mutable application configuration (for updating token_update_id)
///
/// # Returns
/// * `Ok(())` - All tokens processed successfully (config.token_update_id set to 0)
/// * `Err` - Fatal error (database connection, API failure, etc.; config.token_update_id set to max_id)
///
/// # Performance
/// - Only processes new tokens (skips existing via contract_exists check)
/// - Rate limit: 300ms between API calls
/// - Typical runtime: ~1-5 minutes depending on new tokens count
///
/// # Side Effects
/// - Updates config.token_update_id: 0 on completion, max_id on interruption
pub async fn fetch_token_metadata(config: &mut Config) -> Result<()> {
    let pool = &config.postgres_db.pool;

    // Start from last processed token ID (for incremental processing)
    let last_update_id = config.token_update_id;

    let tokenmap: Vec<(i64, String, String, i64, String)> = sqlx::query_as(
        "SELECT id, tokenid, name, chainid, address FROM tokenmap WHERE id > $1 ORDER BY id ASC",
    )
    .bind(last_update_id)
    .fetch_all(pool)
    .await
    .context("Failed to load tokenmap for metadata")?;

    let mut max_id = last_update_id;
    let mut inserted = 0usize;
    let total = tokenmap.len();

    for (i, (id, token_id, _name, chainid, address)) in tokenmap.into_iter().enumerate() {
        max_id = id; // Track current max ID for resume capability

        // Skip tokens that already have metadata (daily sync only adds new ones)
        if config
            .postgres_db
            .contract_exists(&address, chainid)
            .await
            .unwrap_or(false)
        {
            continue; // Metadata exists, skip to save API calls
        }

        let url = format!("https://api.coingecko.com/api/v3/coins/{}", token_id);
        let result = get_json_with_retry::<Value>(
            config,
            &url,
            |r| {
                r.header("x-cg-demo-api-key", &config.coingecko_key)
                    .header("Accept", "application/json")
                    .query(&[
                        ("localization", "false"),
                        ("tickers", "false"),
                        ("market_data", "false"),
                        ("developer_data", "false"),
                        ("sparkline", "false"),
                    ])
            },
            5,
            3,
        )
        .await;

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

                // Insert new metadata (will skip if conflict due to race condition)
                match insert_metadata(pool, &data).await {
                    Ok(_) => {
                        inserted += 1;
                    }
                    Err(e) => warn!("Insert failed for token {}: {}", token_id, e),
                }
            }

            FetchResult::Empty => {
                warn!("⚠️ Token {} returned empty response", token_id);
            }

            FetchResult::Failed(e) => {
                warn!(
                    "❌ Failed to fetch token {} ({}/{}): {}",
                    token_id,
                    i + 1,
                    total,
                    e
                );
                // Update config to resume from this ID on next run
                config.set_token_update_id(max_id);
                return Err(anyhow!("API request failed for token {}: {}", token_id, e));
            }
        }

        sleep(Duration::from_millis(300)).await;
    }

    info!(
        "✅ Daily token metadata sync completed: {} new tokens inserted",
        inserted
    );
    
    // Reset to 0 to indicate full completion (next run starts from beginning)
    config.set_token_update_id(0);
    Ok(())
}

// ======================= Monthly Force Update (Commented Out) =======================

/*
/// Monthly force update of ALL token metadata (CURRENTLY DISABLED DUE TO API LIMITS)
///
/// This function updates metadata for ALL tokens, regardless of whether they exist.
/// It's designed for comprehensive monthly refresh but is commented out due to API rate limits.
///
/// # Workflow
/// 1. Load ALL tokens from tokenmap
/// 2. For each token, fetch latest data from CoinGecko
/// 3. Force update using upsert (updates all fields)
/// 4. No skipping - processes every token
///
/// # WARNING
/// This function makes API calls for ALL tokens and will likely exceed rate limits.
/// Estimated: ~10,000 tokens × 300ms = ~50 minutes runtime
///
/// # TODO
/// - Implement batch processing with longer delays
/// - Add API quota monitoring
/// - Consider splitting into multiple runs
///
/// # Arguments
/// * `config` - Application configuration
///
/// # Returns
/// * `Ok(())` - All tokens updated
/// * `Err` - Error occurred
pub async fn force_update_all_token_metadata(config: &Config) -> Result<()> {
    let pool = &config.postgres_db.pool;

    info!("🔄 Starting FORCE UPDATE for all tokens (comprehensive monthly sync)");

    // Load ALL tokens (no ID filter for complete refresh)
    let tokenmap: Vec<(i64, String, String, i64, String)> = sqlx::query_as(
        "SELECT id, tokenid, name, chainid, address FROM tokenmap ORDER BY id ASC",
    )
    .fetch_all(pool)
    .await
    .context("Failed to load tokenmap for force update")?;

    let mut updated = 0usize;
    let mut failed = 0usize;
    let total = tokenmap.len();

    info!("📊 Total tokens to update: {}", total);

    for (i, (id, token_id, _name, chainid, address)) in tokenmap.into_iter().enumerate() {
        // NO skip check - force update all tokens

        let url = format!("https://api.coingecko.com/api/v3/coins/{}", token_id);
        let result = get_json_with_retry::<Value>(
            config,
            &url,
            |r| {
                r.header("x-cg-demo-api-key", &config.coingecko_key)
                    .header("Accept", "application/json")
                    .query(&[
                        ("localization", "false"),
                        ("tickers", "false"),
                        ("market_data", "false"),
                        ("developer_data", "false"),
                        ("sparkline", "false"),
                    ])
            },
            5,
            3,
        )
        .await;

        match result {
            FetchResult::Success(resp) => {
                let tokenid = resp.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let symbol = resp.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
                let name = resp.get("name").and_then(|v| v.as_str()).unwrap_or("");
                
                if symbol.is_empty() || name.is_empty() {
                    warn!("Skipping token {} with empty symbol/name", token_id);
                    failed += 1;
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

                // Force update using upsert
                match force_update_metadata(pool, &data).await {
                    Ok(_) => {
                        updated += 1;
                    }
                    Err(e) => {
                        warn!("Force update failed for token {}: {}", token_id, e);
                        failed += 1;
                    }
                }
            }

            FetchResult::Empty => {
                warn!("⚠️ Token {} returned empty response", token_id);
                failed += 1;
            }

            FetchResult::Failed(e) => {
                warn!("❌ Failed to fetch token {} ({}/{}): {}", token_id, i + 1, total, e);
                failed += 1;
            }
        }

        // Progress logging every 100 tokens
        if (i + 1) % 100 == 0 {
            info!(
                "📈 Progress: {}/{} ({:.1}%) - {} updated, {} failed",
                i + 1,
                total,
                (i + 1) as f64 / total as f64 * 100.0,
                updated,
                failed
            );
        }

        // Rate limiting - respect CoinGecko limits
        sleep(Duration::from_millis(300)).await;
    }

    info!(
        "✅ Force update completed: {} updated, {} failed out of {} total",
        updated, failed, total
    );
    Ok(())
}
*/

/// Daily incremental sync of NFT metadata from CoinGecko
///
/// Similar to fetch_token_metadata but for NFTs.
/// Only fetches metadata for NEW NFTs that don't exist in the metadata table yet.
///
/// # Workflow
/// 1. Load NFTs from nftmap (where id > last_update_id)
/// 2. For each NFT, check if metadata already exists
/// 3. If exists, skip (to save API calls)
/// 4. If not exists, fetch from CoinGecko API and insert
/// 5. Update config: set nft_update_id to max_id on failure, 0 on success
///
/// # Arguments
/// * `config` - Mutable application configuration (for updating nft_update_id)
///
/// # Returns
/// * `Ok(())` - All NFTs processed successfully (config.nft_update_id set to 0)
/// * `Err` - Fatal error (database connection, API failure, etc.; config.nft_update_id set to max_id)
///
/// # Side Effects
/// - Updates config.nft_update_id: 0 on completion, max_id on interruption
pub async fn fetch_nft_metadata(config: &mut Config) -> Result<()> {
    let pool = &config.postgres_db.pool;

    let last_update_id = config.nft_update_id;

    let nftmap: Vec<(i64, String, String, i64, String)> = sqlx::query_as(
        "SELECT id, nftid, name, chainid, address FROM nftmap WHERE id > $1 ORDER BY id ASC",
    )
    .bind(last_update_id)
    .fetch_all(pool)
    .await
    .context("Failed to load nftmap")?;

    let mut max_id = last_update_id;
    let mut inserted = 0usize;
    let total = nftmap.len();

    for (i, (id, nft_id, _name, chainid, address)) in nftmap.into_iter().enumerate() {
        max_id = id;

        // Skip NFTs that already have metadata (daily sync only adds new ones)
        if config
            .postgres_db
            .contract_exists(&address, chainid)
            .await
            .unwrap_or(false)
        {
            continue; // Metadata exists, skip to save API calls
        }

        let url = format!("https://api.coingecko.com/api/v3/nfts/{}", nft_id);
        let result = get_json_with_retry::<Value>(
            config,
            &url,
            |r| {
                r.header("x-cg-demo-api-key", &config.coingecko_key)
                    .header("Accept", "application/json")
            },
            5,
            3,
        )
        .await;

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

                // Insert new NFT metadata
                match insert_metadata(pool, &data).await {
                    Ok(_) => {
                        inserted += 1;
                    }
                    Err(e) => warn!("Insert failed for NFT {}: {}", nft_id, e),
                }
            }

            FetchResult::Empty => {
                warn!("⚠️ NFT {} returned empty response", nft_id);
            }

            FetchResult::Failed(e) => {
                warn!(
                    "❌ Failed to fetch NFT {} ({}/{}): {}",
                    nft_id,
                    i + 1,
                    total,
                    e
                );
                // Update config to resume from this ID on next run
                config.set_nft_update_id(max_id);
                return Err(anyhow!("API request failed for NFT {}: {}", nft_id, e));
            }
        }

        sleep(Duration::from_millis(300)).await;
    }

    info!(
        "✅ Daily NFT metadata sync completed: {} new NFTs inserted",
        inserted
    );
    
    // Reset to 0 to indicate full completion (next run starts from beginning)
    config.set_nft_update_id(0);
    Ok(())
}

/*
/// Monthly force update of ALL NFT metadata (CURRENTLY DISABLED DUE TO API LIMITS)
///
/// Similar to force_update_all_token_metadata but for NFTs.
/// Updates metadata for ALL NFTs regardless of existing records.
///
/// # WARNING
/// Currently commented out due to API rate limit concerns.
///
/// # Arguments
/// * `config` - Application configuration
///
/// # Returns
/// * `Ok(())` - All NFTs updated
/// * `Err` - Error occurred
pub async fn force_update_all_nft_metadata(config: &Config) -> Result<()> {
    let pool = &config.postgres_db.pool;

    info!("🔄 Starting FORCE UPDATE for all NFTs (comprehensive monthly sync)");

    // Load ALL NFTs
    let nftmap: Vec<(i64, String, String, i64, String)> = sqlx::query_as(
        "SELECT id, nftid, name, chainid, address FROM nftmap ORDER BY id ASC",
    )
    .fetch_all(pool)
    .await
    .context("Failed to load nftmap")?;

    let mut updated = 0usize;
    let mut failed = 0usize;
    let total = nftmap.len();

    info!("📊 Total NFTs to update: {}", total);

    for (i, (id, nft_id, _name, chainid, address)) in nftmap.into_iter().enumerate() {
        // NO skip check - force update all NFTs

        let url = format!("https://api.coingecko.com/api/v3/nfts/{}", nft_id);
        let result = get_json_with_retry::<Value>(
            config,
            &url,
            |r| {
                r.header("x-cg-demo-api-key", &config.coingecko_key)
                    .header("Accept", "application/json")
            },
            5,
            3,
        )
        .await;

        match result {
            FetchResult::Success(resp) => {
                let symbol = resp.get("symbol").and_then(|v| v.as_str()).unwrap_or("");
                let name = resp.get("name").and_then(|v| v.as_str()).unwrap_or("");
                
                if symbol.is_empty() || name.is_empty() {
                    warn!("NFT {} has empty symbol/name, skipping", nft_id);
                    failed += 1;
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

                // Force update using upsert
                match force_update_metadata(pool, &data).await {
                    Ok(_) => {
                        updated += 1;
                    }
                    Err(e) => {
                        warn!("Force update failed for NFT {}: {}", nft_id, e);
                        failed += 1;
                    }
                }
            }

            FetchResult::Empty => {
                warn!("⚠️ NFT {} returned empty response", nft_id);
                failed += 1;
            }

            FetchResult::Failed(e) => {
                warn!("❌ Failed to fetch NFT {} ({}/{}): {}", nft_id, i + 1, total, e);
                failed += 1;
            }
        }

        // Progress logging
        if (i + 1) % 100 == 0 {
            info!(
                "📈 Progress: {}/{} ({:.1}%) - {} updated, {} failed",
                i + 1,
                total,
                (i + 1) as f64 / total as f64 * 100.0,
                updated,
                failed
            );
        }

        sleep(Duration::from_millis(300)).await;
    }

    info!(
        "✅ NFT force update completed: {} updated, {} failed out of {} total",
        updated, failed, total
    );
    Ok(())
}
*/

// ======================= Blockscout Metadata Enhancement =======================

/// Partial metadata structure for Blockscout updates
///
/// Contains only fields needed for selective update from Blockscout API.
/// This avoids loading unnecessary data when checking what needs updating.
#[derive(Debug, sqlx::FromRow)]
struct MetadataPartial {
    /// Database row ID
    id: i64,
    /// Blockchain chain ID
    chainid: i64,
    /// Contract address (lowercase hex)
    address: String,
    /// Token type (ERC-20, ERC-721, ERC-1155, etc.)
    token_type: Option<String>,
    /// Whether contract source code is verified
    is_verified: Option<bool>,
    /// Risk level assessment (e.g., "scam")
    risk_level: Option<String>,
}

/// Blockscout API response structure
///
/// Represents the JSON response from Blockscout's address info endpoint.
/// Uses serde(default) to handle missing fields gracefully.
#[derive(Debug, Deserialize)]
struct BlockscoutResponse {
    /// Whether the address is a smart contract
    #[serde(default)]
    is_contract: bool,
    /// Whether the contract source code is verified on Blockscout
    #[serde(default)]
    is_verified: bool,
    /// Whether the contract is flagged as a scam
    #[serde(default)]
    is_scam: bool,
    /// Token-specific information (if address is a token contract)
    #[serde(default)]
    token: Option<TokenInfo>,
}

/// Token information from Blockscout API
#[derive(Debug, Deserialize)]
struct TokenInfo {
    /// Token standard type (e.g., "ERC-20", "ERC-721")
    #[serde(rename = "type")]
    token_type: Option<String>,
}

/// Updates metadata with contract verification and risk information from Blockscout
///
/// This function enriches existing metadata records with additional information from
/// Blockscout blockchain explorers, including contract verification status, token type,
/// and risk assessment flags.
///
/// # Workflow
/// 1. Load all metadata records that are missing token_type, is_verified, or risk_level
/// 2. For each record, query the corresponding Blockscout API endpoint
/// 3. Parse response and extract: token_type, is_verified, is_scam flags
/// 4. Update database with new information (only non-null fields)
/// 5. Report statistics by chain
///
/// # Optimization Strategies
/// - Skip records that already have all three fields populated
/// - Skip chains without configured Blockscout endpoints
/// - Skip non-contract addresses (is_contract = false)
/// - Use COALESCE in UPDATE to preserve existing non-null values
/// - Retry failed requests up to 3 times with 500ms delay
///
/// # Arguments
/// * `config` - Application configuration with Blockscout endpoints and HTTP client
///
/// # Returns
/// * `Ok(())` - Update completed (some failures are tolerated)
/// * `Err` - Fatal error (database connection failure)
///
/// # Performance
/// - Rate limit: 200ms between requests
/// - Progress log: every 20 records
/// - Typical runtime: ~5-10 minutes for 1000 contracts
///
/// # Error Handling
/// - Individual API failures are logged but don't stop execution
/// - Final summary shows failure counts per chain
/// - Non-contract addresses are silently skipped
pub async fn update_metadata_from_blockscout(config: &Config) -> Result<()> {
    let pool = &config.postgres_db.pool;
    let client = &config.http_client;

    // Step 1: Load all metadata records (only fetch fields we need to check)
    // This minimizes memory usage when dealing with large datasets
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
        // Step 2: Skip if all required fields already populated (optimization)
        // No need to call API if we already have complete data
        if row.token_type.is_some() && row.is_verified.is_some() && row.risk_level.is_some() {
            skipped_count += 1;
            continue;
        }

        // Step 3: Check if Blockscout endpoint is configured for this chain
        let Some(base_url) = config.blockscout_endpoints.get(&row.chainid) else {
            warn!(
                "⚠️ No Blockscout endpoint configured for chainid {}",
                row.chainid
            );
            skipped_count += 1;
            continue;
        };

        let api_url = format!("{}/{}", base_url.trim_end_matches('/'), row.address);

        // Step 4: Call Blockscout API with retry mechanism (up to 3 attempts)
        // Retries handle transient network issues and rate limiting
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
                    break; // Success, exit retry loop
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
                        row.chainid, attempt, row.address, e
                    );
                }
            }
            // Wait before retry (exponential backoff could be added here)
            sleep(Duration::from_millis(500)).await;
        }

        // All retry attempts failed, record and continue to next address
        let Some(resp) = resp_opt else {
            *fail_count_by_chain.entry(row.chainid).or_default() += 1;
            continue;
        };

        // Step 5: Parse JSON response from Blockscout API
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

        // Step 6: Skip non-contract addresses (EOAs don't have metadata)
        if !data.is_contract {
            continue;
        }

        // Step 7: Extract relevant fields from API response
        // risk_level: Only set if flagged as scam (None means safe/unknown)
        let risk_level = if data.is_scam {
            Some("scam".to_string())
        } else {
            None
        };
        let token_type = data.token.as_ref().and_then(|t| t.token_type.clone());
        let is_verified = Some(data.is_verified);

        // Step 8: Check if we have any new data to update
        // Note: is_verified is always Some, so we always have at least one field to update
        // This is intentional - we want to record verification status even if false

        // Step 9: Update database with new information
        // COALESCE ensures we don't overwrite existing data with NULL
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

        // Progress logging every 20 records to monitor execution
        if (i + 1) % 20 == 0 {
            info!(
                "Progress: {}/{} processed ({} updated, {} skipped)",
                i + 1,
                rows.len(),
                updated_count,
                skipped_count
            );
        }

        // Rate limiting: 200ms delay between requests to respect API limits
        sleep(Duration::from_millis(200)).await;
    }

    // Step 10: Final summary with statistics
    info!(
        "✅ Blockscout update finished: {} updated, {} skipped",
        updated_count, skipped_count
    );

    // Report failures grouped by chain for debugging
    for (chainid, fails) in fail_count_by_chain {
        warn!("⚠️ Chain {}: {} failures", chainid, fails);
    }

    Ok(())
}
