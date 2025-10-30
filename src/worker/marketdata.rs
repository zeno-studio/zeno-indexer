use crate::config::Config;
use crate::utils::{FetchResult, get_json_with_retry};
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;
use sqlx::{Postgres, QueryBuilder, Transaction, Executor};
use tracing::info;

/// Maximum number of retry attempts for API requests
const MAX_RETRIES: usize = 3;
/// Maximum consecutive failures before giving up
const MAX_CONSECUTIVE_FAIL: usize = 3;
/// Number of tokens to fetch per page
const TOKENS_PER_PAGE: u32 = 250;

/// Market data structure from CoinGecko API
///
/// Contains comprehensive market information for a cryptocurrency,
/// including price changes, market cap, supply metrics, and historical data.
#[derive(Deserialize, Debug)]
pub struct MarketData {
    /// Token identifier (e.g., "bitcoin", "ethereum")
    pub id: String,
    /// Token symbol (e.g., "BTC", "ETH")
    pub symbol: String,
    /// Full token name
    pub name: String,
    /// URL to token logo/image
    pub image: Option<String>,
    /// Current market capitalization in USD
    pub market_cap: Option<f64>,
    /// Market cap rank (1 = highest market cap)
    pub market_cap_rank: Option<i64>,
    /// Fully diluted valuation in USD
    pub fully_diluted_valuation: Option<f64>,
    /// Price change in last 24 hours (absolute USD)
    pub price_change_24h: Option<f64>,
    /// Price change in last 24 hours (percentage)
    pub price_change_percentage_24h: Option<f64>,
    /// Circulating supply amount
    pub circulating_supply: Option<f64>,
    /// Total supply amount
    pub total_supply: Option<f64>,
    /// Maximum possible supply
    pub max_supply: Option<f64>,
    /// All-time high price in USD
    pub ath: Option<f64>,
    /// Date when all-time high was reached
    pub ath_date: Option<String>,
    /// All-time low price in USD
    pub atl: Option<f64>,
    /// Date when all-time low was reached
    pub atl_date: Option<String>,
    /// Timestamp of last update
    pub last_updated: Option<String>,
}

/// Fetches one page of market data from CoinGecko API
///
/// Uses the shared rate limiter and get_json_with_retry utility for robust API calls.
///
/// # Arguments
/// * `config` - Application configuration with HTTP client and rate limiter
/// * `page` - Page number (1-indexed)
///
/// # Returns
/// * `Ok(Vec<MarketData>)` - List of tokens on this page (may be empty if no more data)
/// * `Err(anyhow::Error)` - Request failed after all retries
///
/// # Rate Limiting
/// Uses shared CoinGecko rate limiter (28 requests per minute)
async fn fetch_tokens_page(config: &Config, page: u32) -> Result<Vec<MarketData>> {
    let url = format!(
        "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page={}&page={}",
        TOKENS_PER_PAGE, page
    );

    let result = get_json_with_retry::<Value>(
        config,
        &url,
        |r| {
            r.header("x-cg-demo-api-key", &config.coingecko_key)
                .header("Accept", "application/json")
        },
        MAX_RETRIES,
        MAX_CONSECUTIVE_FAIL,
        Some(&config.coingecko_rate_limiter),
    )
    .await;

    match result {
        FetchResult::Success(resp) => {
            // Parse as array of MarketData
            let tokens: Vec<MarketData> = serde_json::from_value(resp)
                .context("Failed to parse market data from JSON")?;
            Ok(tokens)
        }
        FetchResult::Empty => {
            // Empty response means no more data
            Ok(Vec::new())
        }
        FetchResult::Failed(e) => {
            anyhow::bail!("Failed to fetch page {}: {}", page, e);
        }
    }
}

/// Bulk inserts market data into the database
///
/// Uses SQLx QueryBuilder for efficient batch insertion within a transaction.
/// This ensures thread safety (Send-safe) and atomic operations.
///
/// # Arguments
/// * `tx` - Active database transaction
/// * `tokens` - Slice of MarketData to insert
///
/// # Returns
/// * `Ok(())` - All tokens inserted successfully
/// * `Err(anyhow::Error)` - Database insertion failed
///
/// # Performance
/// Bulk insertion is much faster than individual inserts,
/// especially for large datasets (e.g., 250 tokens per page).
async fn insert_bulk_tokens(
    tx: &mut Transaction<'_, Postgres>,
    tokens: &[MarketData],
) -> Result<()> {
    // Early return if no data to insert
    if tokens.is_empty() {
        return Ok(());
    }

    // Build bulk insert query
    let mut qb = QueryBuilder::<Postgres>::new(
        "INSERT INTO marketdata (
            tokenid, symbol, name, image, market_cap, market_cap_rank,
            fully_diluted_valuation, price_change_24h, price_change_percentage_24h,
            circulating_supply, total_supply, max_supply, ath, ath_date,
            atl, atl_date, last_updated
        ) ",
    );

    // Add VALUES clause with all tokens
    qb.push_values(tokens.iter(), |mut b, token| {
        b.push_bind(&token.id)
            .push_bind(&token.symbol)
            .push_bind(&token.name)
            .push_bind(&token.image)
            .push_bind(token.market_cap)
            .push_bind(token.market_cap_rank)
            .push_bind(token.fully_diluted_valuation)
            .push_bind(token.price_change_24h)
            .push_bind(token.price_change_percentage_24h)
            .push_bind(token.circulating_supply)
            .push_bind(token.total_supply)
            .push_bind(token.max_supply)
            .push_bind(token.ath)
            .push_bind(&token.ath_date)
            .push_bind(token.atl)
            .push_bind(&token.atl_date)
            .push_bind(&token.last_updated);
    });

    // Execute the bulk insert
    qb.build()
        .execute(&mut **tx)
        .await
        .context("Failed to insert marketdata in bulk")?;

    Ok(())
}

/// Synchronizes cryptocurrency market data from CoinGecko
///
/// This is the main entry point for market data synchronization.
/// It performs a full data refresh by:
/// 1. Truncating the existing marketdata table
/// 2. Fetching all pages from CoinGecko API
/// 3. Bulk inserting data in a single transaction
///
/// # Arguments
/// * `config` - Application configuration with database pool and API keys
///
/// # Returns
/// * `Ok(())` - Sync completed successfully
/// * `Err(anyhow::Error)` - Sync failed (transaction rolled back)
///
/// # Performance Characteristics
/// - Uses pagination (250 tokens per page)
/// - Respects CoinGecko rate limits (300ms between requests)
/// - Single transaction ensures atomicity
/// - Typical runtime: ~1-2 minutes for ~10,000 tokens
///
/// # Database Schema
/// Requires the `marketdata` table to exist (created via migrations)
pub async fn sync_marketdata(config: &Config) -> Result<()> {
    info!("🚀 Market data synchronization started");

    // Start database transaction
    let mut tx = config
        .postgres_db
        .pool
        .begin()
        .await
        .context("Failed to start transaction")?;

    // Clear existing data (full refresh strategy)
    tx.execute("TRUNCATE TABLE marketdata")
        .await
        .context("Failed to truncate marketdata table")?;

    // Fetch and insert data page by page
    let mut page = 1;
    let mut total_tokens = 0;
    
    loop {
        // Fetch one page of data (uses shared rate limiter)
        let tokens = fetch_tokens_page(config, page)
            .await
            .with_context(|| format!("Failed to fetch page {}", page))?;

        // Empty response means we've reached the end
        if tokens.is_empty() {
            info!("Reached end of data at page {}", page);
            break;
        }

        let token_count = tokens.len();
        total_tokens += token_count;

        // Bulk insert this page's data
        insert_bulk_tokens(&mut tx, &tokens).await?;
        info!("✓ Page {}: inserted {} tokens (total: {})", page, token_count, total_tokens);

        page += 1;
        
        // Rate limiting is now handled by the shared rate limiter in get_json_with_retry
        // No additional sleep needed here
    }

    // Commit all changes atomically
    tx.commit().await.context("Failed to commit transaction")?;

    // Verify final count
    let row_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM marketdata")
        .fetch_one(&config.postgres_db.pool)
        .await
        .unwrap_or((0,));

    info!(
        "✅ Market data sync completed: {} tokens across {} pages",
        row_count.0, page - 1
    );
    Ok(())
}

// ============= Unit Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert_eq!(MAX_RETRIES, 3);
        assert_eq!(MAX_CONSECUTIVE_FAIL, 3);
        assert_eq!(TOKENS_PER_PAGE, 250);
    }

    #[test]
    fn test_market_data_deserialization() {
        let json = r#"{
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "image": "https://example.com/btc.png",
            "market_cap": 1000000000.0,
            "market_cap_rank": 1,
            "fully_diluted_valuation": 1100000000.0,
            "price_change_24h": 1000.0,
            "price_change_percentage_24h": 2.5,
            "circulating_supply": 19000000.0,
            "total_supply": 21000000.0,
            "max_supply": 21000000.0,
            "ath": 69000.0,
            "ath_date": "2021-11-10T00:00:00.000Z",
            "atl": 67.81,
            "atl_date": "2013-07-06T00:00:00.000Z",
            "last_updated": "2024-01-01T00:00:00.000Z"
        }"#;

        let result: Result<MarketData, _> = serde_json::from_str(json);
        assert!(result.is_ok());

        let data = result.unwrap();
        assert_eq!(data.id, "bitcoin");
        assert_eq!(data.symbol, "btc");
        assert_eq!(data.name, "Bitcoin");
        assert_eq!(data.market_cap_rank, Some(1));
    }

    #[test]
    fn test_market_data_optional_fields() {
        let json = r#"{
            "id": "test-coin",
            "symbol": "test",
            "name": "Test Coin"
        }"#;

        let result: Result<MarketData, _> = serde_json::from_str(json);
        assert!(result.is_ok());

        let data = result.unwrap();
        assert_eq!(data.id, "test-coin");
        assert!(data.image.is_none());
        assert!(data.market_cap.is_none());
        assert!(data.ath.is_none());
    }
}
