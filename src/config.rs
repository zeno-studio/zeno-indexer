use anyhow::{Result, Context};
use reqwest::Client;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::collections::HashMap;
use std::env;
use tokio::time::Duration;
use tracing::info;
use crate::utils::RateLimiter;

/// Default maximum number of database connections in the pool
const DEFAULT_MAX_CONNECTIONS: u32 = 5;

/// PostgreSQL database connection manager
///
/// Manages the primary database connection pool and provides utilities
/// for database operations including migrations and health checks.
#[derive(Clone)]
pub struct PostgresDb {
    /// Primary database connection URL
    pub primary_db_url: String,
    /// SQLx connection pool for async database operations
    pub pool: PgPool,
}

impl PostgresDb {
    /// Creates a new PostgresDb instance with a lazy connection pool
    ///
    /// # Arguments
    /// * `primary_db_url` - PostgreSQL connection URL (e.g., "postgres://user:pass@host/db")
    ///
    /// # Panics
    /// Panics if the database URL format is invalid
    pub fn new(primary_db_url: String) -> Self {
        let pool = PgPoolOptions::new()
            .max_connections(DEFAULT_MAX_CONNECTIONS)
            .connect_lazy(&primary_db_url)
            .expect("Failed to create database connection pool: invalid DATABASE_URL format");
        
        PostgresDb {
            primary_db_url,
            pool,
        }
    }

    /// Updates the primary database URL and connection pool
    ///
    /// Performs validation to ensure the new database is:
    /// 1. A primary (not replica) node
    /// 2. Writable (performs test write operation)
    ///
    /// # Arguments
    /// * `new_url` - New PostgreSQL connection URL
    ///
    /// # Returns
    /// * `Ok(())` - Successfully updated to new database
    /// * `Err(sqlx::Error)` - Failed to connect or validate new database
    ///
    /// # Safety
    /// Uses atomic update pattern - only updates if all validations pass
    pub async fn update_primary_db_url(&mut self, new_url: String) -> Result<(), sqlx::Error> {
        // Create new pool first (don't modify state until validation passes)
        let new_pool = PgPoolOptions::new()
            .max_connections(DEFAULT_MAX_CONNECTIONS)
            .connect(&new_url)
            .await?;

        // Validation 1: Check if it's a primary node (not replica)
        let is_in_recovery: bool = sqlx::query("SELECT pg_is_in_recovery()")
            .fetch_one(&new_pool)
            .await?
            .get::<bool, _>(0);
        
        if is_in_recovery {
            return Err(sqlx::Error::Configuration(
                "New URL is a replica, not a primary database".into(),
            ));
        }

        // Validation 2: Test write capability
        sqlx::query("CREATE TEMPORARY TABLE IF NOT EXISTS health_check (id SERIAL PRIMARY KEY)")
            .execute(&new_pool)
            .await?;
        sqlx::query("INSERT INTO health_check DEFAULT VALUES")
            .execute(&new_pool)
            .await?;
        sqlx::query("DROP TABLE health_check")
            .execute(&new_pool)
            .await?;

        // All validations passed - now safe to update state atomically
        self.primary_db_url = new_url;
        self.pool = new_pool;
        
        info!("✅ Database URL updated successfully to new primary");
        Ok(())
    }

    /// Runs all pending database migrations
    ///
    /// Uses embedded migrations from the `migrations/` directory.
    /// Migrations are applied in chronological order based on filename.
    ///
    /// # Returns
    /// * `Ok(())` - All migrations applied successfully
    /// * `Err(anyhow::Error)` - Migration failed
    ///
    /// # Note
    /// This uses compile-time embedded migrations via `sqlx::migrate!()` macro.
    /// New migrations require recompilation to be included.
    pub async fn init_database(&self) -> Result<()> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .context("Failed to execute database migrations")?;

        info!("✅ Database migrations completed successfully");
        Ok(())
    }

    /// Initializes the chains table with default blockchain networks
    ///
    /// Only inserts data if the table is empty (idempotent operation).
    ///
    /// # Default Chains
    /// - Ethereum (1)
    /// - Optimism (10)
    /// - Polygon (137)
    /// - BSC (56)
    /// - Base (8453)
    /// - Arbitrum (42161)
    /// - Linea (59144)
    ///
    /// # Returns
    /// * `Ok(())` - Chains initialized or already exist
    /// * `Err(anyhow::Error)` - Database operation failed
    pub async fn init_chains_table(&self) -> Result<()> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chains")
            .fetch_one(&self.pool)
            .await?;

        if count == 0 {
            let chains = vec![
                (1, "ethereum"),
                (56, "binance-smart-chain"),
                (8453, "base"),
                (42161, "arbitrum-one"),
                (59144, "linea")
            ];

            for (chainid, name) in &chains {
                sqlx::query("INSERT INTO chains (chainid, name) VALUES ($1, $2)")
                    .bind(chainid)
                    .bind(name)
                    .execute(&self.pool)
                    .await?;
            }
            info!("✅ Inserted {} default chains", chains.len());
        } else {
            info!("Chains table already populated ({} chains)", count);
        }
        Ok(())
    }

    /// Adds a new blockchain to the chains table
    ///
    /// # Arguments
    /// * `chainid` - Chain ID (e.g., 1 for Ethereum mainnet)
    /// * `name` - Chain name (e.g., "ethereum")
    ///
    /// # Returns
    /// * `Ok(())` - Chain added successfully
    /// * `Err(anyhow::Error)` - Insert failed (possibly duplicate chainid)
    pub async fn add_chain(&self, chainid: i64, name: &str) -> Result<()> {
        sqlx::query("INSERT INTO chains (chainid, name) VALUES ($1, $2)")
            .bind(chainid)
            .bind(name)
            .execute(&self.pool)
            .await?;
        
        info!("✅ Added chain: {} ({})", name, chainid);
        Ok(())
    }

}

/// Application configuration
///
/// Contains all runtime configuration including database connections,
/// API keys, HTTP client, and feature flags.
#[derive(Clone)]
pub struct Config {
    /// PostgreSQL database connection manager
    pub postgres_db: PostgresDb,
    /// Management API key for admin operations
    pub manager_key: String,
    /// CoinGecko API key for market data
    pub coingecko_key: String,
    /// OpenExchangeRates API key for forex data
    pub openexchangerates_key: String,
    /// Shared HTTP client for all external API calls
    pub http_client: Client,
    /// Blockscout API endpoints by chain ID
    pub blockscout_endpoints: HashMap<i64, String>,
    /// Forex update interval in seconds
    pub forex_interval_secs: u64,
    /// Whether the system is in metadata initialization mode
    pub is_initializing_metadata: bool,
    /// Last processed token ID for incremental updates
    pub token_update_id: i64,
    /// Last processed NFT ID for incremental updates
    pub nft_update_id: i64,
    /// Shared rate limiter for CoinGecko API calls (28 requests per minute)
    /// This ensures all functions calling CoinGecko API share the same rate limit
    pub coingecko_rate_limiter: RateLimiter,
}

impl Config {
    /// Loads configuration from environment variables
    ///
    /// # Environment Variables Required
    /// - `MASTER_DATABASE_URL` - PostgreSQL connection string
    /// - `MANAGER_KEY` - Admin API key
    /// - `COINGECKO_KEY` - CoinGecko API key
    /// - `OPENEXCHANGERATES_KEY` - OpenExchangeRates API key
    ///
    /// # Environment Variables Optional
    /// - `IS_INITIALIZING_METADATA` - Boolean, defaults to `true`
    /// - `FOREX_INTERVAL_SECS` - Integer, defaults to `3600` (1 hour)
    ///
    /// # Panics
    /// Panics if any required environment variable is missing or invalid
    ///
    /// # Example
    /// ```no_run
    /// std::env::set_var("MASTER_DATABASE_URL", "postgres://localhost/db");
    /// let config = Config::from_env();
    /// ```
    pub fn from_env() -> Self {
        // Load .env file if it exists (development convenience)
        dotenvy::dotenv().ok();

        // Initialize Blockscout endpoints for supported chains
        let mut blockscout_endpoints = HashMap::new();
        blockscout_endpoints.insert(1, "https://eth.blockscout.com/api/v2/addresses".to_string());
        blockscout_endpoints.insert(42161, "https://arbitrum.blockscout.com/api/v2/addresses".to_string());
        blockscout_endpoints.insert(8453, "https://base.blockscout.com/api/v2/addresses".to_string());

        // Initialize database connection
        let postgres_db = PostgresDb::new(
            env::var("MASTER_DATABASE_URL").expect("MASTER_DATABASE_URL must be set"),
        );

        // Build HTTP client with optimized settings
        let client = Client::builder()
            .use_rustls_tls()
            .http2_keep_alive_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(10))
            .gzip(true)
            .brotli(true)
            .build()
            .expect("Failed to build reqwest HTTP client");

        // Parse optional configuration with defaults
        let is_initializing_metadata = env::var("IS_INITIALIZING_METADATA")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(true);

        let forex_interval_secs = env::var("FOREX_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3600);

        // Create shared rate limiter for CoinGecko API
        // 28 requests per minute to stay safely below the 30/min limit
        let coingecko_rate_limiter = RateLimiter::new(28, Duration::from_secs(60));

        Config {
            postgres_db,
            manager_key: env::var("MANAGER_KEY").expect("MANAGER_KEY must be set"),
            coingecko_key: env::var("COINGECKO_KEY").expect("COINGECKO_KEY must be set"),
            openexchangerates_key: env::var("OPENEXCHANGERATES_KEY")
                .expect("OPENEXCHANGERATES_KEY must be set"),
            http_client: client,
            blockscout_endpoints,
            forex_interval_secs,
            is_initializing_metadata,
            token_update_id: 0,
            nft_update_id: 0,
            coingecko_rate_limiter,
        }
    }

    /// Updates the database URL to a new primary database
    ///
    /// # Arguments
    /// * `new_url` - New PostgreSQL connection URL
    ///
    /// # Returns
    /// * `Ok(())` - Successfully switched to new database
    /// * `Err(sqlx::Error)` - Validation or connection failed
    pub async fn update_db_url(&mut self, new_url: String) -> Result<(), sqlx::Error> {
        self.postgres_db.update_primary_db_url(new_url).await
    }

    /// Adds or updates a Blockscout API endpoint for a specific chain
    ///
    /// # Arguments
    /// * `chainid` - Chain ID
    /// * `url` - Blockscout API base URL
    pub fn add_blockscout_endpoint(&mut self, chainid: i64, url: String) {
        info!("Adding blockscout endpoint: chain {} -> {}", chainid, &url);
        self.blockscout_endpoints.insert(chainid, url);
    }

    /// Sets the metadata initialization mode flag
    ///
    /// When `true`, the system will fetch metadata for all contracts.
    /// When `false`, only fetch metadata for new contracts.
    ///
    /// # Arguments
    /// * `is_initializing_metadata` - Whether to enable initialization mode
    pub fn set_is_initializing_metadata(&mut self, is_initializing_metadata: bool) {
        self.is_initializing_metadata = is_initializing_metadata;
        info!("Set is_initializing_metadata to {}", is_initializing_metadata);
    }

    /// Sets the forex interval in seconds
    pub fn set_forex_interval_secs(&mut self, interval_secs: u64) {
        self.forex_interval_secs = interval_secs;
        info!("Set forex_interval_secs to {}", interval_secs);
    }

    /// Updates the last processed token ID for incremental sync
    ///
    /// # Arguments
    /// * `id` - Last processed token ID (0 means start from beginning)
    pub fn set_token_update_id(&mut self, id: i64) {
        self.token_update_id = id;
        info!("Set token_update_id to {}", id);
    }

    /// Updates the last processed NFT ID for incremental sync
    ///
    /// # Arguments
    /// * `id` - Last processed NFT ID (0 means start from beginning)
    pub fn set_nft_update_id(&mut self, id: i64) {
        self.nft_update_id = id;
        info!("Set nft_update_id to {}", id);
    }
}

  