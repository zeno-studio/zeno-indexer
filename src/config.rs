use anyhow::{Result, Context};
use reqwest::Client;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use std::collections::HashMap;
use std::env;
use tokio::time::Duration;
use tracing::{info, error};


#[derive(Clone)]
pub struct PostgresDb {
    pub primary_db_url: String,
    pub pool: PgPool,
}

impl PostgresDb {
    pub fn new(primary_db_url: String) -> Self {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_lazy(&primary_db_url)
            .unwrap();
        PostgresDb {
            primary_db_url,
            pool,
        }
    }

    pub async fn update_primary_db_url(&mut self, new_url: String) -> Result<(), sqlx::Error> {
        let new_pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&new_url)
            .await?;

        // 检查是否为主节点
        let is_in_recovery: bool = sqlx::query("SELECT pg_is_in_recovery()")
            .fetch_one(&new_pool)
            .await?
            .get::<bool, _>(0);
        if is_in_recovery {
            return Err(sqlx::Error::Configuration(
                "New URL is a replica, not a primary database".into(),
            ));
        }

        // 测试写入
        sqlx::query("CREATE TEMPORARY TABLE IF NOT EXISTS health_check (id SERIAL PRIMARY KEY)")
            .execute(&new_pool)
            .await?;
        sqlx::query("INSERT INTO health_check DEFAULT VALUES")
            .execute(&new_pool)
            .await?;
        sqlx::query("DROP TABLE health_check")
            .execute(&new_pool)
            .await?;

        self.primary_db_url = new_url;
        self.pool = new_pool;
        Ok(())
    }

    pub async fn init_database(&self) -> Result<()> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .context("Failed to execute database migrations")?;

        info!("✅ Database initialized successfully from schema.sql.");
        Ok(())
    }

    pub async fn init_chains_table(&self) -> Result<()> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM chains")
            .fetch_one(&self.pool)
            .await?;

        if count == 0 {
            let chains = vec![
                (1, "ethereum"),
                (10, "optimistic-ethereum"),
                (137, "polygon-pos"),
                (56, "binance-smart-chain"),
                (8453, "base"),
                (42161, "arbitrum-one"),
            ];

            for (chainid, name) in chains {
                sqlx::query("INSERT INTO chains (chainid, name) VALUES ($1, $2)")
                    .bind(chainid)
                    .bind(name)
                    .execute(&self.pool)
                    .await?;
            }
            info!("Inserted default chains data.");
        }
        Ok(())
    }

    pub async fn add_chain(&self, chainid: i64, name: &str) -> Result<()> {
        sqlx::query("INSERT INTO chains (chainid, name) VALUES ($1, $2)")
            .bind(chainid)
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn delete_chain(&self, chainid: i64) -> Result<()> {
        sqlx::query("DELETE FROM chains WHERE chainid = $1")
            .bind(chainid)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn contract_exists(&self, address: &str, chainid: i64) -> Result<bool, sqlx::Error> {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM metadata WHERE address = $1 AND chainid = $2 LIMIT 1",
        )
        .bind(address)
        .bind(chainid)
        .fetch_optional(&self.pool)
        .await?;

        Ok(exists.is_some())
    }
}

#[derive(Clone)]
pub struct Config {
    pub postgres_db: PostgresDb,
    pub manager_key: String,
    pub coingecko_key: String,
    pub openexchangerates_key: String,
    pub http_client: Client,
    pub max_token_indexed: i64,
    pub blockscout_endpoints: HashMap<i64, String>,
    pub metadata_interval_secs: u64,    // 默认 24*3600
    pub marketdata_interval_secs: u64,  // 默认 24*3600
    pub forex_interval_secs: u64,    
}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();

        let mut blockscout_endpoints = HashMap::new();
        blockscout_endpoints.insert(1, "https://eth.blockscout.com/api/v2/addresses".to_string());
        blockscout_endpoints.insert(10, "https://explorer.optimism.io/api/v2/addresses".to_string());
        blockscout_endpoints.insert(42161, "https://arbitrum.blockscout.com/api/v2/addresses".to_string());
        blockscout_endpoints.insert(8453, "https://base.blockscout.com/api/v2/addresses".to_string());
        blockscout_endpoints.insert(137, "https://polygon.blockscout.com/api/v2/addresses".to_string());

        let postgres_db = PostgresDb::new(
            env::var("MASTER_DATABASE_URL").expect("MASTER_DATABASE_URL must be set"),
        );
        let client = Client::builder()
            .use_rustls_tls()
            .http2_keep_alive_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(10))
            .gzip(true)
            .brotli(true)
            .build()
            .map_err(|e| format!("Failed to build reqwest client: {}", e))
            .unwrap();

        Config {
            postgres_db,
            manager_key: env::var("MANAGER_KEY").expect("MANAGER_KEY must be set"),
            coingecko_key: env::var("COINGECKO_KEY").expect("COINGECKO_KEY must be set"),
            openexchangerates_key: env::var("OPENEXCHANGERATES_KEY").expect("OPENEXCHANGERATES must be set"),
            metadata_interval_secs: env::var("METADATA_INTERVAL_SECS")
                .expect("METADATA_INTERVAL_SECS must be set")
                .parse()
                .unwrap_or(24 * 3600),
            marketdata_interval_secs: env::var("MARKETDATA_INTERVAL_SECS")
                .expect("MARKETDATA_INTERVAL_SECS must be set")
                .parse()
                .unwrap_or(24 * 3600),
            forex_interval_secs: env::var("FOREX_INTERVAL_SECS")
                .expect("FOREX_INTERVAL_SECS must be set")
                .parse()
                .unwrap_or(3600),
            http_client: client,
            max_token_indexed: env::var("MAX_TOKEN_INDEXED")
                .expect("MAX_TOKEN_INDEXED must be set")
                .parse()
                .unwrap_or(1000),
            blockscout_endpoints,
        }
    }

    pub async fn update_db_url(&mut self, new_url: String) -> Result<(), sqlx::Error> {
        self.postgres_db.update_primary_db_url(new_url).await
    }

    /// 添加一个新的 blockscout endpoint
    pub fn add_blockscout_endpoint(&mut self, chainid: i64, url: &str) {
        self.blockscout_endpoints.insert(chainid, url.to_string());
        info!("Added blockscout endpoint: chain {} -> {}", chainid, url);
    }

  
}

  