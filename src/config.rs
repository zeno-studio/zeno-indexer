use std::env;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use reqwest::Client;
use tokio::time::Duration;
use anyhow::Result;


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
        // 健康检查：尝试连接新 URL

        let new_pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&new_url)
            .await?;

        // 检查是否为主节点（非恢复模式）
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

     pub async fn init_chains(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS chains (
                chainid BIGINT PRIMARY KEY,
                name TEXT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

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
            println!("Inserted default chains data.");
        }
        Ok(())
    }

    /// 添加一个链
    pub async fn add_chain(&self, chainid: i64, name: &str) -> Result<()> {
        sqlx::query("INSERT INTO chains (chainid, name) VALUES ($1, $2)")
            .bind(chainid)
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// 删除一个链
    pub async fn delete_chain(&self, chainid: i64) -> Result<()> {
        sqlx::query("DELETE FROM chains WHERE chainid = $1")
            .bind(chainid)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    pub async fn token_exists(&self, tokenid: &str, chainid: i64) -> Result<bool, sqlx::Error> {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM tokenmap WHERE tokenid = $1 AND chainid = $2 LIMIT 1"
        )
        .bind(tokenid)
        .bind(chainid)
        .fetch_optional(&self.pool)
        .await?;

        Ok(exists.is_some())
    }

    pub async fn nft_exists(&self, nftid: &str, chainid: i64) -> Result<bool, sqlx::Error> {
        let exists: Option<i64> = sqlx::query_scalar(
            "SELECT 1 FROM nftmap WHERE nftid = $1 AND chainid = $2 LIMIT 1"
        )
        .bind(nftid)
        .bind(chainid)
        .fetch_optional(&self.pool)
        .await?;

        Ok(exists.is_some())
    }

}


pub struct Config {
    pub postgres_db: PostgresDb, 
    pub eth_rpc_url: String,
    pub manager_key: String,
    pub chainbase_key: String,
    pub coingecko_key: String,
    pub private_key: String,
    pub contract_address: String,
    pub http_client: Client,
}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();
        let postgres_db = PostgresDb::new(env::var("MASTER_DATABASE_URL").expect("MASTER_DATABASE_URL must be set"));
        let client = Client::builder()
        .use_rustls_tls()
        .http2_keep_alive_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(10))
        .gzip(true) 
        .brotli(true)
        .build()
        .map_err(|e| format!("Failed to build reqwest client: {}", e)).unwrap();
        Config {
            postgres_db,
            eth_rpc_url: env::var("eth_rpc_url").expect("eth_rpc_url must be set"),
            manager_key: env::var("manager_key").expect("manager_key must be set"),
            chainbase_key: env::var("CHAINBASE_KEY").expect("CHAINBASE_KEY must be set"),
            coingecko_key: env::var("COINGECKO_KEY").expect("COINGECKO_KEY must be set"),
            private_key: env::var("PRIVATE_KEY").expect("PRIVATE_KEY must be set"),
            contract_address: env::var("CONTRACT_ADDRESS").expect("CONTRACT_ADDRESS must be set"),
            http_client: client,
        }
    }
    pub async fn update_db_url(&mut self, new_url: String) -> Result<(), sqlx::Error> {
        self.postgres_db.update_primary_db_url(new_url).await
    }
 

}

