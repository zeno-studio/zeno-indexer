use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use serde_json::Value;
use tokio::sync::RwLock;
use std::sync::Arc;

pub struct PostgresDb {
    pool: Arc<RwLock<Pool<Postgres>>>, // 当前写入的数据库连接池
    master_url: String,
    replica_url: String,
}

impl PostgresDb {
    pub async fn new(master_url: &str, replica_url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(master_url)
            .await?;
        Ok(Self {
            pool: Arc::new(RwLock::new(pool)),
            master_url: master_url.to_string(),
            replica_url: replica_url.to_string(),
        })
    }

    pub async fn switch_to_replica(&self) -> anyhow::Result<()> {
        let new_pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&self.replica_url)
            .await?;
        let mut pool = self.pool.write().await;
        *pool = new_pool;
        tracing::info!("Switched to replica database: {}", self.replica_url);
        Ok(())
    }

    pub async fn switch_to_master(&self) -> anyhow::Result<()> {
        let new_pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&self.master_url)
            .await?;
        let mut pool = self.pool.write().await;
        *pool = new_pool;
        tracing::info!("Switched to master database: {}", self.master_url);
        Ok(())
    }

    pub async fn insert_block(&self, block_number: i64, block_data: Value) -> anyhow::Result<()> {
        let pool = self.pool.read().await;
        sqlx::query(
            r#"
            INSERT INTO blocks (block_number, block_data)
            VALUES ($1, $2)
            ON CONFLICT (block_number) DO NOTHING
            "#,
        )
        .bind(block_number)
        .bind(block_data)
        .execute(&*pool)
        .await?;
        Ok(())
    }

    pub async fn insert_transaction(&self, tx_hash: &str, tx_data: Value) -> anyhow::Result<()> {
        let pool = self.pool.read().await;
        sqlx::query(
            r#"
            INSERT INTO transactions (tx_hash, tx_data)
            VALUES ($1, $2)
            ON CONFLICT (tx_hash) DO NOTHING
            "#,
        )
        .bind(tx_hash)
        .bind(tx_data)
        .execute(&*pool)
        .await?;
        Ok(())
    }

    pub async fn insert_event(&self, event_id: &str, event_data: Value) -> anyhow::Result<()> {
        let pool = self.pool.read().await;
        sqlx::query(
            r#"
            INSERT INTO events (event_id, event_data)
            VALUES ($1, $2)
            ON CONFLICT (event_id) DO NOTHING
            "#,
        )
        .bind(event_id)
        .bind(event_data)
        .execute(&*pool)
        .await?;
        Ok(())
    }
}