use serde_json::Value;
use sqlx::{PgPool, Row, postgres::PgPoolOptions};
use alloy::{
    providers::{Provider, ProviderBuilder},
    transports::http::reqwest::Url,
    pubsub::PubSubFrontend,
    sol_types::SolEvent,
    primitives::{Address, U256, B256},
    contract::{EventFilter, Log},
    sol,
};

#[derive(Clone)]
pub struct PostgresDb {
    pub primary_db_url: String,
    pool: PgPool,
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

 pub async fn write_transfer(
        &self,
        block_number: i64,
        tx_hash: B256,
        from: Address,
        to: Address,
        value: U256,
        contract_address: Address,
        timestamp: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO transfers (block_number, transaction_hash, from_address, to_address, value, contract_address, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, to_timestamp($7))
            "#,
        )
        .bind(block_number as i64)
        .bind(format!("{:?}", tx_hash))
        .bind(format!("{:?}", from))
        .bind(format!("{:?}", to))
        .bind(value.to_string())
        .bind(format!("{:?}", contract_address))
        .bind(timestamp)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

 pub async fn write_approval(
        &self,
        block_number: i64,
        tx_hash: B256,
        owner: Address,
        spender: Address,
        value: U256,
        contract_address: Address,
        timestamp: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO approvals (block_number, transaction_hash, owner_address, spender_address, value, contract_address, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, to_timestamp($7))
            "#,
        )
        .bind(block_number as i64)
        .bind(format!("{:?}", tx_hash))
        .bind(format!("{:?}", owner))
        .bind(format!("{:?}", spender))
        .bind(value.to_string())
        .bind(format!("{:?}", contract_address))
        .bind(timestamp)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

   pub async fn get_latest_block_number(&self) -> Result<Option<u64>, sqlx::Error> {
        // 从 transfers 或 approvals 表获取最新块号
        let transfer_block: Option<i64> = sqlx::query("SELECT MAX(block_number) FROM transfers")
            .fetch_optional(&self.pool)
            .await?
            .map(|row| row.get(0));
        let approval_block: Option<i64> = sqlx::query("SELECT MAX(block_number) FROM approvals")
            .fetch_optional(&self.pool)
            .await?
            .map(|row| row.get(0));

        Ok(match (transfer_block, approval_block) {
            (Some(t), Some(a)) => Some(t.max(a) as u64),
            (Some(t), None) => Some(t as u64),
            (None, Some(a)) => Some(a as u64),
            (None, None) => None,
        })
    }
}

sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);
    event Approval(address indexed owner, address indexed spender, uint256 value);
}