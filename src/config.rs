use std::env;
use sqlx::{PgPool, postgres::PgPoolOptions};
use crate::db::postgres::PostgresDb;


pub struct Config {
    pub postgres_db: PostgresDb, 
    pub eth_rpc_url: String,
    pub api_key: String,
    pub private_key: String,
    pub rest_api_url: String,
    pub contract_address: String,
    pub abi_path: String,
}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();
        let postgres_db = PostgresDb::new(env::var("MASTER_DATABASE_URL").expect("MASTER_DATABASE_URL must be set"));
        Config {
            postgres_db,
            eth_rpc_url: env::var("eth_rpc_url").expect("eth_rpc_url must be set"),
            api_key: env::var("API_KEY").expect("API_KEY must be set"),
            private_key: env::var("PRIVATE_KEY").expect("PRIVATE_KEY must be set"),
            rest_api_url: env::var("REST_API_URL").expect("REST_API_URL must be set"),
            contract_address: env::var("CONTRACT_ADDRESS").expect("CONTRACT_ADDRESS must be set"),
            abi_path: env::var("ABI_PATH").expect("ABI_PATH must be set"),
        }
    }
    pub async fn update_db_url(&mut self, new_url: String) -> Result<(), sqlx::Error> {
        self.postgres_db.update_primary_db_url(new_url).await
    }

    fn get_eth_rpc_url(&self) -> &str {
        &self.eth_rpc_url
    }

    fn get_rest_api_url(&self) -> &str {
        &self.rest_api_url
    }

}