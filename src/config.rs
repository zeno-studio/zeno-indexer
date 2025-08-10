use std::env;

pub struct Config {
    pub master_database_url: String, // 主库用于写
    pub replica_database_url: String, // 备用库（故障时切换）
    pub eth_provider_url: String,
    pub private_key: String,
    pub rest_api_url: String,
    pub contract_address: String,
    pub abi_path: String,
}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();
        Config {
            master_database_url: env::var("MASTER_DATABASE_URL").expect("MASTER_DATABASE_URL must be set"),
            replica_database_url: env::var("REPLICA_DATABASE_URL").expect("REPLICA_DATABASE_URL must be set"),
            eth_provider_url: env::var("ETH_PROVIDER_URL").expect("ETH_PROVIDER_URL must be set"),
            private_key: env::var("PRIVATE_KEY").expect("PRIVATE_KEY must be set"),
            rest_api_url: env::var("REST_API_URL").expect("REST_API_URL must be set"),
            contract_address: env::var("CONTRACT_ADDRESS").expect("CONTRACT_ADDRESS must be set"),
            abi_path: env::var("ABI_PATH").expect("ABI_PATH must be set"),
        }
    }
}