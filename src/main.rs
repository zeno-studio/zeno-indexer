use axum::{
    Router,
    extract::State,
    middleware,
    routing::{get, post},
};
use axum_server::tls_rustls::RustlsConfig;
use ethers::signers::Signer;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{Level, error, info};
use tracing_subscriber;

mod abi;
mod api;
mod config;
mod db;
mod importer;
mod processor;
mod provider;
mod subscription;
mod wallet;

use crate::config::Config;
use abi::parser::AbiParser;
use api::handler::{api_key_auth, update_db_url};
use db::postgres::PostgresDb;
use importer::rest::RestImporter;
use provider::eth::EthProvider;
use subscription::{manager::SubscriptionManager, types::SubscriptionType};
use wallet::local::Account;

#[tokio::main]

async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv()?;

    // Initialize rustls
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|e| format!("Failed to install rustls crypto provider: {:?}", e))?;
    // 初始化日志
    tracing_subscriber::fmt::init();

    // 创建 Config 并包装在 Arc<RwLock<...>>
    let config = Arc::new(RwLock::new(Config::from_env()));
    let config_clone = Arc::clone(&config);

    // 初始化 EVM Provider
    let provider = EthProvider::new(&config_clone.read().await.eth_rpc_url).await?;

    // 初始化 ABI 解析器
    let abi_parser = AbiParser::new(&config_clone.read().await.abi_path)?;

    // 初始化订阅管理器
    let (sub_manager, _shutdown_rx) = SubscriptionManager::new(
        provider,
        config_clone.read().await.postgres_db.clone(),
        abi_parser,
    );
    let account = Account::from_private_key(&config_clone.read().await.private_key);
    info!("Account address: {}", account.unwrap().wallet.address());

    // 启动区块订阅
    tokio::spawn(async move {
        sub_manager
            .start(SubscriptionType::NewBlocks)
            .await
            .unwrap();
    });

    // 启动合约事件订阅（示例：监听 Transfer 事件）
    let contract_sub = SubscriptionType::ContractEvents {
        contract_address: config_clone.read().await.contract_address.clone(),
        event_name: "Transfer".to_string(),
    };
    tokio::spawn(async move {
        sub_manager.start(contract_sub).await.unwrap();
    });

    // 初始化 REST 导入器
    let importer = RestImporter::new(
        config_clone.read().await.rest_api_url.to_string(),
        config_clone.read().await.postgres_db.clone(),
    );
    tokio::spawn(async move {
        importer.start_import().await.unwrap();
    });

    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/switch_db", post(update_db_url))
        .route_layer(middleware::from_fn_with_state(
            Arc::clone(&config),
            api_key_auth,
        ))
        .with_state(Arc::clone(&config));

    // Load TLS certificates
    let cert_path = env::var("TLS_CERT_PATH").unwrap_or("./cert.pem".to_string());
    let key_path = env::var("TLS_KEY_PATH").unwrap_or("./key.pem".to_string());
    let tls_config = RustlsConfig::from_pem_file(&cert_path, &key_path)
        .await
        .map_err(|e| {
            error!(
                "Failed to load TLS certificates: cert={}, key={}, error={}",
                cert_path, key_path, e
            );
            format!("Failed to load TLS certificates: {}", e)
        })?;

    // Start HTTPS server
    axum_server::bind_rustls("0.0.0.0:8443".parse()?, tls_config)
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await?;

    Ok(())
}
