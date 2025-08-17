use anyhow::Result;
use axum::{
    Router,
    routing::{get, post},
};
use axum_server::tls_rustls::RustlsConfig;
use ethers::signers::Signer;
use serde_json::json;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, sleep};
use tracing::{error, info};
use tracing_subscriber;

mod abi;
mod config;
mod manage;
mod processor;
mod provider;
mod subscription;
mod wallet;
mod worker;

use abi::parser::AbiParser;
use config::Config;
use manage::manager_rpc;
use provider::eth::EthProvider;
use wallet::local::Account;
use worker::metadata::{fetch_token_metadata, sync_nftmap, sync_tokenmap};

#[tokio::main]

async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initial enviroment variables
    dotenvy::dotenv()?;

    // Initialize rustls
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|e| format!("Failed to install rustls crypto provider: {:?}", e))?;

    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Create Config and wrap in Arc<RwLock<...>>
    let config = Arc::new(RwLock::new(Config::from_env()));
    {
        let cfg = config.read().await;
        cfg.postgres_db.init_database().await.unwrap();
    }
    // initialize database
    {
        let cfg = config.read().await;
        cfg.postgres_db.init_chains_table().await.unwrap();
    }

    {
        let config_clone = Arc::clone(&config);
        tokio::spawn(async move {
            loop {
                {
                    let cfg = config_clone.read().await;
                    if let Err(e) = sync_tokenmap(&cfg).await {
                        eprintln!("❌ sync_tokenmap failed: {e}");
                    }
                }
                sleep(Duration::from_secs(24 * 3600)).await; // 24小时
            }
        });
    }

    {
        let config_clone = Arc::clone(&config);
        tokio::spawn(async move {
            let delay_secs = 300;
            println!("⏳ First sync_nftmap will start in {delay_secs} seconds...");
            sleep(Duration::from_secs(delay_secs)).await;
            loop {
                {
                    let cfg = config_clone.read().await;
                    if let Err(e) = sync_nftmap(&cfg).await {
                        eprintln!("❌ sync_nftmap failed: {e}");
                    }
                }
                sleep(Duration::from_secs(24 * 3600)).await; // 每天跑一次
            }
        });
    }

    {
        let config_clone = Arc::clone(&config);
        tokio::spawn(async move {
            let delay_secs = 300;
            println!("⏳ First sync_nftmap will start in {delay_secs} seconds...");
            sleep(Duration::from_secs(delay_secs)).await;
            loop {
                {
                    let cfg = config_clone.read().await;
                    if let Err(e) = fetch_token_metadata(&cfg).await {
                        eprintln!("❌ fetch_token_metadata failed: {e}");
                    }
                }
                sleep(Duration::from_secs(24 * 3600)).await; // 每天跑一次
            }
        });
    }

    // Initialize EVM Provider
    let provider = EthProvider::new(&config.read().await.eth_rpc_url).await?;

    // Initialize ABI parser
    let abi_parser = AbiParser::new(&config.read().await.abi_path)?;

    let account = Account::from_private_key(&config.read().await.private_key);
    info!("Account address: {}", account.unwrap().wallet.address());

    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/manager", post(manager_rpc))
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
