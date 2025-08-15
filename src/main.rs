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
mod indexer;
mod manage;
mod processor;
mod provider;
mod subscription;
mod wallet;

use abi::parser::AbiParser;
use config::Config;
use indexer::external::{fetch_token_metadata, sync_nftmap, sync_tokenmap};
use manage::{ManagerRouter, manager_rpc};
use provider::eth::EthProvider;
use wallet::local::Account;

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
    let config_clone = Arc::clone(&config);

    // initialize database
    {
        let cfg = config.read().await;
        cfg.postgres_db.init_chains().await?;
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

    //  // 启动后台任务
    // {
    //     let config_clone = Arc::clone(&config);
    //     tokio::spawn(async move {
    //         // 0~300秒随机延迟，避免所有节点同时请求
    //         let delay_secs = rand::thread_rng().gen_range(0..=300);
    //         println!("⏳ First sync_tokenmap will start in {delay_secs} seconds...");
    //         sleep(Duration::from_secs(delay_secs)).await;

    //         loop {
    //             {
    //                 let cfg = config_clone.read().await;
    //                 if let Err(e) = sync_tokenmap(&cfg).await {
    //                     eprintln!("❌ sync_tokenmap failed: {e}");
    //                 }
    //             }
    //             sleep(Duration::from_secs(24 * 3600)).await; // 24小时
    //         }
    //     });
    // }

    {
        let config_clone = Arc::clone(&config);
        tokio::spawn(async move {
            use rand::Rng;
            use tokio::time::{Duration, sleep};

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

    // Initialize EVM Provider
    let provider = EthProvider::new(&config_clone.read().await.eth_rpc_url).await?;

    // Initialize ABI parser
    let abi_parser = AbiParser::new(&config_clone.read().await.abi_path)?;

    // Initialize subscription manager

    let account = Account::from_private_key(&config_clone.read().await.private_key);
    info!("Account address: {}", account.unwrap().wallet.address());

    // 注册管理方法
    let router = ManagerRouter::new()
        .register("add_chain", |cfg, params| async move {
            if let (Some(chainid), Some(name)) = (
                params.get("chainid").and_then(|v| v.as_i64()),
                params.get("name").and_then(|v| v.as_str()),
            ) {
                let cfg = cfg.read().await;
                match cfg.postgres_db.add_chain(chainid, name).await {
                    Ok(_) => json!({"result": "ok"}),
                    Err(e) => json!({"error": e.to_string()}),
                }
            } else {
                json!({"error": "Invalid params"})
            }
        })
        .register("delete_chain", |cfg, params| async move {
            if let Some(chainid) = params.get("chainid").and_then(|v| v.as_i64()) {
                let cfg = cfg.read().await;
                match cfg.postgres_db.delete_chain(chainid).await {
                    Ok(_) => json!({"result": "ok"}),
                    Err(e) => json!({"error": e.to_string()}),
                }
            } else {
                json!({"error": "Invalid params"})
            }
        })
        .register("update_primary_db_url", |cfg, params| async move {
            if let Some(new_url) = params.get("new_url").and_then(|v| v.as_str()) {
                let mut cfg = cfg.write().await;
                match cfg.update_db_url(new_url.to_string()).await {
                    Ok(_) => json!({"result": "ok"}),
                    Err(e) => json!({"error": e.to_string()}),
                }
            } else {
                json!({"error": "Invalid params"})
            }
        });



    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/manager", post(manager_rpc))
        .with_state((Arc::clone(&config), router));

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
