use anyhow::Result;
use axum::{
    Router,
    routing::{get, post},
};
use axum_server::tls_rustls::RustlsConfig;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, sleep, Instant};
use tracing::{error, info};
use tracing_loki::url::Url;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use std::process;


mod config;
mod manage;
mod worker;

use config::Config;
use manage::manager_rpc;
use worker::metadata::{
    fetch_token_metadata,
    sync_nftmap,
    sync_tokenmap,
    update_metadata_from_blockscout,
};
use worker::marketdata::sync_marketdata;

/// 顺序执行的一整条 pipeline：每天跑一次
async fn daily_sync_pipeline(cfg: Arc<RwLock<Config>>) {
    loop {
        let pipeline_start = Instant::now();

        {
            let start = Instant::now();
            let cfg = cfg.read().await;
            if let Err(e) = sync_tokenmap(&cfg).await {
                error!(error=?e, "❌ sync_tokenmap failed");
            } else {
                info!(elapsed=?start.elapsed(), "✅ sync_tokenmap finished");
            }
        }

        {
            let start = Instant::now();
            let cfg = cfg.read().await;
            if let Err(e) = sync_nftmap(&cfg).await {
                error!(error=?e, "❌ sync_nftmap failed");
            } else {
                info!(elapsed=?start.elapsed(), "✅ sync_nftmap finished");
            }
        }

        {
            let start = Instant::now();
            let cfg = cfg.read().await;
            if let Err(e) = fetch_token_metadata(&cfg).await {
                error!(error=?e, "❌ fetch_token_metadata failed");
            } else {
                info!(elapsed=?start.elapsed(), "✅ fetch_token_metadata finished");
            }
        }

        {
            let start = Instant::now();
            let cfg = cfg.read().await;
            if let Err(e) = update_metadata_from_blockscout(&cfg).await {
                error!(error=?e, "❌ update_metadata_from_blockscout failed");
            } else {
                info!(elapsed=?start.elapsed(), "✅ update_metadata_from_blockscout finished");
            }
        }

        info!(
            total_elapsed=?pipeline_start.elapsed(),
            "✅ daily sync pipeline finished, sleeping 24h..."
        );

        sleep(Duration::from_secs(24 * 3600)).await;
    }
}

/// 独立的市场数据同步：每天跑一次
async fn daily_marketdata(cfg: Arc<RwLock<Config>>) {
    loop {
        let start = Instant::now();
        {
            let cfg = cfg.read().await;
            if let Err(e) = sync_marketdata(&cfg).await {
                error!(error=?e, "❌ sync_marketdata failed");
            } else {
                info!(elapsed=?start.elapsed(), "✅ sync_marketdata finished");
            }
        }

        sleep(Duration::from_secs(24 * 3600)).await;
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv()?; // load env

    // Initialize rustls
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|e| format!("Failed to install rustls crypto provider: {:?}", e))?;

    // Initialize tracing-loki
    let (layer, task) = tracing_loki::builder()
        .label("service", "indexer")?
         .extra_field("pid", format!("{}", process::id()))?
        .build_url(Url::parse("http://127.0.0.1:3100").unwrap())?;

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(layer) 
        .init();

    tokio::spawn(task);

    info!("✅ tracing successfully set up");

    // Initialize config
    let config = Arc::new(RwLock::new(Config::from_env()));
    {
        let cfg = config.read().await;
        cfg.postgres_db.init_database().await.unwrap();
        cfg.postgres_db.init_chains_table().await?;
    }

    // 启动 pipeline 任务
    {
        let config_clone = Arc::clone(&config);
        tokio::spawn(async move {
            daily_sync_pipeline(config_clone).await;
        });
    }

    // 启动 marketdata 任务
    {
        let config_clone = Arc::clone(&config);
        tokio::spawn(async move {
            daily_marketdata(config_clone).await;
        });
    }

    // HTTP server
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/manager", post(manager_rpc))
        .with_state(Arc::clone(&config));

    let cert_path = env::var("TLS_CERT_PATH").unwrap_or_else(|_| "./cert.pem".to_string());
    let key_path = env::var("TLS_KEY_PATH").unwrap_or_else(|_| "./key.pem".to_string());

    let tls_config = RustlsConfig::from_pem_file(&cert_path, &key_path)
        .await
        .map_err(|e| {
            error!(
                "Failed to load TLS certificates: cert={}, key={}, error={}",
                cert_path, key_path, e
            );
            format!("Failed to load TLS certificates: {}", e)
        })?;

    axum_server::bind_rustls("0.0.0.0:8443".parse()?, tls_config)
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await?;

    Ok(())
}
