use anyhow::{Result, Context, anyhow};
use axum::{Router, routing::{get, post}};
use axum_server::tls_rustls::RustlsConfig;
use std::{env, net::SocketAddr, process, sync::Arc};
use tokio::sync::RwLock;
use tracing::{error, info};
use tracing_loki::url::Url;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod config;
mod manage;
mod worker;
mod tasks; 

use config::Config;
use manage::manager_rpc;
use tasks::{start_all_tasks};

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok(); // 安静加载 .env

    // ========= 初始化 Rustls =========
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow!("Failed to install ring crypto provider"))?;

    // ========= 初始化 tracing + Loki =========
    if let Err(e) = setup_tracing().await {
        error!("⚠️ tracing_loki setup failed: {:?}", e);
    } else {
        info!("✅ tracing successfully set up");
    }

    // ========= 加载配置 =========
    let config = Arc::new(RwLock::new(Config::from_env()));
    {
        let cfg = config.read().await;
        cfg.postgres_db.init_database().await.context("init_database failed")?;
        cfg.postgres_db.init_chains_table().await.context("init_chains_table failed")?;
    }

    // ========= 启动后台任务 =========
    {
        let cfg = config.clone();
        tokio::spawn(async move {
            start_all_tasks(cfg).await;
        });
    }
  
    // ========= 启动 HTTP 服务 =========
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/manager", post(manager_rpc))
        .with_state(Arc::clone(&config));

    let addr: SocketAddr = "0.0.0.0:8443".parse()?;
    let tls_config = load_tls_config().await?;

    info!("🚀 Starting HTTPS server at https://{addr}");
    axum_server::bind_rustls(addr, tls_config)
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .context("axum_server failed")?;

    Ok(())
}

/// 初始化 tracing-loki
async fn setup_tracing() -> Result<()> {
    let loki_url = Url::parse("http://127.0.0.1:3100").context("Invalid Loki URL")?;

    let (layer, task) = tracing_loki::builder()
        .label("service", "indexer")?
        .extra_field("pid", format!("{}", process::id()))?
        .build_url(loki_url)?;

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer()) // 本地输出
        .with(layer)
        .init();

    tokio::spawn(task);
    Ok(())
}

/// TLS 配置加载
async fn load_tls_config() -> Result<RustlsConfig> {
    let cert_path = env::var("TLS_CERT_PATH").unwrap_or_else(|_| "./cert.pem".to_string());
    let key_path = env::var("TLS_KEY_PATH").unwrap_or_else(|_| "./key.pem".to_string());

    RustlsConfig::from_pem_file(&cert_path, &key_path)
        .await
        .context(format!(
            "Failed to load TLS certificates: cert={}, key={}",
            cert_path, key_path
        ))
}

