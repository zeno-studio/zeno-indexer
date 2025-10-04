use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant, sleep};
use tracing::{info, error};
use anyhow::Result;

use crate::config::Config;
use crate::worker::{
    forex::update_forex,
    marketdata::sync_marketdata,
    metadata::{fetch_token_metadata, sync_nftmap, sync_tokenmap, update_metadata_from_blockscout},
};

/// 通用安全任务执行器
pub async fn safe_run<F, Fut>(name: &str, task: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send,
{
    let start = Instant::now();
    match task().await {
        Ok(_) => info!(elapsed=?start.elapsed(), "✅ {} finished", name),
        Err(e) => error!(error=?e, "❌ {} failed", name),
    }
}

/// 每日元数据同步
async fn metadata_task(cfg: Arc<RwLock<Config>>) {
    loop {
        let pipeline_start = Instant::now();

        safe_run("sync_tokenmap", {
            let cfg = cfg.clone();
            move || async move {
                let cfg_read = cfg.read().await;
                sync_tokenmap(&*cfg_read).await
            }
        }).await;

        safe_run("sync_nftmap", {
            let cfg = cfg.clone();
            move || async move {
                let cfg_read = cfg.read().await;
                sync_nftmap(&*cfg_read).await
            }
        }).await;

        safe_run("fetch_token_metadata", {
            let cfg = cfg.clone();
            move || async move {
                let cfg_read = cfg.read().await;
                fetch_token_metadata(&*cfg_read).await
            }
        }).await;

        safe_run("update_metadata_from_blockscout", {
            let cfg = cfg.clone();
            move || async move {
                let cfg_read = cfg.read().await;
                update_metadata_from_blockscout(&*cfg_read).await
            }
        }).await;

        let sleep_secs = cfg.read().await.metadata_interval_secs;
        info!(total_elapsed=?pipeline_start.elapsed(), "✅ daily metadata pipeline finished, sleeping {}s...", sleep_secs);
        sleep(Duration::from_secs(sleep_secs)).await;
    }
}

/// 每日市场数据同步
async fn marketdata_task(cfg: Arc<RwLock<Config>>) {
    loop {
        let start = Instant::now();

        safe_run("sync_marketdata", {
            let cfg = cfg.clone();
            move || async move {
                let cfg_read = cfg.read().await;
                sync_marketdata(&*cfg_read).await
            }
        }).await;

        let sleep_secs = cfg.read().await.marketdata_interval_secs;
        info!(elapsed=?start.elapsed(), "✅ daily marketdata finished, sleeping {}s...", sleep_secs);
        sleep(Duration::from_secs(sleep_secs)).await;
    }
}

/// 每小时汇率同步
async fn forex_task(cfg: Arc<RwLock<Config>>) {
    loop {
        let start = Instant::now();

        safe_run("update_forex", {
            let cfg = cfg.clone();
            move || async move {
                let cfg_read = cfg.read().await;
                update_forex(&*cfg_read).await
            }
        }).await;

        let sleep_secs = cfg.read().await.forex_interval_secs;
        info!(elapsed=?start.elapsed(), "✅ hourly forex finished, sleeping {}s...", sleep_secs);
        sleep(Duration::from_secs(sleep_secs)).await;
    }
}

/// 启动所有任务
pub async fn start_all_tasks(cfg: Arc<RwLock<Config>>) {
    tokio::join!(
        metadata_task(cfg.clone()),
        marketdata_task(cfg.clone()),
        forex_task(cfg)
    );
}
