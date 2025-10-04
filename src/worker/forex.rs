use crate::config::Config;
use anyhow::Result;
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};

const MAX_RETRY: u8 = 3;

// ============= HTTP 获取（带重试） =============
async fn get_json_with_retry(
    config: &Config,
    headers: impl Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
) -> Result<Value> {
    let api_url = format!(
        "https://openexchangerates.org/api/latest.json?app_id={}",
        config.openexchangerates_key
    );

    for attempt in 1..=MAX_RETRY {
        let req = headers(config.http_client.get(&api_url));
        match req.send().await {
            Ok(resp) => match resp.error_for_status() {
                Ok(resp_ok) => {
                    let json = resp_ok.json::<Value>().await?;
                    return Ok(json);
                }
                Err(e) => warn!(
                    "HTTP error {} on {} (attempt {}/{})",
                    e, api_url, attempt, MAX_RETRY
                ),
            },
            Err(e) => warn!(
                "Request error {} on {} (attempt {}/{})",
                e, api_url, attempt, MAX_RETRY
            ),
        }
        sleep(Duration::from_millis(300 * attempt as u64)).await;
    }

    Err(anyhow::anyhow!(
        "Failed to fetch {} after {} attempts",
        &api_url,
        MAX_RETRY
    ))
}

// ============= 更新汇率 =============
pub async fn update_forex(config: &Config) -> Result<()> {
    let pool = &config.postgres_db.pool;

    // 1️⃣尝试从 API 获取数据
    let forex_json = match get_json_with_retry(config, |req| req).await {
        Ok(data) => data,
        Err(e) => {
            error!("❌ Failed to fetch forex data: {}", e);
            return Ok(()); // 保留旧数据，不清空表
        }
    };

    // 2️⃣ 插入或覆盖旧数据（仅当成功时）
    let mut tx = pool.begin().await?;
    sqlx::query("TRUNCATE TABLE forex_rates")
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO forex_rates (data) VALUES ($1)")
        .bind(forex_json)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    info!("✅ Forex data updated successfully.");
    Ok(())
}
