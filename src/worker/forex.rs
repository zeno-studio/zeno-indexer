use crate::config::Config;
use anyhow::Result;
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, warn};

/// Maximum number of retry attempts for API requests
const MAX_RETRY: u8 = 3;
/// Base delay in milliseconds between retries (will be multiplied by attempt number)
const RETRY_DELAY_MS: u64 = 300;

// ============= HTTP Fetch with Retry Logic =============

/// Fetches forex data from OpenExchangeRates API with exponential backoff retry
///
/// # Arguments
/// * `config` - Application configuration containing API key and HTTP client
///
/// # Returns
/// * `Ok(Value)` - JSON response from the API on success
/// * `Err(anyhow::Error)` - Error after all retry attempts are exhausted
///
/// # Retry Strategy
/// - Attempts up to MAX_RETRY times (default: 3)
/// - Uses exponential backoff: 300ms, 600ms between retries
/// - Does not wait after the final failed attempt
async fn get_forex_with_retry(config: &Config) -> Result<Value> {
    // Construct API URL with authentication key
    let api_url = format!(
        "https://openexchangerates.org/api/latest.json?app_id={}",
        config.openexchangerates_key
    );

    // Retry loop with exponential backoff
    for attempt in 1..=MAX_RETRY {
        // Send HTTP GET request
        match config.http_client.get(&api_url).send().await {
            Ok(resp) => {
                // Check if HTTP status is successful (2xx)
                match resp.error_for_status() {
                    Ok(resp_ok) => {
                        // Parse JSON response
                        match resp_ok.json::<Value>().await {
                            Ok(json) => {
                                // Success: return the parsed JSON data
                                return Ok(json);
                            }
                            Err(e) => {
                                warn!(
                                    "Failed to parse JSON (attempt {}/{}): {}",
                                    attempt, MAX_RETRY, e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        // HTTP error (4xx, 5xx status codes)
                        warn!(
                            "HTTP error (attempt {}/{}): {}",
                            attempt, MAX_RETRY, e
                        );
                    }
                }
            }
            Err(e) => {
                // Network error or request failed to send
                warn!(
                    "Request error (attempt {}/{}): {}",
                    attempt, MAX_RETRY, e
                );
            }
        }
        
        // Apply exponential backoff delay only if not the last attempt
        // This avoids unnecessary waiting before returning the final error
        if attempt < MAX_RETRY {
            sleep(Duration::from_millis(RETRY_DELAY_MS * attempt as u64)).await;
        }
    }

    // All retry attempts failed, return error
    Err(anyhow::anyhow!(
        "Failed to fetch forex data after {} attempts",
        MAX_RETRY
    ))
}

// ============= Forex Rate Update Function =============

/// Updates forex exchange rates in the database
///
/// This function performs an atomic replacement of all forex data:
/// 1. Fetches latest rates from OpenExchangeRates API
/// 2. Truncates the existing table
/// 3. Inserts new data with current timestamp

pub async fn update_forex(config: &Config) -> Result<()> {
    let pool = &config.postgres_db.pool;

    // Step 1: Fetch latest forex data from API (with retry logic)
    let forex_json = get_forex_with_retry(config).await?;

    // Step 2: Atomic database update using transaction
    let mut tx = pool.begin().await?;
    
    // Clear all existing forex data
    sqlx::query("TRUNCATE TABLE forex_rates")
        .execute(&mut *tx)
        .await?;
    
    // Insert new forex data (created_at will be set automatically by database)
    sqlx::query("INSERT INTO forex_rates (data) VALUES ($1)")
        .bind(&forex_json)
        .execute(&mut *tx)
        .await?;
    
    // Commit transaction - both operations succeed together
    tx.commit().await?;

    info!("✅ Forex data updated successfully.");
    Ok(())
}

