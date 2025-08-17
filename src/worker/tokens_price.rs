use serde::Deserialize;
use reqwest::StatusCode;
use std::time::Duration;
use tokio::time::sleep;
use crate::config::Config;

#[derive(Deserialize)]
struct HotCoinGeckoToken {
    id: String,
    symbol: String,
    name: String,
    image: Option<String>,
    market_cap: Option<i64>,
    market_cap_rank: Option<i64>,
    total_supply: Option<f64>,
    max_supply: Option<f64>,
    ath: Option<i64>,
    ath_date: Option<String>,
    atl: Option<i64>,
    atl_date: Option<String>,
    last_updated: Option<String>,
}

pub async fn sync_hot_tokens(config: &Config) -> Result<(), StatusCode> {
    // Record sync start time for stale data cleanup
    let sync_start_time = chrono::Utc::now();

    // Fetch and process tokens
    for page in 1..=config.max_hot_token_page {
        let url = format!(
            "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=250&page={}",
            page
        );
        let tokens: Vec<HotCoinGeckoToken> = config
            .http_client
            .get(&url)
            .header("x-cg-demo-api-key", &config.coingecko_key)
            .header("Accept", "application/json")
            .query(&[("vs_currency", "usd"), ("per_page", "250")])
            .send()
            .await
            .map_err(|e| {
                println!("Failed to fetch page {}: {}", page, e);
                StatusCode::BAD_GATEWAY
            })?
            .json()
            .await
            .map_err(|e| {
                println!("Failed to parse JSON for page {}: {}", page, e);
                StatusCode::BAD_GATEWAY
            })?;

        // Break if no tokens returned
        if tokens.is_empty() {
            println!("No tokens on page {}, stopping", page);
            break;
        }

        // Use a transaction for batch updates
        let mut tx = config.postgres_db.pool.begin().await.map_err(|e| {
            println!("Failed to start transaction for page {}: {}", page, e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        for token in tokens {
            // Validate NOT NULL fields
            if token.symbol.is_empty() || token.name.is_empty() {
                println!("Token {} has empty symbol or name, skipping", token.id);
                continue;
            }

            // Convert float supplies to BIGINT (truncate decimals)
            let total_supply = token.total_supply.map(|v| v.trunc() as i64);
            let max_supply = token.max_supply.map(|v| v.trunc() as i64);

            sqlx::query(
                r#"
                INSERT INTO hot_tokens (
                    token_id, symbol, name, image, market_cap, market_cap_rank,
                    total_supply, max_supply, ath, ath_date, atl, atl_date, last_updated,
                    last_synced
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP)
                ON CONFLICT (token_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    name = EXCLUDED.name,
                    image = EXCLUDED.image,
                    market_cap = EXCLUDED.market_cap,
                    market_cap_rank = EXCLUDED.market_cap_rank,
                    total_supply = EXCLUDED.total_supply,
                    max_supply = EXCLUDED.max_supply,
                    ath = EXCLUDED.ath,
                    ath_date = EXCLUDED.ath_date,
                    atl = EXCLUDED.atl,
                    atl_date = EXCLUDED.atl_date,
                    last_updated = EXCLUDED.last_updated,
                    last_synced = EXCLUDED.last_synced
                "#,
            )
            .bind(&token.id)
            .bind(&token.symbol)
            .bind(&token.name)
            .bind(&token.image)
            .bind(&token.market_cap)
            .bind(&token.market_cap_rank)
            .bind(total_supply)
            .bind(max_supply)
            .bind(&token.ath)
            .bind(&token.ath_date)
            .bind(&token.atl)
            .bind(&token.atl_date)
            .bind(&token.last_updated)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                println!("Failed to insert/update token {}: {}", token.id, e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        }

        tx.commit().await.map_err(|e| {
            println!("Failed to commit transaction for page {}: {}", page, e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
        sleep(Duration::from_millis(300)).await; // Respect API rate limits
    }

    // Clean up stale tokens (not updated in this sync)
    sqlx::query("DELETE FROM hot_tokens WHERE last_synced < $1")
        .bind(sync_start_time)
        .execute(&config.postgres_db.pool)
        .await
        .map_err(|e| {
            println!("Failed to clean up stale tokens: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    println!("✅ sync_hot_tokens completed.");
    Ok(())
}