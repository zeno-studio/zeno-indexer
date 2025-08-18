use serde::Deserialize;
use reqwest::StatusCode;
use std::time::Duration;
use tokio::time::sleep;
use sqlx::{Transaction, Postgres};
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

/// 批量插入 tokens
async fn insert_bulk_tokens(
    tx: &mut Transaction<'_, Postgres>,
    tokens: Vec<HotCoinGeckoToken>,
) -> Result<(), StatusCode> {
    if tokens.is_empty() {
        return Ok(());
    }

    let mut query = String::from(
        "INSERT INTO hot_tokens (
            token_id, symbol, name, image, market_cap, market_cap_rank,
            total_supply, max_supply, ath, ath_date, atl, atl_date, last_updated
        ) VALUES ",
    );

    let mut placeholders = Vec::new();
    let mut binders: Vec<Box<dyn FnOnce(sqlx::query::Query<'_, Postgres, sqlx::postgres::PgArguments>)
        -> sqlx::query::Query<'_, Postgres, sqlx::postgres::PgArguments>>> = Vec::new();

    for (i, token) in tokens.into_iter().enumerate() {
        if token.id.is_empty() || token.symbol.is_empty() || token.name.is_empty() {
            println!("Skipping invalid token: {:?}", token.id);
            continue;
        }

        let total_supply = token.total_supply.map(|v| v.trunc() as i64);
        let max_supply = token.max_supply.map(|v| v.trunc() as i64);

        // 构造占位符 ($1, $2, ...)
        let base = i * 13;
        let ph: Vec<String> = (1..=13).map(|j| format!("${}", base + j)).collect();
        placeholders.push(format!("({})", ph.join(",")));

        // 捕获字段值，生成闭包来执行 bind
        let id = token.id;
        let symbol = token.symbol;
        let name = token.name;
        let image = token.image;
        let mc = token.market_cap;
        let mc_rank = token.market_cap_rank;
        let ts = total_supply;
        let ms = max_supply;
        let ath = token.ath;
        let ath_date = token.ath_date;
        let atl = token.atl;
        let atl_date = token.atl_date;
        let lu = token.last_updated;

        binders.push(Box::new(move |q| {
            q.bind(id)
                .bind(symbol)
                .bind(name)
                .bind(image)
                .bind(mc)
                .bind(mc_rank)
                .bind(ts)
                .bind(ms)
                .bind(ath)
                .bind(ath_date)
                .bind(atl)
                .bind(atl_date)
                .bind(lu)
        }));
    }

    if placeholders.is_empty() {
        return Ok(());
    }

    query.push_str(&placeholders.join(","));

    let mut q = sqlx::query(&query);
    for b in binders {
        q = b(q);
    }

    q.execute(&mut **tx)
        .await
        .map_err(|e| {
            println!("Failed to bulk insert tokens: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(())
}

pub async fn sync_hot_tokens(config: &Config) -> Result<(), StatusCode> {
    // Verify table exists
    sqlx::query("SELECT 1 FROM hot_tokens LIMIT 1")
        .fetch_optional(&config.postgres_db.pool)
        .await
        .map_err(|e| {
            println!("hot_tokens table does not exist or is inaccessible: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut tx = config.postgres_db.pool.begin().await.map_err(|e| {
        println!("Failed to start transaction: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // 清空表格
    sqlx::query("TRUNCATE TABLE hot_tokens")
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            println!("Failed to truncate hot_tokens: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    // 分页拉取并批量导入
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

        if tokens.is_empty() {
            println!("No tokens on page {}, stopping", page);
            break;
        }

        insert_bulk_tokens(&mut tx, tokens).await?;
        println!("Inserted page {}", page);

        sleep(Duration::from_millis(300)).await; // Respect API rate limits
    }

    tx.commit().await.map_err(|e| {
        println!("Failed to commit transaction: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    println!("✅ sync_hot_tokens completed (bulk overwrite).");
    Ok(())
}
