use serde::Deserialize;
use reqwest::StatusCode;
use std::time::{Duration};
use tokio::time::sleep;
use sqlx::{Transaction, Postgres};
use crate::config::Config;
use tracing::{info};
use anyhow::Result;

#[derive(Deserialize, Debug)]
struct MarketData {
    id: String,
    symbol: String,
    name: String,
    image: Option<String>,
    market_cap: Option<f64>,                 
    market_cap_rank: Option<i64>,            
    fully_diluted_valuation: Option<f64>,
    price_change_24h: Option<f64>,
    price_change_percentage_24h: Option<f64>,
    circulating_supply: Option<f64>,
    total_supply: Option<f64>,
    max_supply: Option<f64>,
    ath: Option<f64>,
    ath_date: Option<String>,
    atl: Option<f64>,
    atl_date: Option<String>,
    last_updated: Option<String>,
}
/// 批量插入 tokens
async fn insert_bulk_tokens(
    tx: &mut Transaction<'_, Postgres>,
    tokens: Vec<MarketData>,
) -> Result<(), StatusCode> {
    if tokens.is_empty() {
        return Ok(());
    }

    let mut query = String::from(
        "INSERT INTO marketdata (
            token_id, symbol, name, image, market_cap, market_cap_rank,
            fully_diluted_valuation, price_change_24h, price_change_percentage_24h,
            circulating_supply, total_supply, max_supply, ath, ath_date,
            atl, atl_date, last_updated
        ) VALUES ",
    );

    let mut placeholders = Vec::new();
    let mut binders: Vec<
        Box<
            dyn FnOnce(
                sqlx::query::Query<'_, Postgres, sqlx::postgres::PgArguments>,
            ) -> sqlx::query::Query<'_, Postgres, sqlx::postgres::PgArguments>,
        >,
    > = Vec::new();

    for (i, token) in tokens.into_iter().enumerate() {
        if token.id.is_empty() || token.symbol.is_empty() || token.name.is_empty() {
            println!("Skipping invalid token: {:?}", token.id);
            continue;
        }

        let base = i * 17; // 一共有 17 个字段
        let ph: Vec<String> = (1..=17).map(|j| format!("${}", base + j)).collect();
        placeholders.push(format!("({})", ph.join(",")));

        let id = token.id;
        let symbol = token.symbol;
        let name = token.name;
        let image = token.image;
        let mc = token.market_cap;
        let mc_rank = token.market_cap_rank;
        let fdv = token.fully_diluted_valuation;
        let pc24 = token.price_change_24h;
        let pcp24 = token.price_change_percentage_24h;
        let cs = token.circulating_supply;
        let ts = token.total_supply;
        let ms = token.max_supply;
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
                .bind(fdv)
                .bind(pc24)
                .bind(pcp24)
                .bind(cs)
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

/// 从 coingecko 拉取一页数据，带重试
async fn fetch_tokens_page(
    client: &reqwest::Client,
    api_key: &str,
    page: u32,
) -> Result<Vec<MarketData>, StatusCode> {
    let url = format!(
        "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=250&page={}",
        page
    );

    let mut retries = 3;
    loop {
        let resp = client
            .get(&url)
            .header("x-cg-demo-api-key", api_key)
            .header("Accept", "application/json")
            .send()
            .await;

        match resp {
            Ok(r) => match r.json::<Vec<MarketData>>().await {
                Ok(tokens) => return Ok(tokens),
                Err(e) => {
                    println!("Failed to parse JSON for page {}: {}", page, e);
                    return Err(StatusCode::BAD_GATEWAY);
                }
            },
            Err(e) => {
                println!("Request error page {}: {}", page, e);
                retries -= 1;
                if retries == 0 {
                    return Err(StatusCode::BAD_GATEWAY);
                }
                println!("Retrying page {}... ({} retries left)", page, retries);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

pub async fn sync_marketdata(config: &Config) -> Result<(), StatusCode> {
    let start_time = chrono::Utc::now();
    info!("🚀 sync_marketdata started at {}", start_time);
    sqlx::query("SELECT 1 FROM marketdata LIMIT 1")
        .fetch_optional(&config.postgres_db.pool)
        .await
        .map_err(|e| {
            println!("marketdata table does not exist or is inaccessible: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut tx = config.postgres_db.pool.begin().await.map_err(|e| {
        println!("Failed to start transaction: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // reset table to empty state
    sqlx::query("TRUNCATE TABLE marketdata")
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            println!("Failed to truncate marketdata: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut page = 1;
    loop {
        let tokens = fetch_tokens_page(&config.http_client, &config.coingecko_key, page).await?;
        if tokens.is_empty() {
            println!("No tokens on page {}, stopping", page);
            break;
        }

        insert_bulk_tokens(&mut tx, tokens).await?;
        println!("Inserted page {}", page);

        page += 1;
        sleep(Duration::from_millis(300)).await; // Respect API rate limits
    }

    tx.commit().await.map_err(|e| {
        println!("Failed to commit transaction: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let row_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM marketdata")
        .fetch_one(&config.postgres_db.pool)
        .await
        .unwrap_or((0,));

    let end_time = chrono::Utc::now();
    info!("✅ sync_marketdata finished at {}, total rows: {}", end_time, row_count.0);

    Ok(())
}

