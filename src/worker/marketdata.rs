use crate::config::Config;
use anyhow::{Context, Result};
use reqwest::Client;
use serde::Deserialize;
use sqlx::{Postgres, QueryBuilder, Transaction,Executor};
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

#[derive(Deserialize, Debug)]
pub struct MarketData {
    pub id: String,
    pub symbol: String,
    pub name: String,
    pub image: Option<String>,
    pub market_cap: Option<f64>,
    pub market_cap_rank: Option<i64>,
    pub fully_diluted_valuation: Option<f64>,
    pub price_change_24h: Option<f64>,
    pub price_change_percentage_24h: Option<f64>,
    pub circulating_supply: Option<f64>,
    pub total_supply: Option<f64>,
    pub max_supply: Option<f64>,
    pub ath: Option<f64>,
    pub ath_date: Option<String>,
    pub atl: Option<f64>,
    pub atl_date: Option<String>,
    pub last_updated: Option<String>,
}

/// 拉取一页数据
async fn fetch_tokens_page(client: &Client, api_key: &str, page: u32) -> Result<Vec<MarketData>> {
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
            .await
            .context("HTTP request failed")?;

        if !resp.status().is_success() {
            retries -= 1;
            if retries == 0 {
                anyhow::bail!("Failed to fetch page {}: HTTP {}", page, resp.status());
            }
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let tokens: Vec<MarketData> = resp
            .json()
            .await
            .context("Failed to parse JSON from CoinGecko")?;
        return Ok(tokens);
    }
}

/// 批量插入 tokens，使用 QueryBuilder 保证 Send-safe
async fn insert_bulk_tokens(
    tx: &mut Transaction<'_, Postgres>,
    tokens: &[MarketData],
) -> Result<()> {
    if tokens.is_empty() {
        return Ok(());
    }

    let mut qb = QueryBuilder::<Postgres>::new(
        "INSERT INTO marketdata (
            token_id, symbol, name, image, market_cap, market_cap_rank,
            fully_diluted_valuation, price_change_24h, price_change_percentage_24h,
            circulating_supply, total_supply, max_supply, ath, ath_date,
            atl, atl_date, last_updated
        ) ",
    );

    qb.push_values(tokens.iter(), |mut b, token| {
        b.push_bind(&token.id)
            .push_bind(&token.symbol)
            .push_bind(&token.name)
            .push_bind(&token.image)
            .push_bind(&token.market_cap)
            .push_bind(&token.market_cap_rank)
            .push_bind(&token.fully_diluted_valuation)
            .push_bind(&token.price_change_24h)
            .push_bind(&token.price_change_percentage_24h)
            .push_bind(&token.circulating_supply)
            .push_bind(&token.total_supply)
            .push_bind(&token.max_supply)
            .push_bind(&token.ath)
            .push_bind(&token.ath_date)
            .push_bind(&token.atl)
            .push_bind(&token.atl_date)
            .push_bind(&token.last_updated);
    });

    qb.build()
        .execute(&mut **tx)
        .await
        .context("Failed to insert marketdata in bulk")?;

    Ok(())
}

/// 主同步函数
pub async fn sync_marketdata(config: &Config) -> Result<()> {
    info!("🚀 sync_marketdata started");

    // 检查表是否存在
    sqlx::query("SELECT 1 FROM marketdata LIMIT 1")
        .fetch_optional(&config.postgres_db.pool)
        .await
        .context("marketdata table does not exist")?;

    let mut tx = config
        .postgres_db
        .pool
        .begin()
        .await
        .context("Failed to start transaction")?;

    // 清空表
    tx.execute("TRUNCATE TABLE marketdata")
        .await
        .context("Failed to truncate marketdata table")?;

    // 拉取分页
    let mut page = 1;
    loop {
        let tokens = fetch_tokens_page(&config.http_client, &config.coingecko_key, page)
            .await
            .with_context(|| format!("Failed to fetch page {}", page))?;

        if tokens.is_empty() {
            break;
        }

        insert_bulk_tokens(&mut tx, &tokens).await?;
        info!("Inserted page {}", page);

        page += 1;
        sleep(Duration::from_millis(300)).await; // Respect rate limit
    }

    tx.commit().await.context("Failed to commit transaction")?;

    let row_count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM marketdata")
        .fetch_one(&config.postgres_db.pool)
        .await
        .unwrap_or((0,));

    info!("✅ sync_marketdata finished, total rows: {}", row_count.0);
    Ok(())
}
