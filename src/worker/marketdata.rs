use serde::Deserialize;
use reqwest::StatusCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use sqlx::{Transaction, Postgres,Row};
use crate::config::Config;
use tracing::{info, warn, error};
use serde_json::Value;
use std::collections::HashSet;
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

/// 获取 Binance 日 K 线并写入 daily_price 表
pub async fn fetch_binance_daily_price(config: &Config) -> Result<StatusCode, StatusCode> {
    let pool = &config.postgres_db.pool;
    let client = &config.http_client;

    info!("Starting fetch_binance_daily_price task...");

    // 清空 daily_price 表
    sqlx::query("TRUNCATE TABLE daily_price")
        .execute(pool)
        .await
        .map_err(|e| {
            error!("Failed to truncate daily_price: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    info!("daily_price table truncated.");

    // 获取 symbol 列表
    let rows = sqlx::query(&format!(
        "SELECT symbol FROM marketdata LIMIT {}",
        config.max_token_indexed
    ))
    .fetch_all(pool)
    .await
    .map_err(|e| {
        error!("Failed to fetch symbols: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let symbols: Vec<String> = rows
        .into_iter()
        .map(|r| format!("{}USDT", r.get::<String, _>("symbol").to_uppercase()))
        .collect();

    // 计算 24 个月前时间戳 (ms)
    let start_time = SystemTime::now()
        .checked_sub(Duration::from_secs(24 * 30 * 24 * 3600)) // 粗略 24 个月
        .unwrap()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let mut total_success = 0;
    let mut total_skipped = 0;
    let mut total_failed = 0;

    for symbol in symbols.iter() {
        let mut attempts = 0;
        loop {
            attempts += 1;
            info!(symbol = %symbol, "Fetching Klines (attempt {})", attempts);

            match client
                .get("https://api3.binance.com/api/v3/klines")
                .query(&[
                    ("symbol", symbol),
                    ("interval", "1d"),
                    ("startTime", &start_time.to_string()),
                ])
                .send()
                .await
            {
                Ok(resp) => match resp.json::<Value>().await {
                    Ok(klines_json) => {
                        if klines_json.as_array().map(|a| !a.is_empty()).unwrap_or(false) {
                            // 写入数据库
                            if let Err(e) = sqlx::query(
                                r#"
                                INSERT INTO daily_price (symbol, data)
                                VALUES ($1, $2)
                                "#,
                            )
                            .bind(symbol)
                            .bind(&klines_json)
                            .execute(pool)
                            .await
                            {
                                warn!(symbol = %symbol, "Failed to insert daily_price JSON: {}", e);
                                total_failed += 1;
                            } else {
                                total_success += 1;
                            }
                        } else {
                            info!(symbol = %symbol, "No Kline data returned, skipping");
                            total_skipped += 1;
                        }
                        break;
                    }
                    Err(e) => {
                        warn!(symbol = %symbol, "Failed to parse JSON: {}", e);
                    }
                },
                Err(e) => {
                    warn!(symbol = %symbol, "Request failed: {}", e);
                }
            }

            if attempts >= 3 {
                error!(symbol = %symbol, "Max retries reached, skipping symbol");
                total_failed += 1;
                break;
            } else {
                let backoff = 1000u64 * attempts; // 每次重试等待 1s/2s/3s
                sleep(Duration::from_millis(backoff)).await;
            }
        }

        // 每个 symbol 间隔 3 秒
        sleep(Duration::from_secs(3)).await;
    }

    info!(
        "✅ fetch_binance_daily_price completed. Success: {}, Skipped: {}, Failed: {}",
        total_success, total_skipped, total_failed
    );

    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct ExchangeInfo {
    symbols: Vec<BinanceSymbol>,
}

#[derive(Deserialize)]
struct BinanceSymbol {
    symbol: String,
    status: String, // e.g. "TRADING", "BREAK"
}

/// 预处理函数：获取 Binance 支持的交易对 + 数据库清理
pub async fn preprocess_symbols(config: &Config) -> Result<Vec<String>> {
    let client = &config.http_client;
    let pool = &config.postgres_db.pool;

    // 1. 获取 Binance 所有交易对
    let url = "https://api.binance.com/api/v3/exchangeInfo";
    let resp: ExchangeInfo = client.get(url).send().await?.json().await?;

    let binance_symbols: HashSet<String> = resp
        .symbols
        .into_iter()
        .filter(|s| s.status == "TRADING") // 可选过滤
        .map(|s| s.symbol)
        .collect();

    info!("Fetched {} TRADING symbols from Binance", binance_symbols.len());

    // 2. 从数据库获取前 N 个 CoinGecko symbol
    let rows = sqlx::query("SELECT symbol FROM marketdata LIMIT $1")
        .bind(config.max_token_indexed as i64)
        .fetch_all(pool)
        .await?;

    let coingecko_symbols: Vec<String> = rows
        .into_iter()
        .map(|row| {
            let sym: String = row.get("symbol");
            format!("{}USDT", sym.to_uppercase())
        })
        .collect();

    info!("Loaded {} symbols from marketdata", coingecko_symbols.len());

    // 3. 过滤 Binance 上存在的交易对
    let supported_symbols: Vec<String> = coingecko_symbols
        .into_iter()
        .filter(|s| binance_symbols.contains(s))
        .collect();

    info!("Filtered {} supported symbols", supported_symbols.len());


    // 5. 清理不再支持的 symbol
    let db_rows = sqlx::query("SELECT symbol FROM fifteen_minute_price")
        .fetch_all(pool)
        .await?;

    let existing_symbols: HashSet<String> = db_rows
        .into_iter()
        .map(|row| row.get::<String, _>("symbol"))
        .collect();

    let supported_set: HashSet<String> = supported_symbols.iter().cloned().collect();
    let obsolete: Vec<String> = existing_symbols
        .difference(&supported_set)
        .cloned()
        .collect();

    if !obsolete.is_empty() {
        info!("Removing {} obsolete symbols", obsolete.len());
        sqlx::query("DELETE FROM fifteen_minute_price WHERE symbol = ANY($1)")
            .bind(&obsolete)
            .execute(pool)
            .await?;
    }

    Ok(supported_symbols)
}

#[derive(Deserialize)]
struct TickerPrice {
    symbol: String,
    price: String,
}
/// 批量更新所有 symbol 的价格
pub async fn update_all_symbol_prices(config: &Config) -> Result<()> {
    let client = &config.http_client;
    let pool = &config.postgres_db.pool;

    // 获取受支持的 symbols
    let supported_symbols = preprocess_symbols(config).await?;
    let total = supported_symbols.len();

    info!(task = "update_prices", status = "started", total_symbols = total);

    let mut success_count = 0;
    let mut fail_count = 0;

    // 每 100 个 symbol 一批
    for chunk in supported_symbols.chunks(100) {
        let url = "https://api.binance.com/api/v3/ticker/price";
        let resp = client
            .get(url)
            .query(&[("symbols", serde_json::to_string(&chunk)?)])
            .send()
            .await;

        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                error!(task = "update_prices", batch_size = chunk.len(), error = ?e);
                fail_count += chunk.len();
                continue;
            }
        };

        if !resp.status().is_success() {
            error!(task = "update_prices", batch_size = chunk.len(), status = ?resp.status());
            fail_count += chunk.len();
            continue;
        }

        let prices: Vec<TickerPrice> = match resp.json().await {
            Ok(p) => p,
            Err(e) => {
                error!(task = "update_prices", parse_error = ?e);
                fail_count += chunk.len();
                continue;
            }
        };

        // 写数据库
        for ticker in prices {
            let price: f64 = match ticker.price.parse() {
                Ok(p) => p,
                Err(_) => {
                    fail_count += 1;
                    continue;
                }
            };

            let now = chrono::Utc::now().timestamp_millis();

            // 查询已有数据
            let row = sqlx::query("SELECT prices FROM fifteen_minute_price WHERE symbol = $1")
                .bind(&ticker.symbol)
                .fetch_optional(pool)
                .await?;

            let mut history: Vec<[serde_json::Value; 2]> = if let Some(row) = row {
                let json: serde_json::Value = row.get("prices");
                serde_json::from_value(json)?
            } else {
                Vec::new()
            };

            history.push([json!(now), json!(price)]);

            // 保持 ≤ 288
            if history.len() > 288 {
                let start = history.len() - 288;
                history = history[start..].to_vec();
            }

            sqlx::query(
                r#"
                INSERT INTO fifteen_minute_price (symbol, prices, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (symbol) DO UPDATE
                SET prices = $2, updated_at = NOW()
                "#
            )
            .bind(&ticker.symbol)
            .bind(serde_json::to_value(history)?)
            .execute(pool)
            .await?;

            success_count += 1;
        }

        // 每批暂停 3 秒，避免触发限流
        sleep(Duration::from_secs(3)).await;
    }

    info!(
        task = "update_prices",
        status = "finished",
        total_symbols = total,
        success = success_count,
        failed = fail_count
    );

    Ok(())
}