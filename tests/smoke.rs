//! Smoke Tests for Environment Connectivity
//!
//! This test suite validates basic connectivity to external dependencies:
//! - PostgreSQL database connection
//! - CoinGecko API accessibility
//! - OpenExchangeRates API accessibility
//!
//! Run with: `RUN_SMOKE=1 cargo test --test smoke -- --ignored`
//! Or run all: `RUN_SMOKE=1 cargo test --test smoke`

use std::env;
use std::time::Duration;

/// Helper function to check if smoke tests should run
///
/// Tests are opt-in via RUN_SMOKE=1 environment variable
/// This prevents accidental execution in CI or normal test runs
fn should_run_smoke_test() -> bool {
    env::var("RUN_SMOKE").ok().as_deref() == Some("1")
}

/// Helper function to test database connectivity
async fn check_db_connectivity() {
    // Load .env file if exists
    dotenvy::dotenv().ok();

    // Get database URL
    let db_url = match env::var("MASTER_DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            println!("⚠️  MASTER_DATABASE_URL not set, skipping test");
            return;
        }
    };

    println!("🔌 Testing database connectivity...");

    // Create connection pool
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&db_url)
        .await
        .expect("Failed to connect to database");

    // Execute simple query
    let (result,): (i32,) = sqlx::query_as("SELECT 1")
        .fetch_one(&pool)
        .await
        .expect("Failed to execute test query");

    assert_eq!(result, 1, "Query should return 1");
    println!("✅ Database connectivity OK");
}

/// Test PostgreSQL database connectivity
///
/// Validates:
/// - Database URL is configured
/// - Connection pool can be created
/// - Simple query can be executed
///
/// # Environment Variables
/// - MASTER_DATABASE_URL: PostgreSQL connection string
#[tokio::test]
#[ignore] // Opt-in only via RUN_SMOKE=1
async fn test_db_connectivity() {
    if !should_run_smoke_test() {
        println!("⏭️  Skipping smoke test (set RUN_SMOKE=1 to enable)");
        return;
    }

    check_db_connectivity().await;
}

/// Helper function to test CoinGecko API connectivity
async fn check_coingecko_api_connectivity() {
    // Load .env file if exists
    dotenvy::dotenv().ok();

    println!("🔌 Testing CoinGecko API connectivity...");

    // Create HTTP client
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to build HTTP client");

    // Test ping endpoint
    let mut request = client.get("https://api.coingecko.com/api/v3/ping");

    // Add API key if available
    if let Ok(api_key) = env::var("COINGECKO_KEY") {
        request = request.header("x-cg-demo-api-key", api_key);
    }

    let response = request
        .send()
        .await
        .expect("Failed to send request to CoinGecko");

    assert!(
        response.status().is_success(),
        "CoinGecko API should respond with success status"
    );

    println!("✅ CoinGecko API connectivity OK");
}

/// Test CoinGecko API accessibility
///
/// Validates:
/// - CoinGecko API is reachable
/// - Ping endpoint responds successfully
///
/// # Environment Variables
/// - COINGECKO_KEY: (optional) CoinGecko API key
#[tokio::test]
#[ignore] // Opt-in only via RUN_SMOKE=1
async fn test_coingecko_api_connectivity() {
    if !should_run_smoke_test() {
        println!("⏭️  Skipping smoke test (set RUN_SMOKE=1 to enable)");
        return;
    }

    check_coingecko_api_connectivity().await;
}

/// Helper function to test OpenExchangeRates API connectivity
async fn check_openexchangerates_api_connectivity() {
    // Load .env file if exists
    dotenvy::dotenv().ok();

    // Get API key
    let app_id = match env::var("OPENEXCHANGERATES_KEY") {
        Ok(key) => key,
        Err(_) => {
            println!("⚠️  OPENEXCHANGERATES_KEY not set, skipping test");
            return;
        }
    };

    println!("🔌 Testing OpenExchangeRates API connectivity...");

    // Create HTTP client
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to build HTTP client");

    // Test latest rates endpoint
    let url = format!(
        "https://openexchangerates.org/api/latest.json?app_id={}",
        app_id
    );

    let response = client
        .get(&url)
        .send()
        .await
        .expect("Failed to send request to OpenExchangeRates");

    assert!(
        response.status().is_success(),
        "OpenExchangeRates API should respond with success status"
    );

    // Verify JSON response structure
    let json: serde_json::Value = response
        .json()
        .await
        .expect("Failed to parse JSON response");

    assert!(
        json.get("rates").is_some(),
        "Response should contain 'rates' field"
    );

    println!("✅ OpenExchangeRates API connectivity OK");
}

/// Test OpenExchangeRates API accessibility
///
/// Validates:
/// - OpenExchangeRates API is reachable
/// - API key is valid
/// - Latest rates endpoint responds
///
/// # Environment Variables
/// - OPENEXCHANGERATES_KEY: OpenExchangeRates API key (required)
#[tokio::test]
#[ignore] // Opt-in only via RUN_SMOKE=1
async fn test_openexchangerates_api_connectivity() {
    if !should_run_smoke_test() {
        println!("⏭️  Skipping smoke test (set RUN_SMOKE=1 to enable)");
        return;
    }

    check_openexchangerates_api_connectivity().await;
}

/// Helper function to test Blockscout API connectivity
async fn check_blockscout_api_connectivity() {
    println!("🔌 Testing Blockscout API connectivity...");

    // Create HTTP client
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to build HTTP client");

    // Test Ethereum mainnet Blockscout (use a known contract address)
    let test_address = "0xdac17f958d2ee523a2206206994597c13d831ec7"; // USDT on Ethereum
    let url = format!(
        "https://eth.blockscout.com/api/v2/addresses/{}",
        test_address
    );

    let response = client
        .get(&url)
        .send()
        .await
        .expect("Failed to send request to Blockscout");

    assert!(
        response.status().is_success(),
        "Blockscout API should respond with success status"
    );

    println!("✅ Blockscout API connectivity OK");
}

/// Test Blockscout API accessibility
///
/// Validates:
/// - Blockscout API is reachable for Ethereum mainnet
/// - API responds to valid requests
#[tokio::test]
#[ignore] // Opt-in only via RUN_SMOKE=1
async fn test_blockscout_api_connectivity() {
    if !should_run_smoke_test() {
        println!("⏭️  Skipping smoke test (set RUN_SMOKE=1 to enable)");
        return;
    }

    check_blockscout_api_connectivity().await;
}

/// Integration test: Run all connectivity tests
///
/// This runs all smoke tests in sequence and reports overall status
#[tokio::test]
async fn test_all_connectivity() {
    if !should_run_smoke_test() {
        println!("⏭️  Skipping smoke tests (set RUN_SMOKE=1 to enable)");
        println!("💡 Run with: RUN_SMOKE=1 cargo test --test smoke");
        return;
    }

    println!("\n🚀 Running comprehensive connectivity tests...\n");

    // Load environment
    dotenvy::dotenv().ok();

    // Test database
    check_db_connectivity().await;

    // Test APIs
    check_coingecko_api_connectivity().await;
    check_openexchangerates_api_connectivity().await;
    check_blockscout_api_connectivity().await;

    println!("\n✅ All connectivity tests passed!\n");
}
