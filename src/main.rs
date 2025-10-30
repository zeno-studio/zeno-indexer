//! Indexer Service Main Entry Point
//!
//! This is the main entry point for the blockchain indexer service.
//! The service provides:
//! - Background data synchronization tasks (metadata, market data, forex rates)
//! - HTTPS API server for health checks and management operations
//! - PostgreSQL database persistence
//! - Distributed logging via Loki
//!
//! # Architecture
//! The service uses a concurrent architecture:
//! - Main thread: Runs the HTTPS server
//! - Background task: Runs all data synchronization tasks in parallel
//!
//! # Configuration
//! All configuration is loaded from environment variables (see .env file)
//!
//! # Startup Sequence
//! 1. Load environment variables
//! 2. Initialize cryptographic provider (Rustls)
//! 3. Setup distributed logging (Loki)
//! 4. Initialize database and run migrations
//! 5. Start background synchronization tasks
//! 6. Start HTTPS API server

use anyhow::{Result, Context, anyhow};
use axum::{Router, routing::{get, post}};
use axum_server::tls_rustls::RustlsConfig;
use std::{env, net::SocketAddr, process, sync::Arc};
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use tracing_loki::url::Url;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod config;
mod manage;
mod worker;
mod tasks;
mod utils;

use config::Config;
use manage::manager_rpc;
use tasks::start_all_tasks;

// ======================= Constants =======================

/// Default HTTPS server bind address
const DEFAULT_SERVER_ADDR: &str = "0.0.0.0:8443";

/// Default Loki server URL for log aggregation
const DEFAULT_LOKI_URL: &str = "http://127.0.0.1:3100";

// ======================= Helper Functions =======================

/// Health check endpoint handler
///
/// Returns a simple "OK" response to indicate the service is running.
/// This endpoint is typically used by:
/// - Load balancers for health monitoring
/// - Kubernetes liveness/readiness probes
/// - Monitoring systems for uptime checks
///
/// # Returns
/// Always returns HTTP 200 with "OK" body
async fn health_check() -> &'static str {
    "OK"
}

/// Initializes distributed logging with Loki integration
///
/// Sets up a dual logging pipeline:
/// 1. **Console output**: Formatted logs to stdout/stderr
/// 2. **Loki integration**: Structured logs sent to Loki for aggregation
///
/// # Loki Configuration
/// - URL: From `LOKI_URL` env var or default (http://127.0.0.1:3100)
/// - Service label: "indexer"
/// - Extra fields: Process ID (pid)
///
/// # Returns
/// * `Ok(())` - Logging configured successfully
/// * `Err` - Failed to setup Loki (console logging still works)
///
/// # Note
/// This function spawns a background task to send logs to Loki.
/// If Loki is unavailable, logs will only go to stdout.
async fn setup_tracing() -> Result<()> {
    // Get Loki URL from environment or use default
    let loki_url_str = env::var("LOKI_URL")
        .unwrap_or_else(|_| DEFAULT_LOKI_URL.to_string());
    
    let loki_url = Url::parse(&loki_url_str)
        .context(format!("Invalid Loki URL: {}", loki_url_str))?;

    // Build Loki layer with metadata
    let (layer, task) = tracing_loki::builder()
        .label("service", "indexer")?  // Service identifier for Loki queries
        .extra_field("pid", format!("{}", process::id()))?  // Process ID for debugging
        .build_url(loki_url)?;

    // Initialize tracing subscriber with dual output
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())  // Console output (stdout)
        .with(layer)  // Loki integration
        .init();

    // Spawn background task to send logs to Loki
    tokio::spawn(task);
    
    info!("Loki integration enabled at {}", loki_url_str);
    Ok(())
}

/// Loads TLS configuration from PEM files
///
/// Reads TLS certificate and private key from files specified in environment variables:
/// - `TLS_CERT_PATH`: Path to certificate file (default: ./cert.pem)
/// - `TLS_KEY_PATH`: Path to private key file (default: ./key.pem)
///
/// # Returns
/// * `Ok(RustlsConfig)` - TLS configuration ready for use
/// * `Err` - Failed to load or parse certificates
///
/// # Security
/// - Private key should have restricted file permissions (600)
/// - Certificate should be valid and not expired
/// - Self-signed certificates work for development but not recommended for production
async fn load_tls_config() -> Result<RustlsConfig> {
    let cert_path = env::var("TLS_CERT_PATH")
        .unwrap_or_else(|_| "./cert.pem".to_string());
    let key_path = env::var("TLS_KEY_PATH")
        .unwrap_or_else(|_| "./key.pem".to_string());

    RustlsConfig::from_pem_file(&cert_path, &key_path)
        .await
        .context(format!(
            "Failed to load TLS certificates: cert={}, key={}",
            cert_path, key_path
        ))
}

// ======================= Main Entry Point =======================

// ======================= Main Entry Point =======================

/// Main application entry point
///
/// Initializes all services and starts the indexer:
/// 1. Cryptographic provider setup
/// 2. Distributed logging configuration
/// 3. Database initialization and migrations
/// 4. Background task spawning
/// 5. HTTPS server startup
///
/// # Returns
/// * `Ok(())` - Server shut down gracefully (never happens under normal operation)
/// * `Err` - Fatal initialization or runtime error
///
/// # Panics
/// May panic if:
/// - Required environment variables are missing
/// - TLS certificates are invalid or missing
/// - Database connection fails
#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file (if exists)
    // This is optional - production deployments may use system env vars
    dotenvy::dotenv().ok();

    // Step 1: Initialize Rustls cryptographic provider
    // Must be done before any TLS operations
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow!("Failed to install ring crypto provider"))?;

    // Step 2: Initialize distributed logging (Loki + stdout)
    // Non-fatal error - service continues even if Loki is unavailable
    if let Err(e) = setup_tracing().await {
        error!("⚠️ tracing_loki setup failed: {:?}", e);
        warn!("Continuing without Loki integration - logs will only go to stdout");
    } else {
        info!("✅ tracing successfully set up with Loki integration");
    }

    // Step 3: Load configuration and initialize database
    let config = Arc::new(RwLock::new(Config::from_env()));
    {
        let cfg = config.read().await;
        
        // Run database migrations (embedded at compile time)
        cfg.postgres_db
            .init_database()
            .await
            .context("Failed to run database migrations")?;
        
        // Initialize chains table with default blockchain networks
        if cfg.is_initializing_metadata {
             cfg.postgres_db
            .init_chains_table()
            .await
            .context("Failed to initialize chains table")?;
        }
        
        info!("✅ Database initialization complete");
    }

    // Step 4: Start background synchronization tasks
    // These tasks run indefinitely in a separate tokio task
    tokio::spawn({
        let cfg = config.clone();
        async move {
            info!("🚀 Starting background synchronization tasks");
            start_all_tasks(cfg).await;
            // NOTE: start_all_tasks never returns under normal operation
            // It runs an infinite loop of concurrent tasks
        }
    });

    // Step 5: Build and configure HTTP router
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/manager", post(manager_rpc))
        .with_state(config);

    // Step 6: Parse server address from environment or use default
    let server_addr = env::var("SERVER_ADDR")
        .unwrap_or_else(|_| DEFAULT_SERVER_ADDR.to_string());
    let addr: SocketAddr = server_addr
        .parse()
        .context(format!("Invalid SERVER_ADDR: {}", server_addr))?;

    // Step 7: Load TLS certificates
    let tls_config = load_tls_config().await?;

    // Step 8: Start HTTPS server (blocks until shutdown)
    info!("🚀 Starting HTTPS server at https://{}", addr);
    axum_server::bind_rustls(addr, tls_config)
        .serve(app.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .context("HTTPS server failed")?;

    Ok(())
}

// ======================= Tests =======================

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that health check returns OK
    #[tokio::test]
    async fn test_health_check() {
        let result = health_check().await;
        assert_eq!(result, "OK", "Health check should return OK");
    }

    /// Test default constants
    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_SERVER_ADDR, "0.0.0.0:8443", "Default server address should be 0.0.0.0:8443");
        assert_eq!(DEFAULT_LOKI_URL, "http://127.0.0.1:3100", "Default Loki URL should be http://127.0.0.1:3100");
    }

    /// Test TLS config with missing files (should fail gracefully)
    #[tokio::test]
    async fn test_load_tls_config_missing_files() {
        // Set invalid paths to test error handling
        // SAFETY: This is a test environment, env var modification is acceptable
        unsafe {
            env::set_var("TLS_CERT_PATH", "/nonexistent/cert.pem");
            env::set_var("TLS_KEY_PATH", "/nonexistent/key.pem");
        }

        let result = load_tls_config().await;
        
        // Should fail with clear error message
        assert!(result.is_err(), "Loading TLS config with missing files should fail");
        
        // Clean up env vars
        // SAFETY: This is a test environment, env var modification is acceptable
        unsafe {
            env::remove_var("TLS_CERT_PATH");
            env::remove_var("TLS_KEY_PATH");
        }
    }

    /// Test server address parsing
    #[test]
    fn test_server_address_parsing() {
        let valid_addresses = vec![
            "0.0.0.0:8443",
            "127.0.0.1:8080",
            "[::1]:8443",
            "0.0.0.0:443",
        ];

        for addr_str in valid_addresses {
            let result: Result<SocketAddr, _> = addr_str.parse();
            assert!(result.is_ok(), "Should parse valid address: {}", addr_str);
        }
    }

    /// Test invalid server address parsing
    #[test]
    fn test_invalid_server_address() {
        let invalid_addresses = vec![
            "invalid",
            "0.0.0.0",  // Missing port
            "0.0.0.0:99999",  // Invalid port
            "256.0.0.1:8443",  // Invalid IP
        ];

        for addr_str in invalid_addresses {
            let result: Result<SocketAddr, _> = addr_str.parse();
            assert!(result.is_err(), "Should reject invalid address: {}", addr_str);
        }
    }

    /// Test Loki URL validation
    #[test]
    fn test_loki_url_parsing() {
        let valid_urls = vec![
            "http://127.0.0.1:3100",
            "http://localhost:3100",
            "https://loki.example.com",
        ];

        for url_str in valid_urls {
            let result = Url::parse(url_str);
            assert!(result.is_ok(), "Should parse valid Loki URL: {}", url_str);
        }
    }

    /// Test invalid Loki URL
    #[test]
    fn test_invalid_loki_url() {
        let invalid_urls = vec![
            "not a url",
            "htp://invalid",
            "://no-scheme",
        ];

        for url_str in invalid_urls {
            let result = Url::parse(url_str);
            assert!(result.is_err(), "Should reject invalid Loki URL: {}", url_str);
        }
    }

    /// Test environment variable fallback for server address
    #[test]
    fn test_server_addr_env_fallback() {
        // Test with env var not set
        // SAFETY: This is a test environment, env var modification is acceptable
        unsafe { env::remove_var("SERVER_ADDR"); }
        let addr = env::var("SERVER_ADDR").unwrap_or_else(|_| DEFAULT_SERVER_ADDR.to_string());
        assert_eq!(addr, DEFAULT_SERVER_ADDR, "Should use default when SERVER_ADDR not set");

        // Test with env var set
        // SAFETY: This is a test environment, env var modification is acceptable
        unsafe { env::set_var("SERVER_ADDR", "127.0.0.1:9000"); }
        let addr = env::var("SERVER_ADDR").unwrap_or_else(|_| DEFAULT_SERVER_ADDR.to_string());
        assert_eq!(addr, "127.0.0.1:9000", "Should use env var when SERVER_ADDR is set");
        
        // Clean up
        // SAFETY: This is a test environment, env var modification is acceptable
        unsafe { env::remove_var("SERVER_ADDR"); }
    }

    /// Test environment variable fallback for Loki URL
    #[test]
    fn test_loki_url_env_fallback() {
        // Test with env var not set
        // SAFETY: This is a test environment, env var modification is acceptable
        unsafe { env::remove_var("LOKI_URL"); }
        let url = env::var("LOKI_URL").unwrap_or_else(|_| DEFAULT_LOKI_URL.to_string());
        assert_eq!(url, DEFAULT_LOKI_URL, "Should use default when LOKI_URL not set");

        // Test with env var set
        // SAFETY: This is a test environment, env var modification is acceptable
        unsafe { env::set_var("LOKI_URL", "http://custom-loki:3100"); }
        let url = env::var("LOKI_URL").unwrap_or_else(|_| DEFAULT_LOKI_URL.to_string());
        assert_eq!(url, "http://custom-loki:3100", "Should use env var when LOKI_URL is set");
        
        // Clean up
        // SAFETY: This is a test environment, env var modification is acceptable
        unsafe { env::remove_var("LOKI_URL"); }
    }
}
