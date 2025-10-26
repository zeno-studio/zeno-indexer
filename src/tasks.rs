//! Task Scheduler Module
//!
//! This module implements the main task scheduling logic for the indexer service.
//! It manages three primary background tasks:
//! - Metadata synchronization (daily)
//! - Market data synchronization (daily)
//! - Forex rate updates (configurable interval)
//!
//! All tasks run concurrently and independently, with automatic retry on failure.

use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant, sleep};
use tracing::{info, error};
use anyhow::Result;

use crate::config::Config;
use crate::worker::{
    forex::update_forex,
    marketdata::sync_marketdata,
    metadata::{fetch_token_metadata, fetch_nft_metadata, sync_nftmap, sync_tokenmap, update_metadata_from_blockscout},
};

// ======================= Constants =======================

/// Daily task interval in seconds (24 hours)
const DAILY_INTERVAL_SECS: u64 = 24 * 3600;

// ======================= Task Runner =======================

/// Generic safe task executor with error handling and timing
///
/// This wrapper function provides:
/// - Automatic error logging
/// - Execution time tracking
/// - Standardized success/failure messages
///
/// # Arguments
/// * `name` - Task name for logging purposes
/// * `task` - Async function to execute (must return Result<()>)
///
/// # Returns
/// * `true` - Task completed successfully
/// * `false` - Task failed with error
///
/// # Example
/// ```no_run
/// let success = safe_run("my_task", || async {
///     // Task implementation
///     Ok(())
/// }).await;
/// ```
pub async fn safe_run<F, Fut>(name: &str, task: F) -> bool
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send,
{
    let start = Instant::now();
    match task().await {
        Ok(_) => {
            info!(elapsed=?start.elapsed(), "✅ {} finished", name);
            true
        }
        Err(e) => {
            error!(error=?e, "❌ {} failed", name);
            false
        }
    }
}

// ======================= Metadata Task =======================

/// Daily metadata synchronization task
///
/// This task runs continuously with a 24-hour interval and performs:
/// 1. Token mapping synchronization from CoinGecko
/// 2. NFT mapping synchronization from CoinGecko
/// 3. Incremental token metadata fetch (new tokens only)
/// 4. Incremental NFT metadata fetch (new NFTs only)
/// 5. Contract verification data from Blockscout
///
/// # Workflow
/// - sync_tokenmap: Updates tokenmap table with latest token addresses
/// - sync_nftmap: Updates nftmap table with latest NFT collections
/// - fetch_token_metadata: Fetches metadata for new tokens (skips existing)
/// - fetch_nft_metadata: Fetches metadata for new NFTs (skips existing)
/// - update_metadata_from_blockscout: Enriches metadata with verification status
///
/// # Error Handling
/// Each sub-task is wrapped in `safe_run`, which logs errors but continues execution.
/// Individual task failures don't stop the pipeline.
///
/// # Arguments
/// * `cfg` - Shared configuration (wrapped in Arc<RwLock> for thread-safety)
///
/// # Note
/// - Uses write lock for metadata fetch tasks (to update progress tracking)
/// - Uses read lock for other tasks (read-only operations)
/// - Runs indefinitely until process termination
async fn metadata_task(cfg: Arc<RwLock<Config>>) {
    // Track if this is the first run (for initialization)
    let mut is_first_run = cfg.read().await.is_initializing_metadata;

    loop {
        let pipeline_start = Instant::now();
        
        // Track success of all steps in this iteration
        let mut all_steps_succeeded = true;

        // Step 1: Sync token mapping from CoinGecko API
        // Populates tokenmap table with token addresses across all chains
        all_steps_succeeded &= safe_run("sync_tokenmap", {
            let cfg = cfg.clone();
            move || async move {
                let cfg_read = cfg.read().await;
                sync_tokenmap(&*cfg_read).await
            }
        }).await;

        // Step 2: Sync NFT mapping from CoinGecko API
        // Populates nftmap table with NFT collection addresses
        all_steps_succeeded &= safe_run("sync_nftmap", {
            let cfg = cfg.clone();
            move || async move {
                let cfg_read = cfg.read().await;
                sync_nftmap(&*cfg_read).await
            }
        }).await;

        // Step 3: Fetch metadata for new tokens (incremental)
        // Uses write lock to update config.token_update_id for resume capability
        all_steps_succeeded &= safe_run("fetch_token_metadata", {
            let cfg = cfg.clone();
            move || async move {
                let mut cfg_write = cfg.write().await;
                fetch_token_metadata(&mut *cfg_write).await
            }
        }).await;

        // Step 4: Fetch metadata for new NFTs (incremental)
        // Uses write lock to update config.nft_update_id for resume capability
        all_steps_succeeded &= safe_run("fetch_nft_metadata", {
            let cfg = cfg.clone();
            move || async move {
                let mut cfg_write = cfg.write().await;
                fetch_nft_metadata(&mut *cfg_write).await
            }
        }).await;

        // Step 5: Update metadata with contract verification info from Blockscout
        // Enriches existing metadata with verification status and risk assessment
        all_steps_succeeded &= safe_run("update_metadata_from_blockscout", {
            let cfg = cfg.clone();
            move || async move {
                let cfg_read = cfg.read().await;
                update_metadata_from_blockscout(&*cfg_read).await
            }
        }).await;

        // Step 6: Mark initialization as complete ONLY if all steps succeeded
        // This ensures we don't incorrectly mark initialization as complete
        // when there were failures that need to be retried
        if is_first_run && all_steps_succeeded {
            let mut cfg_write = cfg.write().await;
            cfg_write.set_is_initializing_metadata(false);
            is_first_run = false;
            info!("🎉 Initial metadata synchronization completed successfully! Switching to incremental mode.");
        } else if is_first_run && !all_steps_succeeded {
            error!("⚠️ Initial metadata synchronization had failures. Will retry on next run.");
        }

        // Pipeline completed, sleep for 24 hours before next run
        info!(
            total_elapsed=?pipeline_start.elapsed(),
            "✅ daily metadata pipeline finished, sleeping {}s...",
            DAILY_INTERVAL_SECS
        );
        sleep(Duration::from_secs(DAILY_INTERVAL_SECS)).await;
    }
}

// ======================= Market Data Task =======================

/// Daily market data synchronization task
///
/// This task fetches and updates cryptocurrency market data including:
/// - Current prices
/// - Market capitalization
/// - Trading volume
/// - Price changes (24h, 7d, etc.)
///
/// # Workflow
/// Runs `sync_marketdata` which fetches data from CoinGecko API and updates
/// the marketdata table in PostgreSQL.
///
/// # Schedule
/// Runs once every 24 hours
///
/// # Error Handling
/// Failures are logged but don't stop the task loop. The task will retry
/// on the next scheduled run.
///
/// # Arguments
/// * `cfg` - Shared configuration (uses read lock for read-only access)
async fn marketdata_task(cfg: Arc<RwLock<Config>>) {
    loop {
        let start = Instant::now();

        // Fetch latest market data from CoinGecko for all tracked tokens
        safe_run("sync_marketdata", {
            let cfg = cfg.clone();
            move || async move {
                let cfg_read = cfg.read().await;
                sync_marketdata(&*cfg_read).await
            }
        }).await;

        // Sleep for 24 hours before next sync
        info!(
            elapsed=?start.elapsed(),
            "✅ daily marketdata finished, sleeping {}s...",
            DAILY_INTERVAL_SECS
        );
        sleep(Duration::from_secs(DAILY_INTERVAL_SECS)).await;
    }
}

// ======================= Forex Rate Task =======================

/// Foreign exchange rate synchronization task
///
/// This task updates forex exchange rates for currency conversion.
/// The update interval is configurable via `config.forex_interval_secs`.
///
/// # Workflow
/// Runs `update_forex` which fetches exchange rates from OpenExchangeRates API
/// and updates the forex_rates table.
///
/// # Schedule
/// Runs at configurable intervals (default: 1 hour)
/// Interval can be adjusted via `config.set_forex_interval_secs()`
///
/// # Use Case
/// Forex rates are used to convert token prices to different fiat currencies
/// for display and reporting purposes.
///
/// # Error Handling
/// Failures are logged but don't stop the task loop.
///
/// # Arguments
/// * `cfg` - Shared configuration (uses read lock to fetch interval setting)
async fn forex_task(cfg: Arc<RwLock<Config>>) {
    loop {
        let start = Instant::now();

        // Fetch latest forex exchange rates from OpenExchangeRates API
        safe_run("update_forex", {
            let cfg = cfg.clone();
            move || async move {
                let cfg_read = cfg.read().await;
                update_forex(&*cfg_read).await
            }
        }).await;

        // Get configurable sleep interval (allows runtime adjustment)
        let sleep_secs = cfg.read().await.forex_interval_secs;
        info!(
            elapsed=?start.elapsed(),
            "✅ forex update finished, sleeping {}s...",
            sleep_secs
        );
        sleep(Duration::from_secs(sleep_secs)).await;
    }
}

// ======================= Main Task Orchestrator =======================

/// Starts all background tasks concurrently
///
/// This is the main entry point for the task scheduler. It launches three
/// independent tasks that run in parallel:
/// 1. **metadata_task** - Daily metadata synchronization (24h interval)
/// 2. **marketdata_task** - Daily market data updates (24h interval)
/// 3. **forex_task** - Forex rate updates (configurable interval)
///
/// # Concurrency Model
/// Uses `tokio::join!` to run all tasks concurrently. All tasks are long-running
/// and will continue indefinitely until the process is terminated.
///
/// # Shared State
/// All tasks share the same `Config` instance wrapped in `Arc<RwLock<T>>`:
/// - Arc: Enables safe sharing across async tasks
/// - RwLock: Allows multiple readers or single writer (prevents deadlocks)
///
/// # Graceful Shutdown
/// This function never returns under normal operation. To stop the tasks,
/// the process should receive a termination signal (SIGTERM/SIGINT).
///
/// # Arguments
/// * `cfg` - Shared application configuration
///
/// # Example
/// ```no_run
/// use std::sync::Arc;
/// use tokio::sync::RwLock;
/// use crate::config::Config;
/// use crate::tasks::start_all_tasks;
///
/// #[tokio::main]
/// async fn main() {
///     let config = Arc::new(RwLock::new(Config::from_env()));
///     start_all_tasks(config).await; // Runs forever
/// }
/// ```
pub async fn start_all_tasks(cfg: Arc<RwLock<Config>>) {
    tokio::join!(
        metadata_task(cfg.clone()),
        marketdata_task(cfg.clone()),
        forex_task(cfg)
    );
}

// ======================= Tests =======================

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Test that safe_run properly handles successful tasks
    #[tokio::test]
    async fn test_safe_run_success() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let success = safe_run("test_task", move || async move {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;

        assert_eq!(counter.load(Ordering::SeqCst), 1, "Task should have executed once");
        assert!(success, "Task should return true on success");
    }

    /// Test that safe_run properly handles task failures
    #[tokio::test]
    async fn test_safe_run_failure() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let success = safe_run("test_task", move || async move {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            anyhow::bail!("Simulated error")
        })
        .await;

        // Task should have executed despite error
        assert_eq!(counter.load(Ordering::SeqCst), 1, "Task should have executed once even on error");
        assert!(!success, "Task should return false on failure");
    }

    /// Test that safe_run logs execution time
    #[tokio::test]
    async fn test_safe_run_timing() {
        use tokio::time::{sleep, Duration};

        let start = Instant::now();
        let success = safe_run("slow_task", || async {
            sleep(Duration::from_millis(100)).await;
            Ok(())
        })
        .await;

        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(100),
            "Task should have taken at least 100ms, took {:?}",
            elapsed
        );
        assert!(success, "Task should succeed");
    }

    /// Test constant values
    #[test]
    fn test_constants() {
        assert_eq!(DAILY_INTERVAL_SECS, 86400, "Daily interval should be 24 hours (86400 seconds)");
        assert_eq!(DAILY_INTERVAL_SECS, 24 * 3600, "Daily interval calculation should be correct");
    }

    /// Test that task functions accept Arc<RwLock<Config>>
    #[tokio::test]
    async fn test_config_sharing() {
        // This test verifies that the type signature is correct
        // We don't actually run the tasks as they loop forever
        
        use crate::config::Config;
        
        // Create a mock config (would need actual implementation)
        // This is a compile-time check to ensure Arc<RwLock<Config>> is accepted
        let _ensure_metadata_task_signature: fn(Arc<RwLock<Config>>) -> _ = metadata_task;
        let _ensure_marketdata_task_signature: fn(Arc<RwLock<Config>>) -> _ = marketdata_task;
        let _ensure_forex_task_signature: fn(Arc<RwLock<Config>>) -> _ = forex_task;
        let _ensure_start_all_tasks_signature: fn(Arc<RwLock<Config>>) -> _ = start_all_tasks;
    }

    /// Test safe_run with different task names
    #[tokio::test]
    async fn test_safe_run_multiple_tasks() {
        let results = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        for i in 0..3 {
            let results_clone = results.clone();
            safe_run(&format!("task_{}", i), move || async move {
                results_clone.lock().await.push(i);
                Ok(())
            })
            .await;
        }

        let results = results.lock().await;
        assert_eq!(results.len(), 3, "All three tasks should have executed");
        assert_eq!(*results, vec![0, 1, 2], "Tasks should execute in order");
    }

    /// Test safe_run error logging doesn't panic
    #[tokio::test]
    async fn test_safe_run_error_types() {
        // Test with different error types to ensure logging works
        let r1 = safe_run("io_error", || async {
            Err(anyhow::anyhow!("IO error"))
        })
        .await;
        assert!(!r1, "IO error should return false");

        let r2 = safe_run("parse_error", || async {
            Err(anyhow::anyhow!("Parse error"))
        })
        .await;
        assert!(!r2, "Parse error should return false");

        let r3 = safe_run("custom_error", || async {
            Err(anyhow::anyhow!("Custom error with details: {}", 42))
        })
        .await;
        assert!(!r3, "Custom error should return false");

        // If we reach here, error logging didn't panic
        assert!(true, "Error logging should not panic");
    }

    /// Test all steps success tracking
    #[tokio::test]
    async fn test_all_steps_success_tracking() {
        // Test that all steps must succeed for initialization to complete
        let step1 = safe_run("step1", || async { Ok(()) }).await;
        let step2 = safe_run("step2", || async { Ok(()) }).await;
        let step3 = safe_run("step3", || async { Ok(()) }).await;
        
        let all_succeeded = step1 && step2 && step3;
        assert!(all_succeeded, "All steps should succeed");
        
        // Test with one failure
        let step1 = safe_run("step1", || async { Ok(()) }).await;
        let step2 = safe_run("step2", || async { Err(anyhow::anyhow!("Failed")) }).await;
        let step3 = safe_run("step3", || async { Ok(()) }).await;
        
        let all_succeeded = step1 && step2 && step3;
        assert!(!all_succeeded, "Should not succeed if any step fails");
    }

    /// Test initialization flag behavior
    #[tokio::test]
    async fn test_initialization_flag() {
        use crate::config::Config;
        
        // Create config with initialization flag set to true
        let config = Arc::new(RwLock::new(Config::from_env()));
        
        // Verify initial state
        {
            let cfg = config.read().await;
            assert_eq!(cfg.is_initializing_metadata, true, "Should start with is_initializing_metadata = true");
        }
        
        // Simulate what metadata_task does after first run
        {
            let mut cfg = config.write().await;
            cfg.set_is_initializing_metadata(false);
        }
        
        // Verify flag was changed
        {
            let cfg = config.read().await;
            assert_eq!(cfg.is_initializing_metadata, false, "Should be false after initialization");
        }
    }
}
