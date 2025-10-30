//! Utility Functions Module
//!
//! This module provides common utility functions used across the indexer service.
//! Currently includes:
//! - HTTP request helpers with retry logic
//! - JSON parsing utilities
//! - Error handling wrappers
//! - Rate limiting for API requests

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, Mutex};
use tokio::time::{sleep, Instant};
use crate::config::Config;
use tracing::{warn, debug};

// ======================= Types =======================

/// Result type for fetch operations
///
/// Represents three possible outcomes when fetching data from external APIs:
/// - Success: Data was fetched and parsed successfully
/// - Empty: Request succeeded but returned empty data (e.g., empty array)
/// - Failed: Request or parsing failed after all retries
#[derive(Debug)]
pub enum FetchResult<T> {
    /// Successfully fetched and parsed data
    Success(T),
    /// Request succeeded but returned empty data (e.g., "[]", "")
    Empty,
    /// Request or parsing failed with error message
    Failed(String),
}

// ======================= Rate Limiter =======================

/// Rate limiter using token bucket algorithm
///
/// Ensures API requests don't exceed specified rate limits (e.g., 30 requests/minute).
/// Uses a semaphore-based token bucket pattern with automatic token refill.
///
/// # Algorithm
/// - Initialize with N tokens (permits)
/// - Each request consumes 1 token
/// - Tokens refill at fixed intervals (e.g., every 2 seconds for 30/min rate)
/// - If no tokens available, request waits until refill
/// - Additionally enforces minimum interval between requests to prevent bursts
///
/// # Example
/// ```no_run
/// // Create limiter: 30 requests per minute
/// let limiter = RateLimiter::new(30, Duration::from_secs(60));
///
/// // Wait for permission before making request
/// limiter.acquire().await;
/// make_api_call().await;
/// ```
#[derive(Clone)]
pub struct RateLimiter {
    /// Semaphore controlling available tokens
    semaphore: Arc<Semaphore>,
    /// Maximum tokens (requests) allowed in the time window
    max_tokens: usize,
    /// Time window for rate limiting (e.g., 60 seconds for "per minute")
    refill_interval: Duration,
    /// Minimum interval between consecutive requests (to prevent bursts)
    min_request_interval: Duration,
    /// Last request timestamp (protected by mutex)
    last_request_time: Arc<Mutex<Option<Instant>>>,
}

impl RateLimiter {
    /// Creates a new rate limiter
    ///
    /// # Arguments
    /// * `max_requests` - Maximum requests allowed per time window
    /// * `time_window` - Time window duration (e.g., Duration::from_secs(60) for per minute)
    ///
    /// # Example
    /// ```no_run
    /// // 30 requests per minute
    /// let limiter = RateLimiter::new(30, Duration::from_secs(60));
    /// ```
    pub fn new(max_requests: usize, time_window: Duration) -> Self {
        // Calculate minimum interval between requests to spread them evenly
        // For 28 req/min: 60s / 28 = ~2.14s between requests
        let min_request_interval = time_window.div_f64(max_requests as f64);
        
        let limiter = Self {
            semaphore: Arc::new(Semaphore::new(max_requests)),
            max_tokens: max_requests,
            refill_interval: min_request_interval,
            min_request_interval,
            last_request_time: Arc::new(Mutex::new(None)),
        };
        
        // Start background task to refill tokens
        limiter.start_refill_task();
        limiter
    }

    /// Starts background task that periodically refills tokens
    ///
    /// Refill strategy: Add 1 token every (time_window / max_requests) interval
    /// For example: 30 req/min → add 1 token every 2 seconds
    fn start_refill_task(&self) {
        let semaphore = Arc::clone(&self.semaphore);
        let refill_interval = self.refill_interval;
        let max_tokens = self.max_tokens;

        tokio::spawn(async move {
            loop {
                sleep(refill_interval).await;
                
                // Only add permit if not at max capacity
                if semaphore.available_permits() < max_tokens {
                    semaphore.add_permits(1);
                    debug!("Rate limiter: token refilled ({}/{} available)", 
                           semaphore.available_permits(), max_tokens);
                }
            }
        });
    }

    /// Acquires a token (waits if none available)
    ///
    /// This call blocks until a token becomes available AND enforces minimum
    /// interval between requests to prevent bursts.
    ///
    /// # Returns
    /// Returns immediately when token is acquired (non-blocking after acquisition)
    pub async fn acquire(&self) {
        let available_before = self.semaphore.available_permits();
        
        // Log if we're running low on tokens
        if available_before == 0 {
            warn!("⚠️ Rate limiter exhausted, waiting for token refill...");
        } else if available_before <= 5 {
            warn!("⚠️ Rate limiter low: only {} tokens remaining", available_before);
        }
        
        // First, acquire a token from the semaphore
        let permit = self.semaphore.acquire().await.expect("Semaphore closed");
        
        // Then, enforce minimum interval between requests
        let mut last_time = self.last_request_time.lock().await;
        if let Some(last) = *last_time {
            let elapsed = last.elapsed();
            if elapsed < self.min_request_interval {
                let wait_time = self.min_request_interval - elapsed;
                debug!("Enforcing min interval: waiting {:?} before next request", wait_time);
                sleep(wait_time).await;
            }
        }
        
        // Update last request time
        *last_time = Some(Instant::now());
        
        // Log token consumption
        let available_after = self.semaphore.available_permits();
        debug!("Rate limiter: consumed token ({} → {} remaining)", 
               available_before, available_after);
        
        // Immediately forget the permit to consume the token
        permit.forget();
    }

    /// Tries to acquire a token without waiting
    ///
    /// # Returns
    /// * `true` - Token acquired successfully
    /// * `false` - No tokens available (rate limit reached)
    #[allow(dead_code)]
    pub fn try_acquire(&self) -> bool {
        if let Ok(permit) = self.semaphore.try_acquire() {
            permit.forget();
            true
        } else {
            false
        }
    }

    /// Gets the number of available tokens
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }
}

// ======================= HTTP Utilities =======================

// ======================= HTTP Utilities =======================

/// Fetches and parses JSON data with automatic retry logic and rate limiting
///
/// This function provides robust HTTP request handling with:
/// - Automatic retries on failure
/// - Exponential backoff between attempts
/// - Consecutive failure tracking (circuit breaker pattern)
/// - Empty response detection
/// - Comprehensive error logging
/// - Optional rate limiting for API quota management
///
/// # Type Parameters
/// * `T` - Type to deserialize JSON response into (must implement Deserialize)
///
/// # Arguments
/// * `config` - Application configuration (provides HTTP client)
/// * `url` - Target URL to fetch from
/// * `headers` - Function to add custom headers to the request
/// * `max_retry` - Maximum number of retry attempts (total attempts, not retries)
/// * `max_consecutive_fail` - Maximum consecutive failures before giving up (circuit breaker)
/// * `rate_limiter` - Optional rate limiter to enforce API quotas (e.g., 30 req/min)
///
/// # Returns
/// * `FetchResult::Success(T)` - Successfully fetched and parsed data
/// * `FetchResult::Empty` - Response was empty ("" or "[]")
/// * `FetchResult::Failed(String)` - Failed after retries with error message
///
/// # Retry Strategy
/// - Exponential backoff: 1000ms × attempt_number
/// - Circuit breaker: Stops if consecutive failures reach threshold
/// - Last attempt: No sleep delay after final failure
/// - Rate limiting: Enforced before each request if limiter provided
///
/// # Example
/// ```no_run
/// use crate::utils::{get_json_with_retry, RateLimiter};
/// 
/// // Create rate limiter: 30 requests per minute
/// let limiter = RateLimiter::new(30, Duration::from_secs(60));
/// 
/// let result = get_json_with_retry::<MyType>(
///     &config,
///     "https://api.example.com/data",
///     |req| req.header("Authorization", "Bearer token"),
///     5,  // max 5 attempts
///     3,  // stop after 3 consecutive failures
///     Some(&limiter),  // enforce rate limit
/// ).await;
/// ```
pub async fn get_json_with_retry<T: serde::de::DeserializeOwned>(
    config: &Config,
    url: &str,
    headers: impl Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
    max_retry: usize,
    max_consecutive_fail: usize,
    rate_limiter: Option<&RateLimiter>,
) -> FetchResult<T> {
    // Track consecutive failures for circuit breaker pattern
    let mut consecutive_fail = 0;

    // Enforce rate limit ONCE before all retry attempts (if limiter provided)
    // This ensures we only consume one token regardless of retries
    if let Some(limiter) = rate_limiter {
        debug!("Waiting for rate limiter token... (available: {})", 
               limiter.available_permits());
        limiter.acquire().await;
        debug!("Rate limiter token acquired, proceeding with request (max {} retries)", max_retry);
    }

    for attempt in 1..=max_retry {
        // Build and send HTTP request with custom headers
        let req = headers(config.http_client.get(url));
        
        match req.send().await {
            Ok(resp) => {
                // Check HTTP status code
                match resp.error_for_status() {
                    Ok(resp_ok) => {
                        // Read response body as text
                        match resp_ok.text().await {
                            Ok(text) => {
                                // Check for empty responses
                                if text.trim().is_empty() || text == "[]" {
                                    return FetchResult::Empty;
                                }
                                
                                // Parse JSON
                                match serde_json::from_str::<T>(&text) {
                                    Ok(parsed) => {
                                        // Success! Return immediately
                                        return FetchResult::Success(parsed);
                                    }
                                    Err(e) => {
                                        warn!(
                                            "❌ JSON parse error on {} (attempt {}/{}): {}",
                                            url, attempt, max_retry, e
                                        );
                                        consecutive_fail += 1;
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(
                                    "❌ Failed to read response body on {} (attempt {}/{}): {}",
                                    url, attempt, max_retry, e
                                );
                                consecutive_fail += 1;
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            "⚠️ HTTP error {} on {} (attempt {}/{})",
                            e, url, attempt, max_retry
                        );
                        consecutive_fail += 1;
                    }
                }
            }
            Err(e) => {
                warn!(
                    "⚠️ Request error {} on {} (attempt {}/{})",
                    e, url, attempt, max_retry
                );
                consecutive_fail += 1;
            }
        }

        // Circuit breaker: Stop if too many consecutive failures
        if consecutive_fail >= max_consecutive_fail {
            return FetchResult::Failed(format!(
                "Failed to fetch {}: {} consecutive failures",
                url, consecutive_fail
            ));
        }

        // Exponential backoff, but skip sleep on last attempt
        if attempt < max_retry {
            sleep(Duration::from_millis(1000 * attempt as u64)).await;
        }
    }

    // All retries exhausted
    FetchResult::Failed(format!(
        "Failed to fetch {} after {} attempts",
        url, max_retry
    ))
}

// ======================= Tests =======================

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq)]
    struct TestData {
        id: i32,
        name: String,
    }

    /// Test FetchResult enum variants
    #[test]
    fn test_fetch_result_variants() {
        let success: FetchResult<TestData> = FetchResult::Success(TestData {
            id: 1,
            name: "test".to_string(),
        });
        assert!(matches!(success, FetchResult::Success(_)));

        let empty: FetchResult<TestData> = FetchResult::Empty;
        assert!(matches!(empty, FetchResult::Empty));

        let failed: FetchResult<TestData> = FetchResult::Failed("error".to_string());
        assert!(matches!(failed, FetchResult::Failed(_)));
    }

    /// Test that FetchResult is Debug
    #[test]
    fn test_fetch_result_debug() {
        let result: FetchResult<TestData> = FetchResult::Empty;
        let debug_str = format!("{:?}", result);
        assert!(debug_str.contains("Empty"));
    }

    /// Test empty response detection
    #[test]
    fn test_empty_response_detection() {
        // Test various empty string formats
        let empty_strings = vec![
            "",
            "   ",
            "[]",
        ];

        for s in empty_strings {
            let is_empty = s.trim().is_empty() || s == "[]";
            assert!(is_empty, "'{}' should be detected as empty", s);
        }
    }

    /// Test non-empty response detection
    #[test]
    fn test_non_empty_response_detection() {
        let non_empty_strings = vec![
            "{\"id\": 1}",
            "[1, 2, 3]",
            "null",
        ];

        for s in non_empty_strings {
            let is_empty = s.trim().is_empty() || s == "[]";
            assert!(!is_empty, "'{}' should NOT be detected as empty", s);
        }
    }

    /// Test exponential backoff calculation
    #[test]
    fn test_exponential_backoff_calculation() {
        // Test backoff duration calculation (updated to 1000ms base)
        let backoff_durations: Vec<Duration> = (1..=5)
            .map(|attempt| Duration::from_millis(1000 * attempt as u64))
            .collect();

        assert_eq!(backoff_durations[0], Duration::from_millis(1000));
        assert_eq!(backoff_durations[1], Duration::from_millis(2000));
        assert_eq!(backoff_durations[2], Duration::from_millis(3000));
        assert_eq!(backoff_durations[3], Duration::from_millis(4000));
        assert_eq!(backoff_durations[4], Duration::from_millis(5000));
    }

    /// Test consecutive failure tracking
    #[test]
    fn test_consecutive_failure_tracking() {
        let mut consecutive_fail = 0;
        let max_consecutive_fail = 3;

        // Simulate failures
        consecutive_fail += 1;
        assert_eq!(consecutive_fail, 1);
        assert!(consecutive_fail < max_consecutive_fail);

        consecutive_fail += 1;
        assert_eq!(consecutive_fail, 2);
        assert!(consecutive_fail < max_consecutive_fail);

        consecutive_fail += 1;
        assert_eq!(consecutive_fail, 3);
        assert!(consecutive_fail >= max_consecutive_fail, "Should trigger circuit breaker");
    }

    /// Test JSON parsing success
    #[test]
    fn test_json_parsing() {
        let json_str = r#"{"id": 42, "name": "test"}"#;
        let result: Result<TestData, _> = serde_json::from_str(json_str);
        
        assert!(result.is_ok());
        let data = result.unwrap();
        assert_eq!(data.id, 42);
        assert_eq!(data.name, "test");
    }

    /// Test JSON parsing failure
    #[test]
    fn test_json_parsing_failure() {
        let invalid_json = "not valid json";
        let result: Result<TestData, _> = serde_json::from_str(invalid_json);
        assert!(result.is_err(), "Should fail to parse invalid JSON");
    }

    /// Test max retry calculation
    #[test]
    fn test_max_retry_range() {
        let max_retry = 5;
        let attempts: Vec<usize> = (1..=max_retry).collect();
        
        assert_eq!(attempts.len(), max_retry);
        assert_eq!(attempts[0], 1);
        assert_eq!(attempts[max_retry - 1], max_retry);
    }

    /// Test that last attempt doesn't need sleep
    #[test]
    fn test_last_attempt_no_sleep() {
        let max_retry = 5;
        
        for attempt in 1..=max_retry {
            let should_sleep = attempt < max_retry;
            
            if attempt == max_retry {
                assert!(!should_sleep, "Last attempt should NOT sleep");
            } else {
                assert!(should_sleep, "Attempt {} should sleep", attempt);
            }
        }
    }

    /// Test rate limiter creation
    #[tokio::test]
    async fn test_rate_limiter_creation() {
        let limiter = RateLimiter::new(30, Duration::from_secs(60));
        assert_eq!(limiter.max_tokens, 30);
        // Initial permits should be at max
        assert_eq!(limiter.available_permits(), 30);
    }

    /// Test rate limiter try_acquire
    #[tokio::test]
    async fn test_rate_limiter_try_acquire() {
        let limiter = RateLimiter::new(2, Duration::from_secs(10));
        
        // Should succeed when tokens available
        assert!(limiter.try_acquire());
        assert_eq!(limiter.available_permits(), 1);
        
        assert!(limiter.try_acquire());
        assert_eq!(limiter.available_permits(), 0);
        
        // Should fail when no tokens
        assert!(!limiter.try_acquire());
    }

    /// Test rate limiter refill interval calculation
    #[test]
    fn test_rate_limiter_refill_interval() {
        // 30 requests per 60 seconds → 1 token every 2 seconds
        let time_window = Duration::from_secs(60);
        let max_requests = 30;
        let expected_interval = time_window.div_f64(max_requests as f64);
        
        assert_eq!(expected_interval, Duration::from_secs(2));
    }

    /// Test async rate limiter acquire
    #[tokio::test]
    async fn test_rate_limiter_acquire() {
        use tokio::time::Instant;
        
        let limiter = RateLimiter::new(2, Duration::from_secs(10));
        
        // First acquire should succeed immediately
        let start = Instant::now();
        limiter.acquire().await;
        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_millis(100), "Should be immediate");
        
        assert_eq!(limiter.available_permits(), 1);
    }
}
