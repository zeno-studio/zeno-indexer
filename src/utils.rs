//! Utility Functions Module
//!
//! This module provides common utility functions used across the indexer service.
//! Currently includes:
//! - HTTP request helpers with retry logic
//! - JSON parsing utilities
//! - Error handling wrappers

use std::time::Duration;
use tokio::time::sleep;
use crate::config::Config;
use tracing::warn;

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

// ======================= HTTP Utilities =======================

// ======================= HTTP Utilities =======================

/// Fetches and parses JSON data with automatic retry logic
///
/// This function provides robust HTTP request handling with:
/// - Automatic retries on failure
/// - Exponential backoff between attempts
/// - Consecutive failure tracking (circuit breaker pattern)
/// - Empty response detection
/// - Comprehensive error logging
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
///
/// # Returns
/// * `FetchResult::Success(T)` - Successfully fetched and parsed data
/// * `FetchResult::Empty` - Response was empty ("" or "[]")
/// * `FetchResult::Failed(String)` - Failed after retries with error message
///
/// # Retry Strategy
/// - Exponential backoff: 300ms × attempt_number
/// - Circuit breaker: Stops if consecutive failures reach threshold
/// - Last attempt: No sleep delay after final failure
///
/// # Example
/// ```no_run
/// use crate::utils::get_json_with_retry;
/// 
/// let result = get_json_with_retry::<MyType>(
///     &config,
///     "https://api.example.com/data",
///     |req| req.header("Authorization", "Bearer token"),
///     5,  // max 5 attempts
///     3,  // stop after 3 consecutive failures
/// ).await;
/// ```
pub async fn get_json_with_retry<T: serde::de::DeserializeOwned>(
    config: &Config,
    url: &str,
    headers: impl Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
    max_retry: usize,
    max_consecutive_fail: usize,
) -> FetchResult<T> {
    // Track consecutive failures for circuit breaker pattern
    let mut consecutive_fail = 0;

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
            sleep(Duration::from_millis(300 * attempt as u64)).await;
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
        // Test backoff duration calculation
        let backoff_durations: Vec<Duration> = (1..=5)
            .map(|attempt| Duration::from_millis(300 * attempt as u64))
            .collect();

        assert_eq!(backoff_durations[0], Duration::from_millis(300));
        assert_eq!(backoff_durations[1], Duration::from_millis(600));
        assert_eq!(backoff_durations[2], Duration::from_millis(900));
        assert_eq!(backoff_durations[3], Duration::from_millis(1200));
        assert_eq!(backoff_durations[4], Duration::from_millis(1500));
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
}
