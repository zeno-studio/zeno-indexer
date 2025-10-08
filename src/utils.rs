use std::time::Duration;
use tokio::time::sleep;
use crate::config::Config;
use tracing::{warn};


#[derive(Debug)]
pub enum FetchResult<T> {
    Success(T),
    Empty,
    Failed(String),
}

pub async fn get_json_with_retry<T: serde::de::DeserializeOwned>(
    config: &Config,
    url: &str,
    headers: impl Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
    max_retry:usize
) -> FetchResult<T> {
    for attempt in 1..=max_retry {
        let req = headers(config.http_client.get(url));
        match req.send().await {
            Ok(resp) => match resp.error_for_status() {
                Ok(resp_ok) => {
                    // 尝试读取文本
                    match resp_ok.text().await {
                        Ok(text) => {
                            if text.trim().is_empty() || text == "[]" {
                                return FetchResult::Empty;
                            }
                            match serde_json::from_str::<T>(&text) {
                                Ok(parsed) => return FetchResult::Success(parsed),
                                Err(e) => {
                                    warn!(
                                        "❌ JSON parse error on {} (attempt {}/{}): {}",
                                        url, attempt, max_retry, e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                "❌ Failed to read response body on {} (attempt {}/{}): {}",
                                url, attempt, max_retry, e
                            );
                        }
                    }
                }
                Err(e) => warn!(
                    "⚠️ HTTP error {} on {} (attempt {}/{})",
                    e, url, attempt, max_retry
                ),
            },
            Err(e) => warn!(
                "⚠️ Request error {} on {} (attempt {}/{})",
                e, url, attempt, max_retry
            ),
        }

        sleep(Duration::from_millis(300 * attempt as u64)).await;
    }

    FetchResult::Failed(format!("Failed to fetch {} after {} attempts", url, max_retry))
}
