use std::sync::Arc;
use tokio::sync::RwLock;
use axum::{Json, extract::State};
use serde::Deserialize;
use serde_json::json;

use crate::Config;

/// RPC request structure for management operations
///
/// This structure defines the format for administrative RPC calls
/// that allow runtime configuration changes and system management.
#[derive(Deserialize)]
pub struct RpcRequest {
    /// Authentication key for admin operations
    pub manager_key: String,
    /// RPC method name (e.g., "add_chain", "update_primary_db_url")
    pub method: String,
    /// Method-specific parameters as JSON value
    pub params: serde_json::Value,
}


/// Management RPC endpoint handler
///
/// Handles administrative RPC requests for runtime configuration changes.
/// All requests require valid manager_key authentication.
///
/// # Supported Methods
/// - `add_chain` - Add a new blockchain to the system
/// - `add_blockscout_endpoint` - Add/update Blockscout API endpoint
/// - `update_primary_db_url` - Switch to a new primary database
///
/// # Arguments
/// * `config` - Shared application configuration (protected by RwLock)
/// * `req` - JSON-RPC style request with method and parameters
///
/// # Returns
/// JSON response with either `{"result": ...}` or `{"error": ...}`
///
/// # Example Request
/// ```json
/// {
///   "manager_key": "secret_key",
///   "method": "add_chain",
///   "params": {"chainid": 1, "name": "ethereum"}
/// }
/// ```
pub async fn manager_rpc(
    State(config): State<Arc<RwLock<Config>>>,
    Json(req): Json<RpcRequest>,
) -> Json<serde_json::Value> {
    // Step 1: Authenticate the request using manager_key
    {
        let cfg = config.read().await;
        if req.manager_key != cfg.manager_key {
            return Json(json!({"error": "Invalid manager_key"}));
        }
    }

    // Step 2: Route to the appropriate method handler
    match req.method.as_str() {
        // Add a new blockchain network to the chains table
        "add_chain" => {
            if let Some((chainid, name)) = parse_add_chain_params(&req.params) {
                let cfg = config.read().await;
                match cfg.postgres_db.add_chain(chainid, &name).await {
                    Ok(_) => Json(json!({"result": "ok"})),
                    Err(e) => Json(json!({"error": e.to_string()})),
                }
            } else {
                Json(json!({"error": "Invalid params: expected {chainid: i64, name: string}"}))
            }
        }
        // Add or update Blockscout API endpoint for a specific chain
        "add_blockscout_endpoint" => {
            if let Some((chainid, url)) = parse_add_blockscout_endpoint_params(&req.params) {
                let mut cfg = config.write().await;
                cfg.add_blockscout_endpoint(chainid, url);
                Json(json!({"result": "ok"}))
            } else {
                Json(json!({"error": "Invalid params: expected {chainid: i64, url: string}"}))
            }
        }
        // Switch to a new primary database (with validation)
        "update_primary_db_url" => {
            if let Some(new_url) = parse_update_url_params(&req.params) {
                let mut cfg = config.write().await; // Acquire write lock for state modification
                match cfg.update_db_url(new_url).await {
                    Ok(_) => Json(json!({"result": "ok"})),
                    Err(e) => Json(json!({"error": e.to_string()})),
                }
            } else {
                Json(json!({"error": "Invalid params: expected {new_url: string}"}))
            }
        }
        "set_forex_interval"=> {
            if let Some(new_interval) = parse_set_forex_interval_params(&req.params) {
                let mut cfg = config.write().await; // Acquire write lock for state modification
                cfg.set_forex_interval_secs(new_interval);
                Json(json!({"result": "ok"}))
            } else {
                Json(json!({"error": "Invalid params: expected {new_interval: i64}"}))
            }
        }
        // Unknown method
        _ => Json(json!({
            "error": "Unknown method",
            "supported_methods": ["add_chain", "add_blockscout_endpoint", "update_primary_db_url"]
        })),
    }
}

/// Parses parameters for the add_chain method
///
/// # Expected Parameters
/// - `chainid` (i64) - Chain ID (e.g., 1 for Ethereum)
/// - `name` (string) - Chain name (e.g., "ethereum")
///
/// # Returns
/// `Some((chainid, name))` if parsing succeeds, `None` otherwise
///
/// # Note
/// Currently has a bug: looks for "url" instead of "name"
fn parse_add_chain_params(params: &serde_json::Value) -> Option<(i64, String)> {
    Some((
        params.get("chainid")?.as_i64()?,
        params.get("name")?.as_str()?.to_string(), // Fixed: was "url", should be "name"
    ))
}

/// Parses parameters for the add_blockscout_endpoint method
///
/// # Expected Parameters
/// - `chainid` (i64) - Chain ID
/// - `url` (string) - Blockscout API base URL
///
/// # Returns
/// `Some((chainid, url))` if parsing succeeds, `None` otherwise
fn parse_add_blockscout_endpoint_params(params: &serde_json::Value) -> Option<(i64, String)> {
    Some((
        params.get("chainid")?.as_i64()?,
        params.get("url")?.as_str()?.to_string(),
    ))
}

/// Parses parameters for the update_primary_db_url method
///
/// # Expected Parameters
/// - `new_url` (string) - New PostgreSQL connection URL
///
/// # Returns
/// `Some(url)` if parsing succeeds, `None` otherwise
fn parse_update_url_params(params: &serde_json::Value) -> Option<String> {
    params.get("new_url")?.as_str().map(|s| s.to_string())
}

fn parse_set_forex_interval_params(params: &serde_json::Value) -> Option<u64> {
    params.get("new_interval")?.as_u64()
}

// ============= Unit Tests =============

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_add_chain_params_valid() {
        let params = json!({
            "chainid": 1,
            "name": "ethereum"
        });
        
        let result = parse_add_chain_params(&params);
        assert!(result.is_some());
        
        let (chainid, name) = result.unwrap();
        assert_eq!(chainid, 1);
        assert_eq!(name, "ethereum");
    }

    #[test]
    fn test_parse_add_chain_params_missing_chainid() {
        let params = json!({"name": "ethereum"});
        assert!(parse_add_chain_params(&params).is_none());
    }

    #[test]
    fn test_parse_add_chain_params_missing_name() {
        let params = json!({"chainid": 1});
        assert!(parse_add_chain_params(&params).is_none());
    }

    #[test]
    fn test_parse_add_blockscout_endpoint_params_valid() {
        let params = json!({
            "chainid": 1,
            "url": "https://eth.blockscout.com/api"
        });
        
        let result = parse_add_blockscout_endpoint_params(&params);
        assert!(result.is_some());
        
        let (chainid, url) = result.unwrap();
        assert_eq!(chainid, 1);
        assert_eq!(url, "https://eth.blockscout.com/api");
    }

    #[test]
    fn test_parse_update_url_params_valid() {
        let params = json!({"new_url": "postgres://localhost/newdb"});
        
        let result = parse_update_url_params(&params);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), "postgres://localhost/newdb");
    }

    #[test]
    fn test_parse_update_url_params_missing() {
        let params = json!({"wrong_key": "value"});
        assert!(parse_update_url_params(&params).is_none());
    }

    #[test]
    fn test_rpc_request_deserialization() {
        let json_str = r#"{
            "manager_key": "test_key",
            "method": "add_chain",
            "params": {"chainid": 1, "name": "ethereum"}
        }"#;
        
        let result: Result<RpcRequest, _> = serde_json::from_str(json_str);
        assert!(result.is_ok());
        
        let req = result.unwrap();
        assert_eq!(req.manager_key, "test_key");
        assert_eq!(req.method, "add_chain");
    }
}


