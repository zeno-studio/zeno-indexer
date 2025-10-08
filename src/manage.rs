use std::sync::Arc;
use tokio::sync::RwLock;
use axum::{Json, extract::State};
use serde::Deserialize;
use serde_json::json;

use crate::Config;

#[derive(Deserialize)]
pub struct RpcRequest {
    pub manager_key: String,
    pub method: String,
    pub params: serde_json::Value,
}


pub async fn manager_rpc(
    State(config): State<Arc<RwLock<Config>>>,
    Json(req): Json<RpcRequest>,
) -> Json<serde_json::Value> {
    // 1. 验证 manager_key
    {
        let cfg = config.read().await;
        if req.manager_key != cfg.manager_key {
            return Json(json!({"error": "Invalid manager_key"}));
        }
    }

    // 2. 调用对应方法
    match req.method.as_str() {
        "add_chain" => {
            if let Some((chainid, name)) = parse_add_chain_params(&req.params) {
                let cfg = config.read().await;
                match cfg.postgres_db.add_chain(chainid, &name).await {
                    Ok(_) => Json(json!({"result": "ok"})),
                    Err(e) => Json(json!({"error": e.to_string()})),
                }
            } else {
                Json(json!({"error": "Invalid params"}))
            }
        }
        "add blockscout endpoin"=> {
            if let Some((chainid, url)) = parse_add_blockscout_endpoint_params(&req.params) {
                let mut cfg = config.write().await;
                match cfg.add_blockscout_endpoint(chainid, url) {
                    Ok(_) => Json(json!({"result": "ok"})),
                    Err(e) => Json(json!({"error": e.to_string()})),
                }
            } else {
                Json(json!({"error": "Invalid params"}))
            }
        }
        "update_primary_db_url" => {
            if let Some(new_url) = parse_update_url_params(&req.params) {
                let mut cfg = config.write().await; // 可写锁
                match cfg.update_db_url(new_url).await {
                    Ok(_) => Json(json!({"result": "ok"})),
                    Err(e) => Json(json!({"error": e.to_string()})),
                }
            } else {
                Json(json!({"error": "Invalid params"}))
            }
        }
        _ => Json(json!({"error": "Unknown method"})),
    }
}

fn parse_add_chain_params(params: &serde_json::Value) -> Option<(i64, String)> {
    Some((
        params.get("chainid")?.as_i64()?,
        params.get("url")?.as_str()?.to_string(),
    ))
}

fn parse_add_blockscout_endpoint_params(params: &serde_json::Value) -> Option<(i64, String)> {
    Some((
        params.get("chainid")?.as_i64()?,
        params.get("url")?.as_str()?.to_string(),
    ))
}

fn parse_update_url_params(params: &serde_json::Value) -> Option<String> {
    params.get("new_url")?.as_str().map(|s| s.to_string())
}


