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

// 必须用 write().await
// 因为 update_primary_db_url 会修改 Config 里的 postgres_db（它包含 pool）。

// manager_key 验证在统一入口做
// 不必每个方法重复验证。

// RPC 兼容性
// 只要 handler 支持 RwLock<Config>，读操作用 read()，写操作用 write()，这个 RPC 风格就可以同时管理数据库和配置本身。

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
        "delete_chain" => {
            if let Some(chainid) = parse_delete_chain_params(&req.params) {
                let cfg = config.read().await;
                match cfg.postgres_db.delete_chain(chainid).await {
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
        params.get("name")?.as_str()?.to_string(),
    ))
}

fn parse_delete_chain_params(params: &serde_json::Value) -> Option<i64> {
    params.get("chainid")?.as_i64()
}

fn parse_update_url_params(params: &serde_json::Value) -> Option<String> {
    params.get("new_url")?.as_str().map(|s| s.to_string())
}


