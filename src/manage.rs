use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use tokio::sync::RwLock;
use axum::{
    extract::{State, Json},
    http::HeaderMap,
};
use serde::Deserialize;
use serde_json::{json, Value};
use anyhow::Result;

use crate::Config;

pub type HandlerFuture = Pin<Box<dyn Future<Output = Value> + Send>>;
pub type HandlerFn = Arc<dyn Fn(Arc<RwLock<Config>>, Value) -> HandlerFuture + Send + Sync>;

#[derive(Clone)]
pub struct ManagerRouter {
    pub handlers: Arc<HashMap<String, HandlerFn>>,
}

impl ManagerRouter {
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(HashMap::new()),
        }
    }

    pub fn register<F, Fut>(mut self, name: &str, f: F) -> Self
    where
        F: Fn(Arc<RwLock<Config>>, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Value> + Send + 'static,
    {
        Arc::get_mut(&mut self.handlers)
            .unwrap()
            .insert(name.to_string(), Arc::new(move |cfg, params| {
                Box::pin(f(cfg, params))
            }));
        self
    }
}

#[derive(Deserialize)]
pub struct RpcRequest {
    pub method: String,
    pub params: Value,
}

pub async fn manager_rpc(
    State((config, router)): State<(Arc<RwLock<Config>>, ManagerRouter)>,
    headers: HeaderMap,
    Json(req): Json<RpcRequest>,
) -> Json<Value> {
    // 1. 鉴权
    if let Some(key) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
        let cfg = config.read().await;
        if key != cfg.manager_key {
            return Json(json!({"error": "Unauthorized"}));
        }
    } else {
        return Json(json!({"error": "Missing x-api-key"}));
    }

    // 2. 分发调用
    if let Some(handler) = router.handlers.get(&req.method) {
        let res = handler(Arc::clone(&config), req.params).await;
        Json(res)
    } else {
        Json(json!({"error": "Unknown method"}))
    }
}
