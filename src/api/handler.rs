use axum::{
    routing::post,
    Json,
    Router,
    http::{StatusCode, HeaderMap},
    middleware::{self, Next},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use crate::config::Config;
use std::sync::Arc;
use tokio::sync::RwLock;


#[derive(Deserialize)]
struct UpdateConfig {
    primary_db_url: String,
}

pub async fn update_db_url(
    state: axum::extract::State<Arc<RwLock<Config>>>,
    Json(payload): Json<UpdateConfig>,
) -> String {
    let mut config = state.write().await;
    match config.update_db_url(payload.primary_db_url).await {
        Ok(()) => format!(
            "Primary DB URL updated to: {}",
            config.postgres_db.primary_db_url,

        ),
        Err(e) => format!("Failed to update DB URL: {}", e),
    }
}

// API 密钥认证中间件
pub async fn api_key_auth(
    headers: HeaderMap,
    state: axum::extract::State<Arc<RwLock<Config>>>,
    request: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let config = state.read().await;
    let expected_api_key = config.api_key.clone();

    let provided_api_key = headers
        .get("X-API-Key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");

    if provided_api_key != expected_api_key {
        return Err((StatusCode::UNAUTHORIZED, "Invalid API key".to_string()));
    }

    Ok(next.run(request).await)
}
