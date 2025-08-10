use axum::{response::Json, http::StatusCode, extract::State};
use serde::{Deserialize, Serialize};
use crate::db::postgres::PostgresDb;

#[derive(Deserialize)]
pub struct SwitchDbRequest {
    target: String, // "master" 或 "replica"
}

#[derive(Serialize)]
pub struct SwitchDbResponse {
    message: String,
    current_url: String,
}


pub async fn switch_db(
    State(db): State<PostgresDb>,
    Json(request): Json<SwitchDbRequest>,
) -> Result<Json<SwitchDbResponse>, (StatusCode, String)> {
    match request.target.as_str() {
        "master" => {
            db.switch_to_master().await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            Ok(Json(SwitchDbResponse {
                message: "Switched to master database".to_string(),
                current_url: db.master_url.clone(),
            }))
        }
        "replica" => {
            db.switch_to_replica().await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            Ok(Json(SwitchDbResponse {
                message: "Switched to replica database".to_string(),
                current_url: db.replica_url.clone(),
            }))
        }
        _ => Err((StatusCode::BAD_REQUEST, "Invalid target: must be 'master' or 'replica'".to_string())),
    }
}