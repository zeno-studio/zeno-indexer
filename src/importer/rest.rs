use crate::db::postgres::PostgresDb;
use crate::processor::handler::DataProcessor;
use reqwest::Client;
use serde_json::Value;

pub struct RestImporter {
    client: Client,
    api_url: String,
    db: PostgresDb,
}

impl RestImporter {
    pub fn new(api_url: String, db: PostgresDb) -> Self {
        Self {
            client: Client::new(),
            api_url,
            db,
        }
    }

    pub async fn start_import(&self) -> anyhow::Result<()> {
        loop {
            let response = self.client.get(&self.api_url).send().await?;
            let json_data: Value = response.json().await?;
            
            // 假设 API 返回的是区块数据数组
            if let Some(blocks) = json_data.as_array() {
                for block in blocks {
                    let processed = DataProcessor::process_block_data(block.clone());
                    if let Some(block_number) = processed.get("block_number").and_then(|n| n.as_i64()) {
                        self.db.insert_block(block_number, processed).await?;
                        tracing::info!("Imported block: {}", block_number);
                    }
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(60)).await; // 每分钟轮询
        }
    }
}