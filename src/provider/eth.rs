
use alloy::providers::{Provider};
use alloy::transports::http::reqwest::Url;

pub struct EthProvider {
    provider: reqwest::Client,
}

impl EthProvider {
    pub async fn new(url: &str) -> anyhow::Result<Self> {
        let url = Url::parse(url)?;
        let provider = reqwest::Client::new();
        Ok(Self { provider })
    }

    pub async fn get_latest_block_number(&self) -> anyhow::Result<u64> {
        let block_number = self.provider.get_block_number().await?;
        Ok(block_number)
    }

    pub async fn get_block_by_number(&self, block_number: u64) -> anyhow::Result<serde_json::Value> {
        let block = self.provider.get_block_by_number(block_number.into(), true).await?;
        Ok(serde_json::to_value(block)?)
    }
}