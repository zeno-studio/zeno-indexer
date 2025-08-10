use crate::provider::eth::EthProvider;
use crate::db::postgres::PostgresDb;
use crate::abi::parser::AbiParser;
use crate::subscription::types::SubscriptionType;
use alloy::primitives::{Address, Log};
use tokio::sync::mpsc;

pub struct SubscriptionManager {
    provider: EthProvider,
    db: PostgresDb,
    abi_parser: AbiParser,
    shutdown: mpsc::Sender<()>,
}

impl SubscriptionManager {
    pub fn new(provider: EthProvider, db: PostgresDb, abi_parser: AbiParser) -> (Self, mpsc::Receiver<()>) {
        let (shutdown, rx) = mpsc::channel(1);
        (Self { provider, db, abi_parser, shutdown }, rx)
    }

    pub async fn start(&self, sub_type: SubscriptionType) -> anyhow::Result<()> {
        match sub_type {
            SubscriptionType::NewBlocks => self.subscribe_blocks().await,
            SubscriptionType::NewTransactions => self.subscribe_transactions().await,
            SubscriptionType::ContractEvents { contract_address, event_name } => {
                self.subscribe_contract_events(&contract_address, &event_name).await
            }
        }
    }

    pub async fn stop(&self) -> anyhow::Result<()> {
        self.shutdown.send(()).await?;
        Ok(())
    }

    async fn subscribe_blocks(&self) -> anyhow::Result<()> {
        // 保持原有逻辑
        let mut latest_block = self.provider.get_latest_block_number().await?;
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(12)).await;
            let current_block = self.provider.get_latest_block_number().await?;
            if current_block > latest_block {
                for block_number in (latest_block + 1)..=current_block {
                    let block_data = self.provider.get_block_by_number(block_number).await?;
                    self.db.insert_block(block_number as i64, block_data).await?;
                    tracing::info!("Indexed block: {}", block_number);
                }
                latest_block = current_block;
            }
        }
    }

    async fn subscribe_transactions(&self) -> anyhow::Result<()> {
        todo!("Implement transaction subscription")
    }

    async fn subscribe_contract_events(&self, contract_address: &str, event_name: &str) -> anyhow::Result<()> {
        let address: Address = contract_address.parse()?;
        let mut latest_block = self.provider.get_latest_block_number().await?;

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(12)).await; // 轮询，生产环境建议用 WebSocket
            let current_block = self.provider.get_latest_block_number().await?;
            if current_block > latest_block {
                for block_number in (latest_block + 1)..=current_block {
                    let filter = alloy::rpc::types::Filter::new()
                        .address(address)
                        .from_block(block_number)
                        .to_block(block_number);
                    let logs = self.provider.get_logs(&filter).await?;
                    for log in logs {
                        let event_data = self.abi_parser.decode_event(log.clone())?;
                        let event_id = format!("{}_{}", log.transaction_hash.unwrap_or_default(), log.log_index.unwrap_or_default());
                        self.db.insert_event(&event_id, event_data).await?;
                        tracing::info!("Indexed event: {} in block {}", event_name, block_number);
                    }
                }
                latest_block = current_block;
            }
        }
    }
}