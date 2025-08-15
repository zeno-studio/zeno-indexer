use crate::db::postgres::PostgresDb;
use crate::config::Config;
use crate::subscription::types::LogWithTimestamp;
use eyre::Result;   


use alloy::{
    providers::{Provider, ProviderBuilder, WsConnect},
    network::Ethereum,
    sol_types::SolEvent,
    primitives::{Address},
    rpc::types::{BlockNumberOrTag, Filter},
};
use tokio::time::{sleep, Duration};
use std::sync::Arc;
use tokio::sync::RwLock;
use futures_util::stream::StreamExt;
use std::str::FromStr;

// Define Transfer and Approval events
alloy::sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);
    event Approval(address indexed owner, address indexed spender, uint256 value);
}

// Type alias for the WebSocket provider
type WsProvider = dyn Provider<Ethereum> + Send + Sync;

//你完全可以在 main 里只建一次
// rust
// 复制
// 编辑
// let provider = Arc::new(ProviderBuilder::new().connect_ws(WsConnect::new(url)).await?);
// 然后把这个 Arc<Provider> clone 给每个合约处理函数。

// 因为底层是同一条 WSS 连接，RPC 服务商按连接数计费的话，这

pub async fn zeno_indexer(config: Arc<RwLock<Config>>) {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(60);

    loop {
        // Read configuration
        let (contract_address, eth_rpc_url, db) = {
            let config = config.read().await;
            (
                config.contract_address.clone(),
                config.eth_rpc_url.clone(),
                config.postgres_db.clone(),
            )
        };

        // Parse contract address
        let token_address = match Address::from_str(&contract_address) {
            Ok(addr) => addr,
            Err(e) => {
                eprintln!("Invalid contract address {}: {}", contract_address, e);
                sleep(Duration::from_secs(10)).await;
                continue;
            }
        };

        // Get latest block number
        let start_block = match db.get_latest_block_number().await {
            Ok(Some(block)) => block + 1,
            Ok(None) => 0,
            Err(e) => {
                eprintln!("Failed to get latest block number: {}", e);
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };

        // Establish WebSocket connection
        let ws = WsConnect::new(eth_rpc_url.clone());
        let provider = match ProviderBuilder::new().connect_ws(ws).await {
            Ok(provider) => Arc::new(provider),
            Err(e) => {
                eprintln!("Failed to connect to WebSocket: {}", e);
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };

        // Reset backoff on success
        backoff = Duration::from_secs(1);

        // Set up event filters
        let transfer_filter = Filter::new()
            .address(token_address)
            .event("Transfer(address,address,uint256)")
            .from_block(BlockNumberOrTag::Number(start_block));

        let approval_filter = Filter::new()
            .address(token_address)
            .event("Approval(address,address,uint256)")
            .from_block(BlockNumberOrTag::Number(start_block));

        // Subscribe to events
        let transfer_sub = match provider.subscribe_logs(&transfer_filter).await {
            Ok(sub) => sub,
            Err(e) => {
                eprintln!("Failed to subscribe to Transfer events: {}", e);
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };



        let approval_sub = match provider.subscribe_logs(&approval_filter).await {
            Ok(sub) => sub,
            Err(e) => {
                eprintln!("Failed to subscribe to Approval events: {}", e);
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };
        let provider_clone = Arc::clone(&provider);
        let transfer_stream = transfer_sub.into_stream().then(move |log| {
            let provider = Arc::clone(&provider_clone);
            async move {
                let block_number = log.block_number.unwrap_or_default();
               
                // 查询区块时间戳
                let block = provider
                    .get_block_by_number(block_number.into())
                    .await?
                    .ok_or_else(|| eyre::eyre!("Block not found"))?;

                let timestamp = block.header.timestamp as i64;

                Ok::<_, eyre::Report>(LogWithTimestamp {
                    timestamp,
                    rpclog: log,
                })
            }
        });

        let provider_clone = Arc::clone(&provider);

        let approval_stream = approval_sub.into_stream().then(move |log| {
            let provider = Arc::clone(&provider_clone);
            async move {
                let block_number = log.block_number.unwrap_or_default();
               
                // 查询区块时间戳
                let block = provider
                    .get_block_by_number(block_number.into())
                    .await?
                    .ok_or_else(|| eyre::eyre!("Block not found"))?;

                let timestamp = block.header.timestamp as i64;

                Ok::<_, eyre::Report>(LogWithTimestamp {
                    timestamp,
                    rpclog: log,
                })
            }
        });

        // Convert to streams
        let mut transfer_stream = Box::pin(transfer_stream);
        let mut approval_stream = Box::pin(approval_stream);

        loop {
            tokio::select! {
                result = transfer_stream.next() => {
                    match result {
                        Some(log) => {
                            if let Err(e) = process_transfer_log(&db, log.unwrap()).await {
                                eprintln!("Failed to process Transfer log: {}", e);
                            }
                        }
                        None => {
                            eprintln!("Transfer stream closed, reconnecting...");
                            break;
                        }
                    }
                }
                result = approval_stream.next() => {
                    match result {
                        Some(log) => {
                            if let Err(e) = process_approval_log(&db, log.unwrap()).await {
                                eprintln!("Failed to process Approval log: {}", e);
                            }
                        }
                        None => {
                            eprintln!("Approval stream closed, reconnecting...");
                            break;
                        }
                    }
                }
                else => {
                    eprintln!("All WebSocket subscriptions closed, reconnecting...");
                    break;
                }
            }
        }

        sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

// Process Transfer event
async fn process_transfer_log(
    db: &PostgresDb,    
    log: LogWithTimestamp,
) -> Result<(), eyre::Error> {
  
    let decoded = Transfer::decode_log(&log.rpclog.inner)?;
    let event = decoded.data;
    let block_number = log.rpclog.block_number.unwrap_or_default() as i64;
    let tx_hash = log.rpclog.transaction_hash.unwrap_or_default();
    let contract_address = decoded.address;
    let timestamp = log.timestamp;

    db.write_transfer(
        block_number,
        tx_hash,
        event.from,
        event.to,
        event.value,
        contract_address,
        timestamp,
    )
    .await?;

    Ok(())
}

// Process Approval event
async fn process_approval_log(
    db: &PostgresDb,
    log: LogWithTimestamp,
) -> Result<(), eyre::Error> {
    let decoded = Approval::decode_log(&log.rpclog.inner)?;
    let event = decoded.data;

    let block_number = log.rpclog.block_number.unwrap_or_default() as i64;
    let tx_hash = log.rpclog.transaction_hash.unwrap_or_default();
    let contract_address = decoded.address;

    let timestamp = log.timestamp;

    db.write_approval(
        block_number,
        tx_hash,    
        event.owner,
        event.spender,
        event.value,
        contract_address,
        timestamp,
    )
    .await?;

    Ok(())
}


 pub async fn write_transfer(
        &self,
        block_number: i64,
        tx_hash: B256,
        from: Address,
        to: Address,
        value: U256,
        contract_address: Address,
        timestamp: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO transfers (block_number, transaction_hash, from_address, to_address, value, contract_address, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, to_timestamp($7))
            "#,
        )
        .bind(block_number as i64)
        .bind(format!("{:?}", tx_hash))
        .bind(format!("{:?}", from))
        .bind(format!("{:?}", to))
        .bind(value.to_string())
        .bind(format!("{:?}", contract_address))
        .bind(timestamp)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

 pub async fn write_approval(
       
        block_number: i64,
        tx_hash: B256,
        owner: Address,
        spender: Address,
        value: U256,
        contract_address: Address,
        timestamp: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO approvals (block_number, transaction_hash, owner_address, spender_address, value, contract_address, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, to_timestamp($7))
            "#,
        )
        .bind(block_number as i64)
        .bind(format!("{:?}", tx_hash))
        .bind(format!("{:?}", owner))
        .bind(format!("{:?}", spender))
        .bind(value.to_string())
        .bind(format!("{:?}", contract_address))
        .bind(timestamp)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

   pub async fn get_latest_block_number() -> Result<Option<u64>, sqlx::Error> {
        // 从 transfers 或 approvals 表获取最新块号
        let transfer_block: Option<i64> = sqlx::query("SELECT MAX(block_number) FROM transfers")
            .fetch_optional(&self.pool)
            .await?
            .map(|row| row.get(0));
        let approval_block: Option<i64> = sqlx::query("SELECT MAX(block_number) FROM approvals")
            .fetch_optional(&self.pool)
            .await?
            .map(|row| row.get(0));

        Ok(match (transfer_block, approval_block) {
            (Some(t), Some(a)) => Some(t.max(a) as u64),
            (Some(t), None) => Some(t as u64),
            (None, Some(a)) => Some(a as u64),
            (None, None) => None,
        })
    }

    sol! {
    event Transfer(address indexed from, address indexed to, uint256 value);
    event Approval(address indexed owner, address indexed spender, uint256 value);
}