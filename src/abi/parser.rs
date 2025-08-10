use alloy::contract::Event;
use alloy::primitives::{Log, Address};
use serde_json::{Value, json};
use std::fs;

pub struct AbiParser {
    abi: alloy::json_abi::JsonAbi,
}

impl AbiParser {
    pub fn new(abi_path: &str) -> anyhow::Result<Self> {
        let abi_str = fs::read_to_string(abi_path)?;
        let abi: alloy::json_abi::JsonAbi = serde_json::from_str(&abi_str)?;
        Ok(Self { abi })
    }

    pub fn decode_event(&self, log: Log) -> anyhow::Result<Value> {
        let event = self.abi.events().find(|e| e.selector() == log.topics()[0])?;
        let decoded = event.decode_log(&log, true)?;
        Ok(json!({
            "event_name": event.name,
            "args": decoded.inputs,
            "log_index": log.log_index.unwrap_or_default(),
            "tx_hash": format!("{:?}", log.transaction_hash.unwrap_or_default()),
        }))
    }
}