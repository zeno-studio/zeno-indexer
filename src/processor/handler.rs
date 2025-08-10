use serde_json::Value;

pub struct DataProcessor;

impl DataProcessor {
    pub fn process_block_data(raw_data: Value) -> Value {
        // 示例：提取关键字段，清洗数据
        let mut processed = serde_json::Map::new();
        if let Some(number) = raw_data.get("number") {
            processed.insert("block_number".to_string(), number.clone());
        }
        if let Some(hash) = raw_data.get("hash") {
            processed.insert("block_hash".to_string(), hash.clone());
        }
        Value::Object(processed)
    }

    pub fn process_transaction_data(raw_data: Value) -> Value {
        // 示例：处理交易数据
        let mut processed = serde_json::Map::new();
        if let Some(hash) = raw_data.get("hash") {
            processed.insert("tx_hash".to_string(), hash.clone());
        }
        Value::Object(processed)
    }
}