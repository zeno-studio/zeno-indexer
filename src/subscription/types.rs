#[derive(Debug, Clone)]
pub enum SubscriptionType {
    NewBlocks,
    NewTransactions,
    ContractEvents { contract_address: String, event_name: String },
}