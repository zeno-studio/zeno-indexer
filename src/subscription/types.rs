use alloy::rpc::types::Log as RpcLog;


#[derive(Debug, Clone)]
pub struct LogWithTimestamp {
    pub timestamp: i64, 
    pub rpclog: RpcLog,    
}

