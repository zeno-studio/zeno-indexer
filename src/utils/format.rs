use anyhow::{Result, bail};

/// 检查 Ethereum 地址格式，如果合法则返回小写形式
pub fn validate_eth_address(address: &str) -> Result<()> {
    // 检查长度
    if address.len() != 42 {
        bail!("Invalid address length: expected 42, got {}", address.len());
    }

    // 检查前缀
    if !address.starts_with("0x") {
        bail!("Invalid address prefix: expected '0x'");
    }

    // 检查是否全部为 hex 字符
    if !address.chars().skip(2).all(|c| c.is_ascii_hexdigit()) {
        bail!("Address contains non-hex characters");
    }

    // 返回小写形式
    Ok(address.to_lowercase())
}