use ethers::signers::{LocalWallet, Signer};

pub struct Account {
    pub wallet: LocalWallet,
}

impl Account {
    pub fn from_private_key(private_key: &str) -> anyhow::Result<Self> {
        let wallet = private_key.parse::<LocalWallet>()?;
        Ok(Self { wallet })
    }

    pub async fn sign_message(&self, message: &str) -> anyhow::Result<String> {
        let signature = self.wallet.sign_message(message).await?;
        Ok(signature.to_string())
    }
}