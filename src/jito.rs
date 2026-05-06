//! Jito bundle sender.

use crate::stats::Stats;
use anyhow::{Context, Result, anyhow};
use rand::Rng;
use rand::rngs::SmallRng;
use serde::Deserialize;
use solana_pubkey::Pubkey;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Jito's published minimum bundle tip. Bundles below this floor are dropped.
/// Documented at https://docs.jito.wtf/lowlatencytxnsend/#tip-amount.
pub const TIP_LAMPORTS: u64 = 1_000;

pub struct JitoSink {
    client: reqwest::Client,
    url: String,
    tip_accounts: Vec<Pubkey>,
    pub stats: Arc<Stats>,
}

impl JitoSink {
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Construct + fetch the active tip-account list from the block engine.
    /// Fails fast if the block engine is unreachable or returns no accounts.
    pub async fn new(url: String) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?;
        let tip_accounts = fetch_tip_accounts(&client, &url).await?;
        println!(
            "jito: fetched {} tip accounts from {url}",
            tip_accounts.len()
        );
        Ok(Self {
            client,
            url,
            tip_accounts,
            stats: Arc::new(Stats::default()),
        })
    }

    pub fn random_tip_account(&self, rng: &mut SmallRng) -> Pubkey {
        self.tip_accounts[rng.gen_range(0..self.tip_accounts.len())]
    }

    /// Fire and forget. `txs` are bincode-serialized wire-format transactions.
    /// Updates stats so the caller can see acceptance / rejection counts and
    /// the most recent error message.
    pub async fn send(&self, txs: Vec<Vec<u8>>) {
        let encoded: Vec<String> = txs.iter().map(|t| bs58::encode(t).into_string()).collect();
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [encoded],
        });
        match self.client.post(&self.url).json(&body).send().await {
            Ok(resp) => match resp.json::<RpcResponse<serde_json::Value>>().await {
                Ok(RpcResponse {
                    result: Some(_), ..
                }) => {
                    self.stats.accepted.fetch_add(1, Ordering::Relaxed);
                }
                Ok(RpcResponse { error: Some(e), .. }) => {
                    self.stats.rejected.fetch_add(1, Ordering::Relaxed);
                    *self.stats.last_error.lock().unwrap() = Some(e.message);
                }
                Ok(_) => {
                    self.stats.rejected.fetch_add(1, Ordering::Relaxed);
                    *self.stats.last_error.lock().unwrap() = Some("empty response".into());
                }
                Err(e) => {
                    self.stats.net_err.fetch_add(1, Ordering::Relaxed);
                    *self.stats.last_error.lock().unwrap() = Some(format!("decode: {e}"));
                }
            },
            Err(e) => {
                self.stats.net_err.fetch_add(1, Ordering::Relaxed);
                *self.stats.last_error.lock().unwrap() = Some(format!("post: {e}"));
            }
        }
    }
}

pub async fn fetch_tip_accounts(client: &reqwest::Client, url: &str) -> Result<Vec<Pubkey>> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTipAccounts",
        "params": [],
    });
    let resp: RpcResponse<Vec<String>> = client
        .post(url)
        .json(&body)
        .send()
        .await
        .context("getTipAccounts request")?
        .json()
        .await
        .context("getTipAccounts decode")?;
    let strs = resp.result.ok_or_else(|| {
        anyhow!(
            "getTipAccounts: empty result ({:?})",
            resp.error.map(|e| e.message)
        )
    })?;
    let accounts: Vec<Pubkey> = strs
        .into_iter()
        .map(|s| Pubkey::from_str(&s).context("parse tip pubkey"))
        .collect::<Result<_>>()?;
    anyhow::ensure!(!accounts.is_empty(), "getTipAccounts: empty list");
    Ok(accounts)
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    #[serde(default)]
    result: Option<T>,
    #[serde(default)]
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    #[allow(dead_code)]
    code: i64,
    message: String,
}
