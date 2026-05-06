//! Blockhash refresh loop.

use solana_hash::Hash;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

pub async fn refresh(rpc: Arc<RpcClient>, tx: watch::Sender<Hash>, mut current: Hash) -> ! {
    loop {
        match rpc.get_new_latest_blockhash(&current).await {
            Ok(h) => {
                current = h;
                let _ = tx.send(h);
            }
            Err(e) => {
                eprintln!("blockhash refresh: {e}");
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}
