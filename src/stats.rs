//! Shared bundle-sink stats + periodic logger.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Default)]
pub struct Stats {
    pub accepted: AtomicU64,
    pub rejected: AtomicU64,
    pub net_err: AtomicU64,
    pub last_error: Mutex<Option<String>>,
}

/// Periodic stats dump. Logs every 5s when anything happened in that window.
pub async fn stats_loop(label: &'static str, stats: Arc<Stats>) -> ! {
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    let (mut prev_ok, mut prev_rej, mut prev_err) = (0u64, 0u64, 0u64);
    loop {
        tick.tick().await;
        let ok = stats.accepted.load(Ordering::Relaxed);
        let rej = stats.rejected.load(Ordering::Relaxed);
        let err = stats.net_err.load(Ordering::Relaxed);
        let (d_ok, d_rej, d_err) = (ok - prev_ok, rej - prev_rej, err - prev_err);
        if d_ok + d_rej + d_err > 0 {
            let last = stats.last_error.lock().unwrap().clone();
            match (d_rej + d_err > 0, last) {
                (true, Some(msg)) => println!(
                    "[{label}] accepted={d_ok} rejected={d_rej} net_err={d_err} last_error={msg:?}"
                ),
                _ => println!("[{label}] accepted={d_ok} rejected={d_rej} net_err={d_err}"),
            }
        }
        prev_ok = ok;
        prev_rej = rej;
        prev_err = err;
    }
}
