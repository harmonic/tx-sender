//! Leader-window tracking.

use anyhow::{Context, Result, anyhow};
use solana_clock::DEFAULT_MS_PER_SLOT;
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// QUIC connection warmup begins this many slots before the leader window,
/// so the first tx at T=0 doesn't pay handshake latency.
pub const WARMER_PRE_ROLL: u64 = 3;

#[derive(Clone, Copy)]
struct SlotAnchor {
    slot: u64,
    when: Instant,
}

#[derive(Clone)]
pub struct LeaderWindow {
    anchor: Arc<Mutex<SlotAnchor>>,
    target_slots: Arc<RwLock<BTreeSet<u64>>>,
}

impl LeaderWindow {
    /// Prime the slot anchor + leader schedule from RPC. Caller must spawn
    /// `anchor_slot` and `refresh_schedule` to keep these fresh.
    pub async fn init(rpc: &RpcClient, identity: &Pubkey) -> Result<Self> {
        let initial_slot = rpc
            .get_slot_with_commitment(CommitmentConfig::processed())
            .await?;
        let target_slots = load_target_slots(rpc, identity).await?;
        Ok(Self {
            anchor: Arc::new(Mutex::new(SlotAnchor {
                slot: initial_slot,
                when: Instant::now(),
            })),
            target_slots: Arc::new(RwLock::new(target_slots)),
        })
    }

    pub fn estimated_current_slot(&self) -> u64 {
        let anchor = self.anchor.lock().expect("anchor mutex poisoned");
        let elapsed_ms = anchor.when.elapsed().as_millis() as u64;
        anchor
            .slot
            .saturating_add((elapsed_ms + DEFAULT_MS_PER_SLOT / 2) / DEFAULT_MS_PER_SLOT)
    }

    /// True if the target is leader for any slot in `[now - post_roll, now + pre_roll]`.
    /// Use `pre_roll` to start sending early (covers timing slack and propagation
    /// latency); use `post_roll` to keep sending after the last leader slot
    /// (covers in-flight traffic).
    pub fn in_window(&self, pre_roll: u64, post_roll: u64) -> bool {
        let now = self.estimated_current_slot();
        let lo = now.saturating_sub(post_roll);
        let hi = now.saturating_add(pre_roll);
        let slots = self.target_slots.read().expect("target_slots poisoned");
        slots.range(lo..=hi).next().is_some()
    }

    /// Lowest leader slot at or after `now`, or `None` if none remain in the
    /// cached schedule.
    pub fn next_leader_slot(&self) -> Option<u64> {
        let now = self.estimated_current_slot();
        let slots = self.target_slots.read().expect("target_slots poisoned");
        slots.range(now..).next().copied()
    }
}

pub async fn resolve_tpu(rpc: &RpcClient, identity: &Pubkey) -> Result<SocketAddr> {
    let ident_str = identity.to_string();
    let nodes = rpc.get_cluster_nodes().await.context("getClusterNodes")?;
    let node = nodes
        .into_iter()
        .find(|n| n.pubkey == ident_str)
        .ok_or_else(|| anyhow!("identity {identity} not found in gossip"))?;
    // Prefer the QUIC-specific TPU port
    node.tpu_quic
        .or(node.tpu)
        .ok_or_else(|| anyhow!("identity {identity} has no advertised TPU socket"))
}

/// Slot anchor refresh loop. Polls every 200ms; wall-clock projection between
/// polls keeps the window accurate to ~half a slot.
pub async fn anchor_slot(rpc: Arc<RpcClient>, window: LeaderWindow) -> ! {
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    loop {
        tick.tick().await;
        let sent = Instant::now();
        if let Ok(s) = rpc
            .get_slot_with_commitment(CommitmentConfig::processed())
            .await
        {
            // Compensate for RPC RTT: server observed this slot ~RTT/2 ago.
            let observed_at = Instant::now() - sent.elapsed() / 2;
            let mut g = window.anchor.lock().expect("anchor mutex poisoned");
            if s >= g.slot {
                *g = SlotAnchor {
                    slot: s,
                    when: observed_at,
                };
            }
        }
    }
}

/// Schedule refresh loop. Reloads the target's leader slots when the epoch rolls over.
pub async fn refresh_schedule(rpc: Arc<RpcClient>, identity: Pubkey, window: LeaderWindow) -> ! {
    let mut last_epoch: Option<u64> = None;
    let mut tick = tokio::time::interval(Duration::from_secs(10));
    loop {
        tick.tick().await;
        let Ok(info) = rpc.get_epoch_info().await else {
            continue;
        };
        if last_epoch == Some(info.epoch) {
            continue;
        }
        match load_target_slots(&rpc, &identity).await {
            Ok(s) => {
                println!(
                    "loaded {} target leader slots for epoch {}",
                    s.len(),
                    info.epoch
                );
                *window.target_slots.write().expect("target_slots poisoned") = s;
                last_epoch = Some(info.epoch);
            }
            Err(e) => eprintln!("reload leader schedule: {e}"),
        }
    }
}

async fn load_target_slots(rpc: &RpcClient, identity: &Pubkey) -> Result<BTreeSet<u64>> {
    let schedule = rpc
        .get_leader_schedule(None)
        .await
        .context("getLeaderSchedule")?
        .ok_or_else(|| anyhow!("leader schedule unavailable"))?;
    let relative = schedule
        .get(&identity.to_string())
        .cloned()
        .unwrap_or_default();
    let info = rpc.get_epoch_info().await?;
    let first_slot = info.absolute_slot - info.slot_index;
    Ok(relative
        .into_iter()
        .map(|r| first_slot + r as u64)
        .collect())
}
