//! Producer loop.
use crate::block_engine::BlockEngineSink;
use crate::jito::JitoSink;
use crate::leader::LeaderWindow;
use crate::tpu::TpuSink;
use crate::tx::TxBuilder;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use solana_hash::Hash;
use solana_pubkey::Pubkey;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

/// Either bundle-delivery path. Mutually exclusive — config picks one.
/// Inner sinks are Arc'd so background tasks (token refresher, stats logger)
/// can hold their own handles without re-matching the enum.
pub enum BundleSink {
    Jito(Arc<JitoSink>),
    BlockEngine(Arc<BlockEngineSink>),
}

impl BundleSink {
    pub fn url(&self) -> &str {
        match self {
            Self::Jito(j) => j.url(),
            Self::BlockEngine(be) => be.url(),
        }
    }

    pub fn random_tip_account(&self, rng: &mut SmallRng) -> Pubkey {
        match self {
            Self::Jito(j) => j.random_tip_account(rng),
            Self::BlockEngine(be) => be.random_tip_account(rng),
        }
    }

    pub fn tip_lamports(&self) -> u64 {
        match self {
            Self::Jito(_) => crate::jito::TIP_LAMPORTS,
            Self::BlockEngine(_) => crate::block_engine::TIP_LAMPORTS,
        }
    }

    pub async fn send(&self, txs: Vec<Vec<u8>>) {
        match self {
            Self::Jito(j) => j.send(txs).await,
            Self::BlockEngine(be) => be.send(txs).await,
        }
    }
}

const SLICE_HZ: u32 = 100;

/// TPU window: 1-slot pre-roll covers slot-anchor estimation error; no
/// post-roll because off-leader sends get forwarded elsewhere.
const TPU_PRE_ROLL: u64 = 1;
const TPU_POST_ROLL: u64 = 0;

/// Bundle window: bundles take a few slots to traverse the block engine,
/// so we start well ahead of the leader slot. No post-roll — once TPU
/// has landed a recipe, bundles arriving after just get rejected as
/// already-processed.
const BUNDLE_PRE_ROLL: u64 = 6;
const BUNDLE_POST_ROLL: u64 = 0;

pub async fn run(
    mut builder: TxBuilder,
    tpu: Arc<TpuSink>,
    bundle: Option<Arc<BundleSink>>,
    blockhash: watch::Receiver<Hash>,
    window: LeaderWindow,
    target_tps: NonZeroU32,
) -> ! {
    println!(
        "starting spam: {target_tps} tps to tpu {}{}",
        tpu.target(),
        match &bundle {
            Some(b) => format!(" + bundle {}", b.url()),
            None => String::new(),
        }
    );
    log_next_slot(&window);

    let mut rng = SmallRng::from_entropy();
    let per_slice = (target_tps.get() as usize).div_ceil(SLICE_HZ as usize);
    let mut tick = tokio::time::interval(Duration::from_millis(1000 / SLICE_HZ as u64));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);
    let mut was_active = false;

    loop {
        tick.tick().await;
        let send_tpu = window.in_window(TPU_PRE_ROLL, TPU_POST_ROLL);
        let send_bundle = bundle.is_some() && window.in_window(BUNDLE_PRE_ROLL, BUNDLE_POST_ROLL);
        let active = send_tpu || send_bundle;
        if active != was_active {
            if active {
                println!(
                    "leader window opened (slot {})",
                    window.estimated_current_slot()
                );
            } else {
                println!("leader window closed");
                log_next_slot(&window);
            }
            was_active = active;
        }
        if !active {
            continue;
        }
        let bh = *blockhash.borrow();

        let mut tpu_batch = Vec::with_capacity(per_slice);
        for _ in 0..per_slice {
            let (recipe_tx, payer_idx, nonce) = builder.next(bh, &mut rng);
            let recipe_bytes = bincode::serialize(&recipe_tx).expect("bincode tx");

            // Pick exactly one sink. In the overlap, coin-flip; otherwise
            // whichever window is currently open.
            let use_bundle = match (send_tpu, send_bundle) {
                (true, true) => rng.r#gen::<bool>(),
                (false, true) => true,
                (true, false) => false,
                (false, false) => unreachable!("active was true"),
            };

            if use_bundle && let Some(b) = &bundle {
                let tip_account = b.random_tip_account(&mut rng);
                let tip_tx =
                    builder.build_tip(payer_idx, bh, &tip_account, b.tip_lamports(), nonce);
                let pkt = vec![
                    recipe_bytes,
                    bincode::serialize(&tip_tx).expect("bincode tip"),
                ];
                let b = b.clone();
                tokio::spawn(async move { b.send(pkt).await });
            } else {
                tpu_batch.push(recipe_bytes);
            }
        }
        // TPU sends inline so a slow handshake/congested link backpressures
        // the producer (instead of growing unbounded in-flight tasks).
        if !tpu_batch.is_empty() {
            tpu.send_batch(tpu_batch).await;
        }
    }
}

fn log_next_slot(window: &LeaderWindow) {
    match window.next_leader_slot() {
        Some(s) => {
            let now = window.estimated_current_slot();
            let delta = s.saturating_sub(now);
            println!(
                "next leader slot: {s} (in {delta} slots, ~{:.1}s)",
                delta as f64 * 0.4
            );
        }
        None => println!("no upcoming leader slots in current schedule"),
    }
}
