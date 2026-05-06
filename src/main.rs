mod block_engine;
mod blockhash;
mod jito;
mod leader;
mod producer;
mod stats;
mod tpu;
mod tx;

use crate::block_engine::BlockEngineSink;
use crate::jito::JitoSink;
use crate::leader::LeaderWindow;
use crate::producer::BundleSink;
use crate::tpu::TpuSink;
use crate::tx::TxBuilder;
use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Deserializer, de};
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use std::env::args;
use std::fs::read_to_string;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Debug, Deserialize)]
struct Config {
    rpc_url: String,
    keypairs: PathBuf,
    #[serde(deserialize_with = "deserialize_pubkey")]
    identity: Pubkey,
    target_tps: NonZeroU32,
    /// Override gossip resolution.
    #[serde(default)]
    tpu_addr: Option<SocketAddr>,
    /// Jito's public block engine (HTTP/JSON-RPC `sendBundle`).
    #[serde(default)]
    jito_url: Option<String>,
    /// Harmonic block engine endpoint.
    #[serde(default)]
    block_engine_url: Option<String>,
    /// Harmonic bundle searcher keypair.
    #[serde(default)]
    harmonic_bundle_keypair: Option<PathBuf>,
}

const TIP_ACCOUNTS_URL: &str = "https://ny.testnet.block-engine.jito.wtf/api/v1/bundles";

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // rustls requires this before any TLS config is built.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("no crypto provider previously installed");

    let config: Config = toml::from_str(&read_to_string(
        args()
            .nth(1)
            .ok_or(anyhow!("usage: harmonic-tx-sender <config.toml>"))?,
    )?)?;
    println!("{config:#?}");
    let keypairs = tx::load_keypairs(&config.keypairs)?;
    let rpc = Arc::new(RpcClient::new_with_commitment(
        config.rpc_url.clone(),
        CommitmentConfig::confirmed(),
    ));

    let target_tpu = match config.tpu_addr {
        Some(addr) => addr,
        None => leader::resolve_tpu(&rpc, &config.identity).await?,
    };
    let window = LeaderWindow::init(&rpc, &config.identity).await?;
    let initial_bh = rpc.get_latest_blockhash().await?;
    let (bh_tx, blockhash_rx) = watch::channel(initial_bh);
    let tpu = Arc::new(TpuSink::new(target_tpu)?);
    let bundle = build_bundle_sink(&config, &keypairs).await?;
    let builder = TxBuilder::new(keypairs)?;

    tokio::spawn(leader::anchor_slot(rpc.clone(), window.clone()));
    tokio::spawn(leader::refresh_schedule(
        rpc.clone(),
        config.identity,
        window.clone(),
    ));
    tokio::spawn(blockhash::refresh(rpc.clone(), bh_tx, initial_bh));
    tokio::spawn(tpu::warmer(tpu.clone(), window.clone()));
    if let Some(b) = &bundle {
        match b.as_ref() {
            BundleSink::Jito(j) => {
                tokio::spawn(stats::stats_loop("jito", j.stats.clone()));
            }
            BundleSink::BlockEngine(be) => {
                tokio::spawn(stats::stats_loop("block_engine", be.stats.clone()));
                tokio::spawn(block_engine::refresh_loop(be.clone()));
            }
        }
    }
    tokio::spawn(producer::run(
        builder,
        tpu,
        bundle,
        blockhash_rx,
        window,
        config.target_tps,
    ));

    tokio::signal::ctrl_c().await?;
    println!("shutting down");
    Ok(())
}

/// Construct the bundle sink based on config. Mutually exclusive: at most one
/// of `jito_url` or `block_engine_url` may be set.
async fn build_bundle_sink(
    config: &Config,
    payers: &[solana_keypair::Keypair],
) -> Result<Option<Arc<BundleSink>>> {
    match (&config.jito_url, &config.block_engine_url) {
        (Some(_), Some(_)) => bail!("set jito_url OR block_engine_url, not both"),
        (Some(url), None) => Ok(Some(Arc::new(BundleSink::Jito(Arc::new(
            JitoSink::new(url.clone()).await?,
        ))))),
        (None, Some(url)) => {
            let http = reqwest::Client::new();
            let tips = jito::fetch_tip_accounts(&http, TIP_ACCOUNTS_URL).await?;
            let owned_kp = config
                .harmonic_bundle_keypair
                .as_deref()
                .map(|path| {
                    solana_keypair::read_keypair_file(path).map_err(|e| {
                        anyhow!("read harmonic_bundle_keypair {}: {e}", path.display())
                    })
                })
                .transpose()?;
            let auth_kp = owned_kp.as_ref().unwrap_or(&payers[0]);
            Ok(Some(Arc::new(BundleSink::BlockEngine(Arc::new(
                BlockEngineSink::new(url.clone(), auth_kp, tips).await?,
            )))))
        }
        (None, None) => Ok(None),
    }
}

fn deserialize_pubkey<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Pubkey, D::Error> {
    Pubkey::from_str(&String::deserialize(d)?).map_err(de::Error::custom)
}
