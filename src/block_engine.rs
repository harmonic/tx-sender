//! Block-engine bundle sender.

use crate::stats::Stats;
use anyhow::{Context, Result, anyhow};
use block_engine_protos::auth::auth_service_client::AuthServiceClient;
use block_engine_protos::auth::{
    GenerateAuthChallengeRequest, GenerateAuthTokensRequest, RefreshAccessTokenRequest,
};
use block_engine_protos::bundle::Bundle;
use block_engine_protos::packet::{Meta, Packet};
use block_engine_protos::searcher::SendBundleRequest;
use block_engine_protos::searcher::searcher_service_client::SearcherServiceClient;
use rand::Rng;
use rand::rngs::SmallRng;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_signer::Signer;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

pub const TIP_LAMPORTS: u64 = 1_000;

pub struct BlockEngineSink {
    url: String,
    searcher: SearcherServiceClient<Channel>,
    auth: AuthServiceClient<Channel>,
    access_token: Arc<Mutex<String>>,
    refresh_token: Arc<Mutex<String>>,
    tip_accounts: Vec<Pubkey>,
    pub stats: Arc<Stats>,
}

impl BlockEngineSink {
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Connect and perform the challenge-sign-tokens auth dance. Network and
    /// auth failures both fail fast — caller decides whether to abort.
    pub async fn new(url: String, keypair: &Keypair, tip_accounts: Vec<Pubkey>) -> Result<Self> {
        anyhow::ensure!(!tip_accounts.is_empty(), "no tip accounts provided");

        let mut endpoint = Endpoint::from_shared(url.clone())
            .context("invalid block engine url")?
            .timeout(Duration::from_secs(5))
            .keep_alive_while_idle(true);
        // Enable TLS for https:// endpoints; harmless for http://.
        if url.starts_with("https://") {
            endpoint = endpoint
                .tls_config(ClientTlsConfig::new().with_webpki_roots())
                .context("tls config")?;
        }
        let channel = endpoint.connect().await.context("block engine connect")?;

        let mut auth = AuthServiceClient::new(channel.clone());
        let searcher = SearcherServiceClient::new(channel);
        let pubkey = keypair.pubkey();
        println!("block_engine: connected to {url}, authenticating as searcher {pubkey}");

        let (access_token, refresh_token) = try_auth(&mut auth, keypair).await?;

        println!(
            "block_engine: authenticated as {pubkey}, {} tip accounts loaded",
            tip_accounts.len()
        );

        Ok(Self {
            url,
            searcher,
            auth,
            access_token: Arc::new(Mutex::new(access_token)),
            refresh_token: Arc::new(Mutex::new(refresh_token)),
            tip_accounts,
            stats: Arc::new(Stats::default()),
        })
    }

    pub fn random_tip_account(&self, rng: &mut SmallRng) -> Pubkey {
        self.tip_accounts[rng.gen_range(0..self.tip_accounts.len())]
    }

    /// Fire and forget bundle send via gRPC SendBundle.
    pub async fn send(&self, txs: Vec<Vec<u8>>) {
        let bundle = Bundle {
            header: None,
            packets: txs
                .into_iter()
                .map(|t| {
                    let size = t.len() as u64;
                    Packet {
                        data: t.into(),
                        meta: Some(Meta {
                            size,
                            addr: String::new(),
                            port: 0,
                            flags: None,
                            sender_stake: 0,
                        }),
                    }
                })
                .collect(),
        };

        let mut req = Request::new(SendBundleRequest {
            bundle: Some(bundle),
        });
        let token = self.access_token.lock().unwrap().clone();
        let auth_value: MetadataValue<_> = match format!("Bearer {token}").parse() {
            Ok(v) => v,
            Err(_) => {
                self.stats.net_err.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        req.metadata_mut().insert("authorization", auth_value);

        let mut client = self.searcher.clone();
        match client.send_bundle(req).await {
            Ok(_) => {
                self.stats.accepted.fetch_add(1, Ordering::Relaxed);
            }
            Err(s) => {
                // `Code::Unknown` covers transport-level failures (HTTP 500
                // with HTML body, proxy errors, frame-parse errors). Specific
                // gRPC codes are real bundle rejections from the engine.
                let counter = if s.code() == tonic::Code::Unknown {
                    &self.stats.net_err
                } else {
                    &self.stats.rejected
                };
                counter.fetch_add(1, Ordering::Relaxed);
                *self.stats.last_error.lock().unwrap() = Some(s.message().to_string());
            }
        }
    }
}

/// One attempt at the challenge-sign-tokens auth dance. Returns the access
/// and refresh token strings on success.
async fn try_auth(
    auth: &mut AuthServiceClient<Channel>,
    keypair: &Keypair,
) -> Result<(String, String)> {
    const AUTH_ROLE_SEARCHER: i32 = 3;

    let pubkey = keypair.pubkey();
    let challenge = auth
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: AUTH_ROLE_SEARCHER,
            pubkey: pubkey.to_bytes().to_vec().into(),
        })
        .await
        .context("GenerateAuthChallenge")?
        .into_inner()
        .challenge;

    // Server expects sig over `<pubkey_base58>-<challenge>`.
    let signature = keypair.sign_message(format!("{pubkey}-{challenge}").as_bytes());

    let tokens = auth
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge,
            client_pubkey: pubkey.to_bytes().to_vec().into(),
            signed_challenge: signature.as_ref().to_vec().into(),
        })
        .await
        .context("GenerateAuthTokens")?
        .into_inner();

    let access = tokens
        .access_token
        .ok_or_else(|| anyhow!("missing access token"))?
        .value;
    let refresh = tokens
        .refresh_token
        .ok_or_else(|| anyhow!("missing refresh token"))?
        .value;
    Ok((access, refresh))
}

/// Background task: refresh the access token before it expires.
pub async fn refresh_loop(sink: Arc<BlockEngineSink>) -> ! {
    let mut tick = tokio::time::interval(Duration::from_secs(5 * 60));
    tick.tick().await; // skip immediate firing
    loop {
        tick.tick().await;
        let refresh = sink.refresh_token.lock().unwrap().clone();
        let mut auth = sink.auth.clone();
        match auth
            .refresh_access_token(RefreshAccessTokenRequest {
                refresh_token: refresh,
            })
            .await
        {
            Ok(resp) => {
                if let Some(token) = resp.into_inner().access_token {
                    *sink.access_token.lock().unwrap() = token.value;
                }
            }
            Err(e) => eprintln!("block_engine token refresh: {e}"),
        }
    }
}
