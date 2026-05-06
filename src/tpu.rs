//! Direct QUIC TPU sender.

use crate::leader::{LeaderWindow, WARMER_PRE_ROLL};
use anyhow::{Context, Result};
use quinn::{ClientConfig, Connection, Endpoint, IdleTimeout, TransportConfig};
use rustls::{
    DigitallySignedStruct, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use solana_keypair::Keypair;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

const ALPN_TPU: &[u8] = b"solana-tpu";
const QUIC_MAX_IDLE: Duration = Duration::from_secs(2);
const QUIC_KEEP_ALIVE: Duration = Duration::from_secs(1);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

pub struct TpuSink {
    endpoint: Endpoint,
    target: SocketAddr,
    connection: Arc<Mutex<Option<Connection>>>,
    /// Serializes handshake attempts so racing callers don't fire N parallel
    /// connects at the validator.
    connect_lock: Arc<Mutex<()>>,
}

impl TpuSink {
    pub fn target(&self) -> SocketAddr {
        self.target
    }

    pub fn new(target: SocketAddr) -> Result<Self> {
        let bind: SocketAddr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0));
        let mut endpoint = Endpoint::client(bind).context("quinn endpoint")?;
        endpoint.set_default_client_config(client_config()?);
        Ok(Self {
            endpoint,
            target,
            connection: Arc::new(Mutex::new(None)),
            connect_lock: Arc::new(Mutex::new(())),
        })
    }

    /// Open one QUIC stream per tx and write concurrently. Awaiting in the
    /// producer loop gives natural backpressure: a slow handshake or
    /// congested link slows the loop, not unbounded task growth.
    pub async fn send_batch(&self, wire_txs: Vec<Vec<u8>>) {
        let conn = match ensure_connected(
            &self.endpoint,
            self.target,
            &self.connection,
            &self.connect_lock,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!("connect to {}: {e:#}", self.target);
                return;
            }
        };

        let writes = wire_txs.into_iter().map(|tx| {
            let conn = conn.clone();
            async move {
                let _ = write_one(&conn, &tx).await;
            }
        });
        futures::future::join_all(writes).await;

        // If the connection died mid-batch, drop it so next send reconnects.
        if conn.close_reason().is_some() {
            let mut g = self.connection.lock().await;
            if let Some(c) = g.as_ref()
                && c.stable_id() == conn.stable_id()
            {
                *g = None;
            }
        }
    }
}

/// Pre-warm the QUIC handshake WARMER_PRE_ROLL slots before the target is
/// leader, so the first tx at T=0 doesn't pay ~500ms handshake latency.
pub async fn warmer(sink: Arc<TpuSink>, window: LeaderWindow) -> ! {
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    loop {
        tick.tick().await;
        if !window.in_window(WARMER_PRE_ROLL, 0) {
            continue;
        }
        let _ = ensure_connected(
            &sink.endpoint,
            sink.target,
            &sink.connection,
            &sink.connect_lock,
        )
        .await;
    }
}

async fn ensure_connected(
    endpoint: &Endpoint,
    target: SocketAddr,
    slot: &Mutex<Option<Connection>>,
    connect_lock: &Mutex<()>,
) -> Result<Connection> {
    if let Some(c) = current_connection(slot).await {
        return Ok(c);
    }
    let _g = connect_lock.lock().await;
    if let Some(c) = current_connection(slot).await {
        return Ok(c);
    }
    let connecting = endpoint
        .connect(target, "solana-tpu")
        .context("connect start")?;
    let conn = tokio::time::timeout(HANDSHAKE_TIMEOUT, connecting)
        .await
        .context("handshake timeout")?
        .context("handshake failed")?;
    println!("quic connected to {target}");
    *slot.lock().await = Some(conn.clone());
    Ok(conn)
}

async fn current_connection(slot: &Mutex<Option<Connection>>) -> Option<Connection> {
    let g = slot.lock().await;
    g.as_ref().filter(|c| c.close_reason().is_none()).cloned()
}

async fn write_one(conn: &Connection, tx: &[u8]) -> Result<()> {
    let mut s = conn.open_uni().await.context("open_uni")?;
    s.write_all(tx).await.context("write_all")?;
    s.finish().context("finish")?;
    Ok(())
}

fn client_config() -> Result<ClientConfig> {
    let ephemeral = Keypair::new();
    let (cert, key) = solana_tls_utils::new_dummy_x509_certificate(&ephemeral);

    let mut crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipVerify))
        .with_client_auth_cert(vec![cert], key)
        .context("rustls client config")?;
    crypto.enable_early_data = true;
    crypto.alpn_protocols = vec![ALPN_TPU.to_vec()];

    let qcc = quinn::crypto::rustls::QuicClientConfig::try_from(crypto).context("quic crypto")?;
    let mut cfg = ClientConfig::new(Arc::new(qcc));

    let mut tc = TransportConfig::default();
    tc.max_idle_timeout(Some(IdleTimeout::try_from(QUIC_MAX_IDLE)?));
    tc.keep_alive_interval(Some(QUIC_KEEP_ALIVE));
    // Bump initial congestion window — the default ~14KB costs us the first
    // ~2 slots of a fresh connection on slow-start.
    let mut cubic = quinn::congestion::CubicConfig::default();
    cubic.initial_window(512 * 1024);
    tc.congestion_controller_factory(Arc::new(cubic));
    cfg.transport_config(Arc::new(tc));
    Ok(cfg)
}

/// Validators use self-signed certs tied to node identity; no PKI to verify against.
#[derive(Debug)]
struct SkipVerify;

impl ServerCertVerifier for SkipVerify {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn verify_tls13_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![SignatureScheme::ED25519]
    }
}
