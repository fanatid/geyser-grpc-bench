use {
    anyhow::Context,
    clap::Parser,
    geyser_grpc_bench::BenchProgressBar,
    log::info,
    prost::Message,
    quinn::{crypto::rustls::QuicClientConfig, ClientConfig, Endpoint, TransportConfig},
    rustls::pki_types::{CertificateDer, ServerName, UnixTime},
    std::{env, path::PathBuf, sync::Arc},
    tokio::{fs, io::AsyncReadExt},
    yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeUpdate},
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("127.0.0.1:10002"))]
    endpoint: String,

    #[clap(long)]
    cert: Option<PathBuf>,

    #[clap(long)]
    insecure: bool,

    /// Value in ms
    #[clap(long, default_value_t = 100)]
    expected_rtt: u32,

    /// Value in bytes/s, default with expected rtt 100 is 100Mbps
    #[clap(long, default_value_t = 12_500 * 1_000)]
    max_stream_bandwidth: u32,
}

/// Dummy certificate verifier that treats any certificate as valid.
/// NOTE, such verification is vulnerable to MITM attacks, but convenient for testing.
#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();

    let builder = if args.insecure {
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_no_client_auth()
    } else if let Some(cert_path) = args.cert {
        let mut roots = rustls::RootCertStore::empty();
        let cert = fs::read(cert_path)
            .await
            .context("failed to read certificate chain")?;
        roots.add(CertificateDer::from(cert))?;
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    } else {
        anyhow::bail!("secure mode require cert")
    };

    let mut transport_config = TransportConfig::default();
    let stream_rwnd = args.max_stream_bandwidth / 1_000 * args.expected_rtt;
    transport_config.stream_receive_window(stream_rwnd.into());
    transport_config.send_window(8 * stream_rwnd as u64);
    transport_config.datagram_receive_buffer_size(Some(stream_rwnd as usize));

    let crypto_config = Arc::new(QuicClientConfig::try_from(builder)?);
    let mut client_config = ClientConfig::new(crypto_config);
    client_config.transport_config(Arc::new(transport_config));

    let mut endpoint = Endpoint::client("[::]:0".parse()?)?;
    endpoint.set_default_client_config(client_config);

    let conn = endpoint
        .connect(args.endpoint.parse()?, "localhost")?
        .await?;
    info!("connected to addr {}", conn.remote_address());

    // let mut send = conn.open_uni().await?;
    // send.write_all(b"hello").await?;
    // send.finish()?;
    // drop(conn);

    let mut stream = conn.accept_uni().await?;
    let mut buf = vec![0; 128 * 1024 * 1024];
    let mut pb = BenchProgressBar::default();
    loop {
        let size = stream.read_u64().await? as usize;
        stream.read_exact(&mut buf.as_mut_slice()[0..size]).await?;

        let message = SubscribeUpdate::decode(&buf.as_slice()[0..size])?;
        if let Some(UpdateOneof::BlockMeta(meta)) = &message.update_oneof {
            pb.set_slot(meta.slot);
        }
        pb.inc(message.encoded_len());
    }

    #[allow(unreachable_code)]
    drop(conn);

    endpoint.wait_idle().await;

    anyhow::bail!("stream failed");
}
