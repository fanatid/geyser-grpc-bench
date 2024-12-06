use {
    anyhow::Context,
    clap::Parser,
    futures::future::try_join_all,
    geyser_grpc_bench::{BenchProgressBar, QuicStreamRequest},
    log::info,
    prost::Message,
    quinn::{crypto::rustls::QuicClientConfig, ClientConfig, Endpoint, TransportConfig},
    rustls::{
        pki_types::{CertificateDer, ServerName, UnixTime},
        RootCertStore,
    },
    std::{collections::HashMap, env, path::PathBuf, sync::Arc},
    tokio::{fs, io::AsyncReadExt, net::lookup_host, sync::mpsc},
    yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeUpdate},
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("127.0.0.1:10002"))]
    endpoint: String,

    /// Override hostname used for certificate verification
    #[clap(long)]
    host: String,

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

    /// Number of quic streams
    #[clap(long, default_value_t = 1)]
    config_streams: u32,

    #[clap(long, default_value_t = 65_536)]
    config_max_backlog: u32,
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
    } else {
        let mut roots = RootCertStore::empty();
        // native
        let rustls_native_certs::CertificateResult { certs, errors, .. } =
            rustls_native_certs::load_native_certs();
        if !errors.is_empty() {
            log::error!("errors occured when loading native certs: {errors:?}");
        }
        roots.add_parsable_certificates(certs);
        // webpki
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        // custom
        if let Some(cert_path) = args.cert {
            let cert_chain = fs::read(&cert_path)
                .await
                .context("failed to read certificate chain")?;
            if cert_path.extension().is_some_and(|x| x == "der") {
                roots.add(CertificateDer::from(cert_chain))?;
            } else {
                for cert in rustls_pemfile::certs(&mut &*cert_chain) {
                    roots.add(cert.context("invalid PEM-encoded certificate")?)?;
                }
            }
        }
        // build
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    };

    let mut transport_config = TransportConfig::default();
    transport_config.max_concurrent_bidi_streams(0u8.into());
    transport_config.max_concurrent_uni_streams(args.config_streams.into());
    let stream_rwnd = args.max_stream_bandwidth / 1_000 * args.expected_rtt;
    transport_config.stream_receive_window(stream_rwnd.into());
    transport_config.send_window(8 * stream_rwnd as u64);
    transport_config.datagram_receive_buffer_size(Some(stream_rwnd as usize));

    let crypto_config = Arc::new(QuicClientConfig::try_from(builder)?);
    let mut client_config = ClientConfig::new(crypto_config);
    client_config.transport_config(Arc::new(transport_config));

    let mut endpoint = Endpoint::client("[::]:0".parse()?)?;
    endpoint.set_default_client_config(client_config);

    let addr = lookup_host(&args.endpoint)
        .await
        .context("failed to lookup")?
        .find(|addr| addr.is_ipv4())
        .ok_or(anyhow::anyhow!("failed to lookup ipv4 addr"))?;
    let conn = endpoint.connect(addr, &args.host)?.await?;
    info!("connected to addr {}", conn.remote_address());

    let mut send = conn.open_uni().await?;
    send.write_all(
        &serde_json::to_vec(&QuicStreamRequest {
            streams: args.config_streams,
            max_backlog: args.config_max_backlog,
        })
        .context("failed to create quic request")?,
    )
    .await?;
    send.finish()?;

    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut readers = Vec::with_capacity(args.config_streams as usize);
    for _ in 0..args.config_streams {
        let mut stream = conn.accept_uni().await?;
        let tx = tx.clone();
        readers.push(tokio::spawn(async move {
            let mut buf = vec![0; 128 * 1024 * 1024];
            loop {
                let msg_id = stream.read_u64().await?;
                let size = stream.read_u64().await? as usize;
                stream.read_exact(&mut buf.as_mut_slice()[0..size]).await?;
                let message = SubscribeUpdate::decode(&buf.as_slice()[0..size])?;
                tx.send((msg_id, message))?;
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        }));
    }
    let read_fut = try_join_all(readers);
    tokio::pin!(read_fut);

    let mut pb = BenchProgressBar::default();
    let mut msg_id = 0u64;
    let mut messages = HashMap::new();
    loop {
        tokio::select! {
            result = &mut read_fut => anyhow::bail!("readers failed/finished: {result:?}"),
            message = rx.recv() => match message {
                Some((id, message)) => messages.insert(id, message),
                None => anyhow::bail!("failed to get message from the channel"),
            },
        };

        while let Some(message) = messages.remove(&msg_id) {
            msg_id = msg_id.wrapping_add(1);

            if let Some(UpdateOneof::BlockMeta(meta)) = &message.update_oneof {
                pb.set_slot(meta.slot);
            }
            pb.inc(message.encoded_len());
        }
    }

    #[allow(unreachable_code)]
    drop(conn);

    endpoint.wait_idle().await;

    anyhow::bail!("stream failed");
}
