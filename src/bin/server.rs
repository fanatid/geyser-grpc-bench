use {
    anyhow::Context,
    clap::Parser,
    futures::{
        future::{pending, ready, FutureExt},
        sink::SinkExt,
        stream::StreamExt,
    },
    geyser_grpc_bench::{BenchProgressBar, QuicStreamRequest},
    log::{error, info},
    maplit::hashmap,
    prost::Message,
    quinn::{crypto::rustls::QuicServerConfig, Endpoint},
    rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    std::{collections::HashMap, env, path::PathBuf, sync::Arc, time::Duration},
    tokio::{
        fs,
        io::AsyncWriteExt,
        signal::unix::{signal, SignalKind},
        sync::{broadcast, mpsc},
        task::JoinSet,
    },
    tokio_stream::wrappers::ReceiverStream,
    tonic::{
        transport::{
            channel::ClientTlsConfig,
            server::{Server, TcpIncoming},
        },
        Request, Response, Result as TonicResult, Status, Streaming,
    },
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::prelude::{
        geyser_server::{Geyser, GeyserServer},
        subscribe_update::UpdateOneof,
        CommitmentLevel, GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
        GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse, GetVersionRequest,
        GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest,
        PongResponse, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions, SubscribeUpdate,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(long, default_value_t = String::from("http://127.0.0.1:10000"))]
    endpoint: String,

    #[clap(long)]
    x_token: Option<String>,

    #[clap(long)]
    multiplier: usize,

    #[clap(long)]
    grpc_listen: Option<String>,

    #[clap(long, default_value_t = false)]
    grpc_http2_adaptive_window: bool,

    #[clap(long, default_value_t = 65535)]
    grpc_initial_connection_window_size: u32,

    #[clap(long, default_value_t = 65535)]
    grpc_initial_stream_window_size: u32,

    #[clap(long)]
    quic_listen: Option<String>,

    #[clap(long)]
    quic_key: Option<PathBuf>,

    #[clap(long)]
    quic_cert: Option<PathBuf>,

    /// Value in ms
    #[clap(long, default_value_t = 100)]
    quic_expected_rtt: u32,

    /// Value in bytes/s, default with expected rtt 100 is 100Mbps
    #[clap(long, default_value_t = 12_500 * 1_000)]
    quic_max_stream_bandwidth: u32,

    #[clap(long, default_value_t = 128)]
    quic_config_max_connections: u32,

    #[clap(long, default_value_t = false)]
    slow_connection: bool,
}

struct GrpcService {
    tx: broadcast::Sender<TonicResult<(u64, Arc<SubscribeUpdate>)>>,
}

#[tonic::async_trait]
impl Geyser for GrpcService {
    type SubscribeStream = ReceiverStream<TonicResult<SubscribeUpdate>>;

    async fn subscribe(
        &self,
        _request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        let (tx, rx) = mpsc::channel(250_000);

        let mut stream = self.tx.subscribe();
        tokio::spawn(async move {
            while let Ok(msg) = stream.recv().await {
                tx.send(msg.map(|(_, x)| x.as_ref().clone())).await?;
            }
            Ok::<_, anyhow::Error>(())
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn get_latest_blockhash(
        &self,
        _request: Request<GetLatestBlockhashRequest>,
    ) -> Result<Response<GetLatestBlockhashResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn get_block_height(
        &self,
        _request: Request<GetBlockHeightRequest>,
    ) -> Result<Response<GetBlockHeightResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn get_slot(
        &self,
        _request: Request<GetSlotRequest>,
    ) -> Result<Response<GetSlotResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn is_blockhash_valid(
        &self,
        _request: Request<IsBlockhashValidRequest>,
    ) -> Result<Response<IsBlockhashValidResponse>, Status> {
        Err(Status::unimplemented(""))
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Err(Status::unimplemented(""))
    }
}

async fn handle_quic(
    incoming: quinn::Incoming,
    tx: broadcast::Sender<TonicResult<(u64, Arc<SubscribeUpdate>)>>,
) -> anyhow::Result<()> {
    let conn = incoming.await?;

    let mut set = JoinSet::new();
    loop {
        let set_fut = if set.is_empty() {
            pending().boxed()
        } else {
            set.join_next().boxed()
        };
        let (mut send, mut recv) = tokio::select! {
            value = conn.accept_bi() => value?,
            result = set_fut => match result {
                Some(result) => anyhow::bail!("bidi stream result: {result:?}"),
                None => unreachable!(),
            }
        };

        let quic_request_body = recv.read_to_end(1024).await?;
        let QuicStreamRequest {
            connections_current,
            connections_total,
        } = serde_json::from_slice(&quic_request_body).context("failed to parse quic request")?;

        let mut rx = tx.subscribe();
        set.spawn(async move {
            loop {
                let (msg_id, message) = match rx.recv().await {
                    Ok(Ok(value)) => value,
                    Ok(Err(error)) => anyhow::bail!("subscribe update error: {error:?}"),
                    Err(_error) => anyhow::bail!("broadcast channel is closed"),
                };

                if msg_id % connections_total == connections_current {
                    let message = message.encode_to_vec();
                    send.write_u64(msg_id).await?;
                    send.write_u64(message.len() as u64).await?;
                    send.write_all(&message).await?;
                }
            }

            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        });
    }

    #[allow(unreachable_code)]
    drop(conn);

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();

    let (tx, _rx) = broadcast::channel::<TonicResult<(u64, Arc<SubscribeUpdate>)>>(524_288); // 2**19

    let stream_tx = tx.clone();
    let stream_jh = tokio::spawn(async move {
        let (accounts, transactions) = if args.slow_connection {
            (HashMap::new(), HashMap::new())
        } else {
            (
                hashmap! { "".to_owned() => SubscribeRequestFilterAccounts {
                    ..Default::default()
                } },
                hashmap! { "".to_owned() => SubscribeRequestFilterTransactions {
                    ..Default::default()
                } },
            )
        };
        let request = SubscribeRequest {
            slots: hashmap! { "".to_owned() => SubscribeRequestFilterSlots { ..Default::default() } },
            accounts,
            transactions,
            transactions_status: HashMap::new(),
            entry: hashmap! { "".to_owned() => SubscribeRequestFilterEntry {} },
            blocks: HashMap::new(),
            blocks_meta: hashmap! { "".to_owned() => SubscribeRequestFilterBlocksMeta {} },
            commitment: Some(CommitmentLevel::Processed as i32),
            accounts_data_slice: vec![],
            ping: None,
        };

        let mut client = GeyserGrpcClient::build_from_shared(args.endpoint)?
            .x_token(args.x_token)?
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .max_decoding_message_size(128 * 1024 * 1024) // 128MiB for block meta with rewards
            .connect()
            .await?;
        let (mut subscribe_tx, mut stream) = client.subscribe().await?;
        subscribe_tx.send(request).await?;

        let mut pb = BenchProgressBar::default();
        let mut msg_id = 0;
        while let Some(Ok(message)) = stream.next().await {
            if let Some(UpdateOneof::BlockMeta(meta)) = &message.update_oneof {
                pb.set_slot(meta.slot);
            }
            pb.inc(message.encoded_len());

            let message = Arc::new(message);
            for _ in 0..args.multiplier {
                let _ = stream_tx.send(Ok((msg_id, Arc::clone(&message))));
                msg_id = msg_id.wrapping_add(1);
            }
        }

        anyhow::bail!("stream failed");
    });

    let grpc_tx = tx.clone();
    let server_grpc_jh = if let Some(addr) = args.grpc_listen {
        tokio::spawn(async move {
            // Bind service address
            let incoming = TcpIncoming::new(
                addr.parse()?,
                true,                          // tcp_nodelay
                Some(Duration::from_secs(20)), // tcp_keepalive
            )
            .map_err(|error| anyhow::anyhow!(error))?;
            info!("start listen grpc on {addr}");

            let service = GeyserServer::new(GrpcService { tx: grpc_tx });

            Server::builder()
                .http2_adaptive_window(Some(args.grpc_http2_adaptive_window))
                .initial_connection_window_size(args.grpc_initial_connection_window_size)
                .initial_stream_window_size(args.grpc_initial_stream_window_size)
                .add_service(service)
                .serve_with_incoming(incoming)
                .await?;

            anyhow::bail!("grpc server failed")
        })
        .boxed()
    } else {
        ready(Ok(Ok(()))).boxed()
    };

    let server_quic_jh = if let Some(addr) = args.quic_listen {
        tokio::spawn(async move {
            let (certs, key) =
                if let (Some(key_path), Some(cert_path)) = (args.quic_key, args.quic_cert) {
                    let key = fs::read(&key_path)
                        .await
                        .context("failed to read private key")?;
                    let key = if key_path.extension().is_some_and(|x| x == "der") {
                        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key))
                    } else {
                        rustls_pemfile::private_key(&mut &*key)
                            .context("malformed PKCS #1 private key")?
                            .ok_or_else(|| anyhow::Error::msg("no private keys found"))?
                    };

                    let cert_chain = fs::read(&cert_path)
                        .await
                        .context("failed to read certificate chain")?;
                    let cert_chain = if cert_path.extension().is_some_and(|x| x == "der") {
                        vec![CertificateDer::from(cert_chain)]
                    } else {
                        rustls_pemfile::certs(&mut &*cert_chain)
                            .collect::<Result<_, _>>()
                            .context("invalid PEM-encoded certificate")?
                    };

                    (cert_chain, key)
                } else {
                    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])
                        .context("failed to generated self-signed cert")?;
                    let cert_der = CertificateDer::from(cert.cert);
                    let priv_key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
                    (vec![cert_der], priv_key.into())
                };

            let server_crypto = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)?;

            let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
                QuicServerConfig::try_from(server_crypto)?,
            ));
            let transport_config = Arc::get_mut(&mut server_config.transport).unwrap();
            transport_config.max_concurrent_bidi_streams(args.quic_config_max_connections.into());
            transport_config.max_concurrent_uni_streams(0u8.into());
            let stream_rwnd = args.quic_max_stream_bandwidth / 1_000 * args.quic_expected_rtt;
            transport_config.stream_receive_window(stream_rwnd.into());
            transport_config.send_window(8 * stream_rwnd as u64);
            transport_config.datagram_receive_buffer_size(Some(stream_rwnd as usize));

            let endpoint = Endpoint::server(server_config, addr.parse()?)?;
            info!("start listen quic on {addr}");

            while let Some(incoming) = endpoint.accept().await {
                let tx = tx.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_quic(incoming, tx.clone()).await {
                        error!("quic connection failed: {error:?}");
                    }
                });
            }

            anyhow::bail!("quic server failed")
        })
        .boxed()
    } else {
        ready(Ok(Ok(()))).boxed()
    };

    let _ = tokio::try_join!(
        async move {
            stream_jh.await??;
            Ok::<_, anyhow::Error>(())
        },
        async move {
            server_grpc_jh.await??;
            Ok::<_, anyhow::Error>(())
        },
        async move {
            server_quic_jh.await??;
            Ok::<_, anyhow::Error>(())
        },
        async {
            let mut sigint = signal(SignalKind::interrupt())?;
            sigint.recv().await;
            anyhow::bail!("sigint");
            #[allow(unreachable_code)]
            Ok(())
        }
    )?;
    Ok(())
}
