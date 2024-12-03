use {
    clap::Parser,
    futures::{sink::SinkExt, stream::StreamExt},
    geyser_grpc_bench::format_thousands,
    indicatif::{ProgressBar, ProgressStyle},
    maplit::hashmap,
    prost::Message,
    std::{collections::HashMap, env, time::Duration},
    tokio::{
        signal::unix::{signal, SignalKind},
        sync::{broadcast, mpsc},
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
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    endpoint: String,

    #[clap(long)]
    x_token: Option<String>,

    #[clap(long)]
    multiplier: usize,

    #[clap(long)]
    grpc_listen: String,
}

struct GrpcService {
    tx: broadcast::Sender<TonicResult<SubscribeUpdate>>,
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
                tx.send(msg).await?;
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();

    let (tx, _rx) = broadcast::channel::<TonicResult<SubscribeUpdate>>(524_288); // 2**19

    let stream_tx = tx.clone();
    let stream_jh = tokio::spawn(async move {
        let mut client = GeyserGrpcClient::build_from_shared(args.endpoint)?
            .x_token(args.x_token)?
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .max_decoding_message_size(128 * 1024 * 1024) // 128MiB for block meta with rewards
            .connect()
            .await?;
        let (mut subscribe_tx, mut stream) = client.subscribe().await?;
        subscribe_tx
            .send(SubscribeRequest {
                slots: hashmap! { "".to_owned() => SubscribeRequestFilterSlots { ..Default::default() } },
                accounts: hashmap! { "".to_owned() => SubscribeRequestFilterAccounts{
                    ..Default::default()
                } },
                transactions: hashmap! { "".to_owned() => SubscribeRequestFilterTransactions {
                    ..Default::default()
                } },
                transactions_status: HashMap::new(),
                entry: hashmap! { "".to_owned() => SubscribeRequestFilterEntry {} },
                blocks: HashMap::new(),
                blocks_meta: hashmap! { "".to_owned() => SubscribeRequestFilterBlocksMeta {} },
                commitment: Some(CommitmentLevel::Processed as i32),
                accounts_data_slice: vec![],
                ping: None,
            })
            .await?;

        let pb = ProgressBar::no_length();
        pb.set_style(ProgressStyle::with_template(
            "{spinner} received messages: {msg} / ~{bytes}",
        )?);

        let mut counter = 0;
        let mut slot = 0;
        while let Some(Ok(message)) = stream.next().await {
            if let Some(UpdateOneof::BlockMeta(meta)) = &message.update_oneof {
                slot = meta.slot;
            }

            counter += 1;
            pb.set_message(format!("{} / slot {}", format_thousands(counter), slot));
            pb.inc(message.encoded_len() as u64);

            for _ in 0..args.multiplier {
                let _ = stream_tx.send(Ok(message.clone()));
            }
        }

        anyhow::bail!("stream failed");
    });

    let server_jh = tokio::spawn(async move {
        // Bind service address
        let incoming = TcpIncoming::new(
            args.grpc_listen.parse()?,
            true,                          // tcp_nodelay
            Some(Duration::from_secs(20)), // tcp_keepalive
        )
        .map_err(|error| anyhow::anyhow!(error))?;

        let service = GeyserServer::new(GrpcService { tx });

        Server::builder()
            // .http2_keepalive_interval(Some(Duration::from_secs(5)))
            .add_service(service)
            .serve_with_incoming(incoming)
            .await?;

        Ok::<_, anyhow::Error>(())
    });

    let _ = tokio::try_join!(
        async move {
            stream_jh.await??;
            Ok::<_, anyhow::Error>(())
        },
        async move {
            server_jh.await??;
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
