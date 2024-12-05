use {
    clap::Parser,
    futures::{sink::SinkExt, stream::StreamExt},
    geyser_grpc_bench::BenchProgressBar,
    maplit::hashmap,
    prost::Message,
    std::{collections::HashMap, env},
    tonic::transport::channel::ClientTlsConfig,
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::prelude::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10001"))]
    endpoint: String,

    #[clap(long)]
    x_token: Option<String>,

    #[clap(long, default_value_t = false)]
    http2_adaptive_window: bool,

    #[clap(long, default_value_t = 65535)]
    initial_connection_window_size: u32,

    #[clap(long, default_value_t = 65535)]
    initial_stream_window_size: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();

    let mut client = GeyserGrpcClient::build_from_shared(args.endpoint)?
        .http2_adaptive_window(args.http2_adaptive_window)
        .initial_connection_window_size(args.initial_connection_window_size)
        .initial_stream_window_size(args.initial_stream_window_size)
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

    let mut pb = BenchProgressBar::default();
    while let Some(Ok(message)) = stream.next().await {
        if let Some(UpdateOneof::BlockMeta(meta)) = &message.update_oneof {
            pb.set_slot(meta.slot);
        }
        pb.inc(message.encoded_len());
    }
    anyhow::bail!("stream failed");
}
