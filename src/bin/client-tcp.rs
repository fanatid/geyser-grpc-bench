use {
    clap::Parser,
    geyser_grpc_bench::BenchProgressBar,
    prost::Message,
    std::{env, net::SocketAddr},
    tokio::{io::AsyncReadExt, net::TcpStream, sync::mpsc},
    yellowstone_grpc_proto::prelude::{subscribe_update::UpdateOneof, SubscribeUpdate},
};

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("127.0.0.1:10003"))]
    endpoint: String,

    #[clap(long, default_value_t = true)]
    nodelay: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();

    let addr: SocketAddr = args.endpoint.parse()?;
    let mut stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(args.nodelay)?;

    let (tx, mut rx) = mpsc::channel(128);
    tokio::spawn(async move {
        let gbuf = vec![0; 128 * 1024 * 1024];
        loop {
            let size = stream.read_u64().await.unwrap() as usize;
            let mut buf = gbuf.as_slice()[0..size].to_vec();
            stream.read_exact(buf.as_mut_slice()).await.unwrap();

            let tx = tx.clone();
            tokio::spawn(async move {
                let message = SubscribeUpdate::decode(buf.as_slice()).unwrap();
                let mut slot = None;
                if let Some(UpdateOneof::BlockMeta(meta)) = &message.update_oneof {
                    slot = Some(meta.slot);
                }
                tx.send((slot, buf.len())).await.unwrap();
            });
        }
    });

    let mut pb = BenchProgressBar::default();
    while let Some((slot, len)) = rx.recv().await {
        if let Some(slot) = slot {
            pb.set_slot(slot);
        }
        pb.inc(len);
    }
    #[allow(unreachable_code)]
    Ok(())
}
