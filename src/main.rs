use common::FetchMessage;
use graph::GraphModel;
use pprof::protos::Message;
use rustls::crypto::CryptoProvider;
use simple_moving_average::{SumTreeSMA, SMA};
use std::sync::Arc;
use std::time::SystemTime;
use std::{env, mem, process};
use std::{fs::File, io::Write, thread};
use tokio::sync::{mpsc, Mutex, RwLock};
use tracing::{error, info, warn};
use tracing_subscriber;

pub mod bsky;
pub mod common;
pub mod graph;
mod server;
mod ws;

/// Override the global allocator with mimalloc
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let compression = env::var("COMPRESS_ENABLE").unwrap_or("".into());
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");
    tracing_subscriber::fmt::init();

    // todo - this properly
    let user = env::var("MM_USER").unwrap_or("user".into());
    let pw = env::var("MM_PW").unwrap_or("pass".into());

    let lock = Arc::new(RwLock::new(()));
    let (send, recv) = mpsc::channel::<FetchMessage>(100);
    info!("Connecting to memgraph");
    let mut graph = GraphModel::new(
        "bolt://localhost:7687",
        "bolt://localhost:7688",
        &user,
        &pw,
        recv,
        lock.clone(),
    )
    .await
    .unwrap();
    info!("Connected to memgraph");

    // Connect to the websocket
    info!("Connecting to Bluesky firehose");
    let compressed = !compression.is_empty();
    let url = format!("wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.graph.*&wantedCollections=app.bsky.feed.*&compress={}", compressed);
    let url2 = format!("wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.graph.*&wantedCollections=app.bsky.feed.*&compress={}", compressed);
    let mut ws = ws::connect("jetstream1.us-east.bsky.network", url.clone()).await?;
    info!("Connected to Bluesky firehose");
    let ma = SumTreeSMA::<_, i64, 15000>::new();
    let ctr = Arc::new(Mutex::new(ma));
    let ctr2 = ctr.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            let avg = ctr2.lock().await.get_average();
            if avg > 50000 {
                error!("Something wrong!");
                panic!()
            }
            info!("Average drift over 60s: {}ms", avg);
        }
    });
    let mut last_time = SystemTime::now();
    let mut recv: Option<mpsc::Receiver<()>> = None; // Has to be an option otherwise mem::take wont work (bc it implements default())
                                                     //TODO - If it looks like we're stalled on memgraph, disconnect from the DB & reconn (probably by adding a select to enqueue_query & having it call an internal method to reset)
    'outer: loop {
        while let Ok(msg) = tokio::select! {
            msg = ws.read_frame() => {
                msg
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                info!("Reconnecting to Bluesky firehose (& memgraph)");
                let nu_url = url.clone() + format!("&cursor={}",&last_time.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros()).as_str();
                ws = match ws::connect("jetstream1.us-east.bsky.network", nu_url).await{
                    Ok(ws) => {
                        ws
                    },
                    Err(e) => {
                        info!("Error reconnecting to firehose: {}", e);
                        continue 'outer;
                    }
                };
                info!("Reconnected to Bluesky firehose");
                let m = ws.read_frame().await;

                graph.reset_connection().await.unwrap();


                info!("We good");
                m
            }
        } {
            match msg.opcode {
                fastwebsockets::OpCode::Binary | fastwebsockets::OpCode::Text => {
                    match msg.payload {
                        fastwebsockets::Payload::Bytes(m) => {
                            let l = lock.read().await;
                            let rec = mem::take(&mut recv);

                            match bsky::handle_event_fast(&m, &mut graph, rec, compressed).await {
                                Err(e) => info!("Error handling event: {}", e),
                                Ok((drift, recv_chan)) => {
                                    if drift > 10000 || drift < 0 {
                                        info!("Weird Drift: {}ms", drift);
                                        info!("Reconnecting to Bluesky firehose");
                                        let nu_url = url2.clone()
                                            + format!(
                                                "&cursor={}",
                                                &last_time
                                                    .duration_since(SystemTime::UNIX_EPOCH)
                                                    .unwrap()
                                                    .as_micros()
                                            )
                                            .as_str();
                                        // switch to jetstream2
                                        warn!("{nu_url}");
                                        ws = match ws::connect(
                                            "jetstream2.us-east.bsky.network",
                                            nu_url,
                                        )
                                        .await
                                        {
                                            Ok(ws) => ws,
                                            Err(e) => {
                                                info!("Error reconnecting to firehose: {}", e);
                                                continue 'outer;
                                            }
                                        };
                                        info!("Reconnected to Bluesky firehose");
                                    }
                                    ctr.lock().await.add_sample(drift);
                                    last_time = SystemTime::now();
                                    if recv_chan.is_some() {
                                        recv = recv_chan;
                                    }
                                }
                            }

                            drop(l);
                        }
                        _ => {
                            panic!("Unsupported payload type {:?}", msg.payload);
                        }
                    };
                }
                fastwebsockets::OpCode::Close => {
                    info!("Closing connection, trying to reopen...");
                    ws = ws::connect("jetstream1.us-east.bsky.network", url.clone()).await?;
                    continue;
                }
                _ => {}
            }
        }
        error!("WS Failed");
        let e = ws.read_frame().await;
        match e {
            Ok(_) => info!("Were ok?"),
            Err(e) => {
                ws = ws::connect("jetstream2.us-east.bsky.network", url2.clone()).await?;
                error!("Err: {e}")
            }
        };
    }
}
