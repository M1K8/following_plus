use at_event_processor::MaybeSemaphore;
use bsky::types::ATEventType;
use common::FetchMessage;
use filter::FilterList;
use pprof::protos::Message;
use processor::MemgraphWrapper;
use simple_moving_average::{SMA, SumTreeSMA};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::SystemTime;
use std::{env, mem, process};
use std::{fs::File, io::Write, thread};
use tokio::sync::{Mutex, RwLock, mpsc};
use tracing::{error, info, warn};
use tracing_subscriber;

mod at_event_processor;
pub mod bsky;
pub mod common;
mod event_database;
mod filter;
mod forward_server;
pub mod graph;
mod processor;
mod server;
mod ws;

//RUSTFLAGS="-Cprofile-generate=./pgo-data"     cargo build --release --target=x86_64-unknown-linux-gnu
/// Override the global allocator with mimalloc
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");
    tracing_subscriber::fmt::init();

    if !env::var("PROFILE_ENABLE").unwrap_or("".into()).is_empty() {
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(1000)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .unwrap();

        ctrlc::set_handler(move || {
            info!("Shutting down");
            match guard.report().build() {
                Ok(report) => {
                    let mut file = File::create("profile.pb").unwrap();
                    let profile = report.pprof().unwrap();

                    let mut content = Vec::new();
                    profile.write_to_vec(&mut content).unwrap();
                    file.write_all(&content).unwrap();
                }
                Err(_) => {}
            };
            process::exit(0x0100);
        })
        .expect("Error setting Ctrl-C handler");
    }

    // todo - this properly
    let compression = env::var("COMPRESS_ENABLE").unwrap_or("".into());
    let forward_mode = env::var("FORWARD_MODE").unwrap_or("".into());
    let user = env::var("MM_USER").unwrap_or("user".into());
    let pw = env::var("MM_PW").unwrap_or("pass".into());
    //
    let lock = Arc::new(RwLock::new(()));
    let (send_channel, recieve_channel) = mpsc::channel::<FetchMessage>(100);
    // If env says we need to forward DB requests, just do that & nothing else
    if !forward_mode.is_empty() {
        info!("Starting forward web server");
        forward_server::serve(forward_mode).await.unwrap();
        info!("Exiting forward web server");
        return Ok(());
    } else {
        // Otherwise, spin this off to accept incoming requests (feed serving atm, will likely just be DB reads)
        thread::spawn(move || {
            let web_runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(16)
                .build()
                .unwrap();

            info!("Starting web listener thread");
            let wait = web_runtime.spawn(async move {
                server::serve(send_channel).await.unwrap();
            });
            web_runtime.block_on(wait).unwrap();
            info!("Exiting web listener thread");
        });
        //
    }

    // Define the filters & scopes we want applied to events
    let mut filters = HashMap::new();

    let mut global_filters: FilterList = VecDeque::new();
    global_filters.push_front(Box::new(filter::date_filter));

    let mut post_filters: FilterList = VecDeque::new();
    post_filters.push_front(Box::new(filter::spam_filter));

    let mut repost_filters: FilterList = VecDeque::new();
    repost_filters.push_front(Box::new(filter::spam_filter));

    filters.insert(ATEventType::Post, post_filters);
    filters.insert(ATEventType::Repost, repost_filters);
    filters.insert(ATEventType::Global, global_filters);
    //

    info!("Connecting to memgraph");
    let mut graph = MemgraphWrapper::new(
        "bolt://localhost:7687",
        "bolt://localhost:7688",
        &user,
        &pw,
        recieve_channel,
        lock.clone(),
        filters,
    )
    .await
    .unwrap();
    info!("Connected to memgraph");

    // Connect to the websocket
    info!("Connecting to Bluesky firehose");
    let compressed = !compression.is_empty();
    let url = format!(
        "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.graph.*&wantedCollections=app.bsky.feed.*&compress={}",
        compressed
    );
    let url2 = format!(
        "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.graph.*&wantedCollections=app.bsky.feed.*&compress={}",
        compressed
    );
    let mut ws = ws::connect("jetstream1.us-east.bsky.network", url.clone()).await?;
    info!("Connected to Bluesky firehose");
    let ma = SumTreeSMA::<_, i64, 25000>::new();
    let ctr = Arc::new(Mutex::new(ma));
    let ctr2 = ctr.clone();

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            let avg = ctr2.lock().await.get_average();
            if avg > 50000 {
                panic!("Something wrong - drift too high!")
            }
            info!("Average drift over 60s: {}ms", avg);
        }
    });
    let mut last_time = SystemTime::now();
    let mut recv: MaybeSemaphore = None; // Has to be an option otherwise mem::take wont work (bc it implements default())

    loop {
        match tokio::select! {
            msg = ws.read_frame() => {
                msg
            }
            _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                info!("Reconnecting to Bluesky firehose");
                let nu_url = url.clone() + format!("&cursor={}",&last_time.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros()).as_str();
                ws = match ws::connect("jetstream1.us-east.bsky.network", nu_url).await{
                    Ok(ws) => {
                        ws
                    },
                    Err(e) => {
                        info!("Error reconnecting to firehose: {}", e);
                        continue;
                    }
                };
                info!("Reconnected to Bluesky firehose");
                let m = ws.read_frame().await;

                info!("We good");
                m
            }
        } {
            // TODO - Write test EventDatabase impl to check params are being processed properly, then chain w/ test ATEventProcessor
            Ok(msg) => {
                match msg.opcode {
                    fastwebsockets::OpCode::Binary | fastwebsockets::OpCode::Text => {
                        match msg.payload {
                            fastwebsockets::Payload::Bytes(m) => {
                                let l = lock.read().await;
                                let rec = mem::take(&mut recv);

                                match bsky::handle_event_fast(&m, &mut graph, rec, compressed).await
                                {
                                    Err(e) => info!("Error handling event: {}", e),
                                    Ok((drift, recv_chan)) => {
                                        if drift > 10000 || drift < 0 {
                                            info!("Weird Drift: {}ms", drift);
                                            info!(
                                                "Reconnecting to Bluesky firehose, falling back to jetstream2"
                                            );
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
                                            ws = match ws::connect(
                                                "jetstream2.us-east.bsky.network",
                                                nu_url,
                                            )
                                            .await
                                            {
                                                Ok(ws) => ws,
                                                Err(e) => {
                                                    info!("Error reconnecting to firehose: {}", e);
                                                    continue;
                                                }
                                            };
                                            info!("Reconnected to Bluesky jetstream2");
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
                        loop {
                            match ws::connect("jetstream1.us-east.bsky.network", url.clone()).await
                            {
                                Ok(w) => {
                                    ws = w;
                                    break;
                                }
                                Err(e) => {
                                    error!("error reconnecting, trying again: {e}")
                                }
                            };
                        }
                        continue;
                    }
                    _ => {
                        warn! {"Unexpected opcode: {:?}", msg.opcode};
                    }
                }
            }
            Err(e) => {
                error!("WS Failed with error {e}, trying again");
                loop {
                    match ws::connect("jetstream1.us-east.bsky.network", url.clone()).await {
                        Ok(w) => {
                            ws = w;
                            break;
                        }
                        Err(e) => {
                            error!("error reconnecting, trying again: {e}")
                        }
                    };
                }
            }
        }
    }
}
