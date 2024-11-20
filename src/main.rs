use common::FetchMessage;
use futures_util::StreamExt;
use graph::GraphModel;
use pprof::protos::Message;
use std::{env, process};
use std::{fs::File, io::Write, thread};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;

pub mod bsky;
pub mod common;
pub mod graph;
mod server;

const URL: &str = "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.graph.*&wantedCollections=app.bsky.feed.*";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let profile = env::var("PROFILE_ENABLE").unwrap_or("".into());
    let compression = env::var("COMPRESS_ENABLE").unwrap_or("".into());
    let compress;
    if !compression.is_empty() {
        println!("Compression enabled");
        compress = true;
    } else {
        compress = false;
    }
    if !profile.is_empty() {
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(1000)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .unwrap();

        ctrlc::set_handler(move || {
            println!("Shutting down");
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
            //TODO Exit properly
            process::exit(0x0100);
        })
        .expect("Error setting Ctrl-C handler");
    }

    let (send, recv) = mpsc::channel::<FetchMessage>(100);
    let mut graph = GraphModel::new("bolt://localhost:7687", "user", "pass", recv)
        .await
        .unwrap();
    let server_conn = graph.inner();
    thread::spawn(move || {
        let web_runtime: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
        println!("Starting web listener thread");
        let wait = web_runtime.spawn(async move {
            server::serve(send, server_conn).await.unwrap();
        });
        web_runtime.block_on(wait).unwrap();
        println!("Exiting web listener thread");
    });

    //

    // Connect to the websocket
    let url;
    if compress {
        url = format!("wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.graph.*&wantedCollections=app.bsky.feed.*&compress={}", "true");
    } else {
        url =  format!("wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.graph.*&wantedCollections=app.bsky.feed.*&compress={}", "false");
    }
    let (ws_stream, _) = connect_async(url).await?;
    println!("Connected to Bluesky firehose");
    // Split the websocket into sender and receiver
    let (_, mut read) = ws_stream.split();

    while let Some(message) = read.next().await {
        match Some(message) {
            Some(m) => {
                match bsky::handle_event(m, &mut graph, compress).await {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Error handling event: {}", e);
                        let (stream, _) = connect_async(URL).await?;
                        read = stream.split().1;
                    }
                };
            }
            None => {}
        }
    }

    Ok(())
}

//Todo:
/*
- Forward port 80 & 443 to this ip (connect over wifi)
- Get Cert from Let's Encrypt
- Setup server
- Test feed
- Unregister
- (and also prune gh tickets xx)




For impl:
- Store languages
- Implement messaging logic to request fetches for first user case
    - Match(u:User) return U; if u.fetched == null, fetch followers, follows & write back to DB
        - Worth creating accounts if they dont exist even with 0 posts, because otherwise it wont be recorded
- For a start try returning 2nd degree posts to test latency
- Add read replica
- Have some kind of cursoring; either pre-fetching a load and trickle-feeding it back, or take note of the oldest post returned & use that in query
(e.g if we LIMIT 50, then the next query will match only for p.timestamp < cursor'd timestamp)
- Tweak algo+++++
*/
