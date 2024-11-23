use common::FetchMessage;
use graph::GraphModel;
use pprof::protos::Message;
use std::{env, process};
use std::{fs::File, io::Write, thread};
use tokio::sync::mpsc;

pub mod bsky;
pub mod common;
pub mod graph;
mod server;
mod ws;

//RUSTFLAGS="-Cprofile-generate=./pgo-data"     cargo build --release --target=x86_64-unknown-linux-gnu

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

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


    // Spin this off to accept incoming requests (feed serving atm, will likely just be DB reads)
    thread::spawn(move || {
        let web_runtime: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
        println!("Starting web listener thread");
        let wait = web_runtime.spawn(async move {
            server::serve(send).await.unwrap();
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
    let mut ws = ws::connect("jetstream1.us-east.bsky.network", url).await?;
    println!("Connected to Bluesky firehose");

    while let Ok(msg) = ws.read_frame().await {
        match msg.opcode {
            fastwebsockets::OpCode::Binary | fastwebsockets::OpCode::Text => {
                let pl: bytes::BytesMut;
                match msg.payload {
                    fastwebsockets::Payload::Bytes(bytes_mut) => {
                        pl = bytes_mut;
                    }
                    _ => {
                        panic!("Unsupported payload type {:?}", msg.payload);
                    }
                };
                
            }
            fastwebsockets::OpCode::Close => {
                println!("Closing connection");
                break;
            }
            _ => {}
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
