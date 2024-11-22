use chrono::Utc;
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
    thread::spawn(move || {
        let web_runtime: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
        println!("Starting web listener thread");
        let wait = web_runtime.spawn(async move {
            server::serve_slim().await.unwrap();
        });
        web_runtime.block_on(wait).unwrap();
        println!("Exiting web listener thread");
    });

    //

    // Connect to the websocket
    let url;
    if false {
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
                let evt: bsky::types::BskyEvent =
                    match serde_json::from_slice(m.unwrap().into_data().as_slice()) {
                        Ok(m) => m,
                        Err(_err) => {
                            
                            panic!("{:?}", _err);
                        }
                    };

                let drift =
                    (Utc::now().naive_utc().and_utc().timestamp_micros() - evt.time_us) / 1000;
                println!("{drift}ms late");
            }
            None => {}
        }
    }

    Ok(())
}
