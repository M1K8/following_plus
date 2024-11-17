use std::thread;

use futures_util::{FutureExt, StreamExt};
use graph::GraphModel;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;

pub mod bsky;
pub mod graph;

const URL: &str = "wss://jetstream1.us-west.bsky.network/subscribe?wantedCollections=app.bsky.graph.*&wantedCollections=app.bsky.feed.*";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (send, recv) = mpsc::channel::<String>(100);
    let mut graph = GraphModel::new("bolt://localhost:7687", "user", "pass", recv)
        .await
        .unwrap();
    thread::spawn(move || {
        let web_runtime: tokio::runtime::Runtime = tokio::runtime::Runtime::new().unwrap();
        println!("Starting web listener thread");
        let wait = web_runtime.spawn(async move {});
        web_runtime.block_on(wait).unwrap();
        println!("Exiting web listener thread");
    });

    // Connect to the websocket
    let (ws_stream, _) = connect_async(URL).await?;
    println!("Connected to Bluesky firehose");
    // Split the websocket into sender and receiver
    let (_, mut read) = ws_stream.split();

    while let Some(message) = read.next().await {
        match Some(message) {
            Some(m) => {
                match bsky::handle_event(m, &mut graph).await {
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
