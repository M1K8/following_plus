use std::sync::Arc;

use futures_util::StreamExt;
use serde::Serialize;
use tokio_tungstenite::connect_async;

pub mod bsky;
pub mod graph;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Bluesky firehose websocket URL
    let url = "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.graph.*&wantedCollections=app.bsky.feed.*";

    // Connect to the websocket
    let (ws_stream, _) = connect_async(url).await?;
    println!("Connected to Bluesky firehose");

    // Split the websocket into sender and receiver
    let (_write, mut read) = ws_stream.split();

    while let Some(message) = read.next().await {
        match Some(message) {
            Some(m) => {
                let err = bsky::handle_event(m).await;
            }
            None => {}
        }
    }

    Ok(())
}
