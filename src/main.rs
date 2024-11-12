use futures_util::StreamExt;
use graph::GraphModel;
use tokio_tungstenite::connect_async;

pub mod bsky;
pub mod graph;

const URL: &str = "wss://jetstream1.us-east.bsky.network/subscribe?wantedCollections=app.bsky.graph.*&wantedCollections=app.bsky.feed.*";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Bluesky firehose websocket URL

    let mut graph = GraphModel::new("bolt://localhost:7687", "user", "pass")
        .await
        .unwrap();

    // Connect to the websocket
    let (ws_stream, _) = connect_async(URL).await?;
    println!("Connected to Bluesky firehose");

    // Split the websocket into sender and receiver
    let (_write, mut read) = ws_stream.split();

    // Main logic - spin off in separate thread & bung behind queue
    // So we can turn the tap on & off (a la trapwire)
    // i.e have a mtx, lock when pruning (but let the queue build up in the interim)
    // probably have an event queue of ~1000 evts for safety
    while let Some(message) = read.next().await {
        match Some(message) {
            Some(m) => {
                bsky::handle_event(m, &mut graph).await?;
            }
            None => {}
        }
    }

    Ok(())
}
