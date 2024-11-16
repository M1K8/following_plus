use futures_util::StreamExt;
use graph::GraphModel;
use tokio_tungstenite::connect_async;

pub mod bsky;
pub mod graph;

const URL: &str = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.graph.*&wantedCollections=app.bsky.feed.*";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph = GraphModel::new("bolt://localhost:7687", "user", "pass")
        .await
        .unwrap();

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
                    Err(e) => println!("Error handling event: {}", e),
                };
            }
            None => {}
        }
    }

    Ok(())
}
