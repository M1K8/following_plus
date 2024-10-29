use futures_util::StreamExt;
use tokio_tungstenite::connect_async;

pub mod graph;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Bluesky firehose websocket URL
    let url = "wss://jetstream1.us-east.bsky.network/subscribe";

    // Connect to the websocket
    let (ws_stream, _) = connect_async(url).await?;
    println!("Connected to Bluesky firehose");

    // Split the websocket into sender and receiver
    let (_write, mut read) = ws_stream.split();

    while let Some(message) = read.next().await {
        match Some(message) {
            Some(m) => {
                println!("{:?}", m.unwrap().into_text().unwrap());
            }
            None => todo!(),
        }
    }

    Ok(())
}
