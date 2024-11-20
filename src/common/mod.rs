#[derive(Debug)]
pub struct FetchMessage {
    did: String,
    cursor: Option<String>,
}
