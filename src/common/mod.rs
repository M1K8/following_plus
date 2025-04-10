use crate::server::types;
use serde_derive::Deserialize;
use tokio::sync::mpsc;

#[derive(Debug)]
pub struct FetchMessage {
    pub did: String,
    pub cursor: Option<String>,
    pub resp: mpsc::Sender<PostResp>,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, Deserialize, Default)]
pub struct PostMsg {
    pub uri: String,
    pub reason: String,
    pub timestamp: u64,
}

pub struct PostResp {
    pub posts: Vec<PostMsg>,
    pub cursor: Option<String>,
}

impl From<&PostMsg> for types::Post {
    fn from(value: &PostMsg) -> Self {
        types::Post {
            post: value.uri.clone(),
        }
    }
}

impl PartialOrd for PostMsg {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PostMsg {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.timestamp.cmp(&self.timestamp)
    }
}
