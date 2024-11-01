use serde_derive::Deserialize;
use serde_derive::Serialize;

pub async fn handle_event(
    evt: Result<tokio_tungstenite::tungstenite::Message, tokio_tungstenite::tungstenite::Error>,
) -> Result<(), Box<dyn std::error::Error>> {
    match evt {
        Ok(msg) => {
            let deser_evt: BskyEvent = match serde_json::from_slice(msg.into_data().as_slice()) {
                Ok(m) => m,
                Err(err) => {
                    return Err(err.into());
                }
            };
            println!("{:?}", deser_evt);
            return Ok(());
        }
        Err(err) => {
            return Err(err.into());
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BskyEvent {
    pub did: String,
    #[serde(rename = "time_us")]
    pub time_us: i64,
    pub kind: String,
    pub commit: Option<Commit>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Commit {
    pub rev: String,
    pub operation: String,
    pub collection: String,
    pub rkey: String,
    pub record: Option<Record>,
    pub cid: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    #[serde(rename = "$type")]
    pub type_field: String,
    pub created_at: String,
    pub subject: Option<Subj>,
    pub lang: Option<Vec<String>>,
    pub text: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subject {
    pub cid: String,
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Subj {
    T1(String),
    T2(Subject),
}
