use crate::graph::GraphModel;
use regex::Regex;
use serde_derive::Deserialize;
use serde_derive::Serialize;

pub async fn handle_event(
    evt: Result<tokio_tungstenite::tungstenite::Message, tokio_tungstenite::tungstenite::Error>,
    g: &mut GraphModel,
) -> Result<(), Box<dyn std::error::Error>> {
    match evt {
        Ok(msg) => {
            let deser_evt: BskyEvent = match serde_json::from_slice(msg.into_data().as_slice()) {
                Ok(m) => m,
                Err(err) => {
                    return Err(err.into());
                }
            };
            let commit: &Commit = match &deser_evt.commit {
                Some(m) => m,
                None => {
                    return Ok(());
                }
            };

            if commit.operation == "create" {
                match commit.collection.as_str() {
                    "app.bsky.feed.post" => {
                        g.add_post(deser_evt.did, commit.cid.clone().unwrap())
                            .await?;
                    }
                    "app.bsky.feed.like" => {
                        let mut did_in = "";
                        let mut cid = "";
                        match &commit.record {
                            Some(r) => {
                                cid = match &r.subject {
                                    Some(s) => match s {
                                        Subj::T1(_) => "",
                                        Subj::T2(subject) => {
                                            let re = Regex::new(r"did:plc:[a-zA-Z0-9]+").unwrap();
                                            if let Some(mat) = re.find(&subject.uri) {
                                                did_in = mat.as_str();
                                            }
                                            &subject.cid
                                        }
                                    },
                                    None => "",
                                };
                            }
                            None => {}
                        }
                        if did_in == "" {
                            panic!("empty did_in");
                        }

                        if cid == "" {
                            panic!("empty cid");
                        }

                        g.add_like(deser_evt.did, did_in.to_string(), cid.to_string())
                            .await?;
                    }
                    _ => {
                        //println!("{:?}", commit.collection.as_str());
                    }
                }
            }

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
