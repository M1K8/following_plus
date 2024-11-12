use crate::graph::GraphModel;
use serde_derive::Deserialize;
use serde_derive::Serialize;

fn get_post_uri(did: String, rkey: String) -> String {
    format!("at://{did}/app.bsky.feed.post/{rkey}")
}

pub async fn handle_event(
    evt: Result<tokio_tungstenite::tungstenite::Message, tokio_tungstenite::tungstenite::Error>,
    g: &mut GraphModel,
) -> Result<(), Box<dyn std::error::Error>> {
    match evt {
        Ok(msg) => {
            let mm = msg.clone().into_text();
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

            // Check for deletes too (unlikes, unfollows)
            if commit.operation == "create" {
                match commit.collection.as_str() {
                    "app.bsky.feed.post" => {
                        let did = deser_evt.did.clone();
                        let rkey = commit.rkey.clone();
                        g.add_post(
                            deser_evt.did,
                            commit.cid.clone().unwrap(),
                            get_post_uri(did, rkey),
                        )
                        .await?;
                    }
                    "app.bsky.feed.repost" => {
                        let mut cid = "";
                        match &commit.record {
                            Some(r) => {
                                cid = match &r.subject {
                                    Some(s) => match s {
                                        Subj::T1(_) => "",
                                        Subj::T2(subject) => &subject.cid,
                                    },
                                    None => "",
                                };
                            }
                            None => {}
                        }

                        if cid == "" {
                            panic!("empty cid");
                        }

                        g.add_repost(deser_evt.did, cid.to_string()).await?;
                    }

                    "app.bsky.feed.like" => {
                        let mut cid = "";
                        match &commit.record {
                            Some(r) => {
                                cid = match &r.subject {
                                    Some(s) => match s {
                                        Subj::T1(_) => "",
                                        Subj::T2(subject) => &subject.cid,
                                    },
                                    None => "",
                                };
                            }
                            None => {}
                        }

                        if cid == "" {
                            panic!("empty cid");
                        }

                        g.add_like(deser_evt.did, cid.to_string()).await?;
                    }
                    "app.bsky.graph.follow" => {
                        let mut did_in = "";
                        match &commit.record {
                            Some(r) => {
                                did_in = match &r.subject {
                                    Some(s) => match s {
                                        Subj::T1(s) => s,
                                        Subj::T2(_) => "",
                                    },
                                    None => "",
                                };
                            }
                            None => {}
                        }
                        if did_in == "" {
                            panic!("empty did_in");
                        }

                        g.add_follow(deser_evt.did, did_in.to_string()).await?;
                    }
                    _ => {
                        println!("{:?}", commit.collection);
                    }
                }
            } else if commit.operation == "delete" {
                match commit.collection.as_str() {
                    "app.bsky.feed.post" => {
                        println!("{:?}", commit);
                        //g.rm_post(deser_evt.did, commit.cid.clone().unwrap())
                        //    .await?;
                    }
                    "app.bsky.feed.repost" => {
                        println!("{:?}", commit);
                        // g.rm_repost(deser_evt.did, cid.to_string()).await?;
                    }

                    "app.bsky.feed.like" => {
                        println!("{:?}", commit);
                        // g.rm_like(deser_evt.did, cid.to_string()).await?;
                    }
                    "app.bsky.graph.follow" => {
                        println!("{:?}", commit);
                        //g.rm_follow(deser_evt.did, did_in.to_string()).await?;
                    }
                    _ => {
                        println!("{:?}", commit.collection);
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
    pub reply: Option<Reply>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reply {
    pub parent: Parent,
    pub root: Parent,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Parent {
    pub cid: String,
    pub uri: String,
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
