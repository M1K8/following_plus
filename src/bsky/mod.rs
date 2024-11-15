use std::collections::HashSet;

use crate::bsky::types::*;
use crate::graph::GraphModel;
use chrono::Utc;

mod types;

pub async fn handle_event(
    evt: Result<tokio_tungstenite::tungstenite::Message, tokio_tungstenite::tungstenite::Error>,
    g: &mut GraphModel,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut spam = HashSet::new();
    spam.insert("did:plc:xdx2v7gyd5dmfqt7v77gf457".to_owned());
    spam.insert("did:plc:a56vfzkrxo2bh443zgjxr4ix".to_owned());

    match evt {
        Ok(msg) => {
            let mm = &msg.clone().into_text().unwrap();
            let deser_evt: BskyEvent = match serde_json::from_slice(msg.into_data().as_slice()) {
                Ok(m) => m,
                Err(err) => {
                    println!("{mm}");
                    return Err(err.into());
                }
            };
            let commit: &Commit = match &deser_evt.commit {
                Some(m) => m,
                None => {
                    return Ok(());
                }
            };
            let rkey = commit.rkey.clone();

            let drift =
                (Utc::now().naive_utc().and_utc().timestamp_micros() - deser_evt.time_us) / 1000;
            if drift > 30000 {
                panic!("{drift}ms late (probably need to speed up ingest)!!!");
            }
            //println!("{drift}ms late");

            //https://bsky.app/profile/atbulls-news.bsky.social 500k posts, but seems to be reposts of the old ones over & over again?
            if spam.contains(&deser_evt.did) {
                return Ok(());
            }

            if commit.operation == "create" {
                let mut is_reply = false;
                match commit.collection.as_str() {
                    "app.bsky.feed.post" => {
                        match &commit.record {
                            Some(r) => match &r.reply {
                                Some(r) => {
                                    let rkey_parent = parse_rkey(&r.parent.uri);
                                    g.add_reply(&deser_evt.did, &rkey, &rkey_parent).await?;
                                    is_reply = true;
                                }
                                _ => {}
                            },
                            _ => {}
                        }

                        g.add_post(&deser_evt.did, &rkey, is_reply).await?;
                    }

                    "app.bsky.feed.repost" => {
                        let rkey_out = get_rkey(&commit);

                        if rkey_out.is_empty() {
                            panic!("empty rkey");
                        }

                        g.add_repost(&deser_evt.did, &rkey_out.to_string(), &rkey)
                            .await?;
                    }

                    "app.bsky.feed.like" => {
                        let rkey_out = get_rkey(&commit);

                        if rkey_out.is_empty() {
                            panic!("empty rkey");
                        }

                        g.add_like(&deser_evt.did, &rkey_out.to_string(), &rkey)
                            .await?;
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
                        g.add_follow(&deser_evt.did, &did_in.to_string(), &rkey)
                            .await?;
                    }

                    "app.bsky.graph.block" => {
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
                        g.add_block(&deser_evt.did, &did_in.to_string(), &rkey)
                            .await?;
                    }
                    _ => {
                        //println!("{:?}", mm);
                    }
                }
            } else if commit.operation == "delete" {
                match commit.collection.as_str() {
                    "app.bsky.feed.post" => {
                        // println!("{:?}", deser_evt);
                        g.rm_post(&deser_evt.did, &rkey).await?;
                    }
                    "app.bsky.feed.repost" => {
                        // println!("{:?}", deser_evt);
                        g.rm_repost(&deser_evt.did, &rkey).await?;
                    }

                    "app.bsky.feed.like" => {
                        // println!("{:?}", deser_evt);
                        g.rm_like(&deser_evt.did, &rkey).await?;
                    }
                    "app.bsky.graph.follow" => {
                        // println!("{:?}", deser_evt);
                        g.rm_follow(&deser_evt.did, &rkey).await?;
                    }
                    "app.bsky.graph.block" => {
                        // println!("{:?}", deser_evt);
                        g.rm_block(&deser_evt.did, &rkey).await?;
                    }
                    _ => {
                        //println!("{:?}", deser_evt);
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

fn parse_rkey(uri: &str) -> String {
    // the rkey are the last 13 characters
    uri.chars()
        .rev()
        .take(13)
        .collect::<Vec<_>>()
        .iter()
        .rev()
        .collect()
}

fn get_rkey(commit: &Commit) -> String {
    let rkey_out;
    match &commit.record {
        Some(r) => {
            rkey_out = match &r.subject {
                Some(s) => match s {
                    Subj::T1(_) => "".to_owned(),
                    Subj::T2(subject) => parse_rkey(&subject.uri),
                },
                None => "".to_owned(),
            };
        }
        None => rkey_out = "".to_owned(),
    }
    rkey_out
}
