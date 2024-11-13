use crate::bsky::types::*;
use crate::graph::GraphModel;
mod types;

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
            let rkey = commit.rkey.clone();
            if commit.operation == "create" {
                match commit.collection.as_str() {
                    "app.bsky.feed.post" => {
                        // check in commit for reply

                        match &commit.record {
                            Some(r) => match &r.reply {
                                Some(r) => {
                                    g.add_reply(&deser_evt.did, &rkey, &r.parent.cid).await?;
                                }
                                _ => {}
                            },
                            _ => {}
                        }
                        g.add_post(&deser_evt.did, commit.cid.clone().unwrap(), &rkey)
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

                        g.add_repost(deser_evt.did, cid.to_string(), rkey).await?;
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

                        g.add_like(deser_evt.did, cid.to_string(), rkey).await?;
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
                        g.add_follow(deser_evt.did, did_in.to_string(), rkey)
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
                        g.add_block(deser_evt.did, did_in.to_string(), rkey).await?;
                    }
                    _ => {
                        //println!("{:?}", mm);
                    }
                }
            } else if commit.operation == "delete" {
                match commit.collection.as_str() {
                    "app.bsky.feed.post" => {
                        //println!("{:?}", deser_evt);
                        //g.rm_post(deser_evt.did, commit.cid.clone().unwrap())
                        //    .await?;
                    }
                    "app.bsky.feed.repost" => {
                        //println!("{:?}", deser_evt);
                        // g.rm_repost(deser_evt.did, cid.to_string()).await?;
                    }

                    "app.bsky.feed.like" => {
                        //println!("{:?}", deser_evt);
                        // g.rm_like(deser_evt.did, cid.to_string()).await?;
                    }
                    "app.bsky.graph.follow" => {
                        //println!("{:?}", deser_evt);
                        //g.rm_follow(deser_evt.did, did_in.to_string()).await?;
                    }
                    _ => {
                        //println!("{:?}", commit.collection);
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
