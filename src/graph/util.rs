use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use dashmap::DashSet;
use neo4rs::Graph;
use tokio::sync::{mpsc, Mutex};

use crate::{
    bsky,
    common::{FetchMessage, PostMsg, PostResp},
    graph::queries::{self, PURGE_OLD_POSTS},
};

const PURGE_TIME: u64 = 15 * 60;

pub fn get_post_uri(did: String, rkey: String) -> String {
    format!("at://{did}/app.bsky.feed.post/{rkey}")
}
pub fn pluralize(word: &str) -> String {
    let word_len = word.len();
    let snip = &word[..word_len - 1];
    let last_char = word.chars().nth(word_len - 1).unwrap();

    if last_char == 'y' || word.ends_with("ay") {
        return format!("{}ies", snip);
    } else if last_char == 's' || last_char == 'x' || last_char == 'z' {
        return format!("{}es", word);
    } else if last_char == 'o' && word.ends_with("o") && !word.ends_with("oo") {
        return format!("{}oes", snip);
    } else if last_char == 'u' && word.ends_with("u") {
        return format!("{}i", snip);
    } else {
        return format!("{}s", word);
    }
}

pub async fn kickoff_purge(spin: Arc<Mutex<()>>, conn: Graph) -> Result<(), neo4rs::Error> {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(PURGE_TIME)).await;
        println!("Purging old posts");
        let lock = spin.lock().await;
        let qry = neo4rs::query(PURGE_OLD_POSTS);
        conn.run(qry).await?;
        drop(lock);
        println!("Done!");
    }
}

pub async fn listen_channel(
    write_lock: Arc<Mutex<()>>,
    conn: Graph,
    mut recv: mpsc::Receiver<FetchMessage>,
) -> Result<(), neo4rs::Error> {
    let mut client = reqwest::ClientBuilder::new();
    client = client.timeout(Duration::from_secs(5));
    let client = client.build().unwrap();

    let mut already_seen: HashSet<String> = HashSet::new();
    loop {
        let msg = match recv.recv().await {
            Some(s) => s,
            None => continue,
        };

        if msg.did.is_empty() {
            msg.resp
                .send(PostResp {
                    posts: vec![PostMsg {
                        uri: "".to_owned(),
                        reason: "".to_owned(),
                    }],
                    cursor: Some("EMPTY_DID".to_owned()),
                })
                .await
                .unwrap();
            continue;
        }
        println!("Got event for {:?}", msg.did);

        if !already_seen.contains(&msg.did) {
            // Follows
            {
                let did_follows = msg.did.clone();
                let cl_follows = client.clone();
                let follows = match bsky::get_follows(did_follows, cl_follows).await {
                    Ok(f) => f,
                    Err(e) => return Err(neo4rs::Error::UnexpectedMessage(e.to_string())),
                };

                let follow_chunks: Vec<HashMap<String, String>> = follows
                    .iter()
                    .map(|vals| {
                        HashMap::from([
                            ("out".to_owned(), vals.0.clone()),
                            ("did".to_owned(), msg.did.clone()),
                            ("rkey".to_owned(), vals.1.clone()),
                        ])
                    })
                    .collect();
                let follow_chunks = follow_chunks.chunks(60).collect::<Vec<_>>();
                for follow_chunk in follow_chunks {
                    let qry = neo4rs::query(queries::ADD_FOLLOW).param("follows", follow_chunk);
                    let l = write_lock.lock().await;
                    match conn.run(qry).await {
                        Ok(_) => {}
                        Err(e) => {
                            println!("Error on backfilling follows for {}", &msg.did);
                            return Err(e);
                        }
                    };
                    drop(l);
                }

                println!("Written {} follows for {}", follows.len(), &msg.did);
            }

            //

            // TODO - Maybe run both http queries in parallel? Might hit rate limits if we do

            // Blocks
            {
                let did_blocks = msg.did.clone();
                let cl_blocks = client.clone();
                let blocks = match bsky::get_blocks(did_blocks, cl_blocks).await {
                    Ok(f) => f,
                    Err(e) => return Err(neo4rs::Error::UnexpectedMessage(e.to_string())),
                };

                let block_chunks: Vec<HashMap<String, String>> = blocks
                    .iter()
                    .map(|vals| {
                        HashMap::from([
                            ("out".to_owned(), vals.0.clone()),
                            ("did".to_owned(), msg.did.clone()),
                            ("rkey".to_owned(), vals.1.clone()),
                        ])
                    })
                    .collect();
                let block_chunks = block_chunks.chunks(60).collect::<Vec<_>>();
                for block_chunk in block_chunks {
                    let qry = neo4rs::query(queries::ADD_BLOCK).param("blocks", block_chunk);
                    let l = write_lock.lock().await;
                    match conn.run(qry).await {
                        Ok(_) => {}
                        Err(e) => {
                            println!("Error on backfilling blocks for {}", &msg.did);
                            return Err(e);
                        }
                    };
                    drop(l);
                }

                println!("Written {} blocks for {}", blocks.len(), &msg.did);
            }
            //
            already_seen.insert(msg.did.clone());
        }
        let qry1 = neo4rs::query(queries::GET_BEST_2ND_DEG_LIKES).param("did", msg.did.clone());
        let qry2 = neo4rs::query(queries::GET_BEST_2ND_DEG_REPOSTS).param("did", msg.did.clone());
        let qry3 = neo4rs::query(queries::GET_FOLLOWING_PLUS_LIKES).param("did", msg.did.clone());
        let qry4 = neo4rs::query(queries::GET_FOLLOWING_PLUS_REPOSTS).param("did", msg.did.clone());

        let res = tokio::try_join!(
            conn.execute(qry1),
            conn.execute(qry2),
            conn.execute(qry3),
            conn.execute(qry4)
        );
        let posts = Arc::new(DashSet::new());
        match res {
            Ok((mut l1, mut l2, mut l3, mut l4)) => {
                let p1 = posts.clone();
                let p2 = posts.clone();
                let p3 = posts.clone();
                let p4 = posts.clone();
                let f1 = tokio::spawn(async move {
                    loop {
                        match l1.next().await {
                            Ok(v) => match v {
                                Some(v) => {
                                    let uri: String = v.get("url").unwrap();
                                    let user: String = v.get("user").unwrap();
                                    let uri = get_post_uri(user, uri);
                                    p1.insert(PostMsg {
                                        reason: "2ND_DEG_LIKE".to_owned(),
                                        uri,
                                    });
                                }
                                None => {
                                    break;
                                }
                            },
                            Err(e) => {
                                println!("{:?}", e);
                                break;
                            }
                        }
                    }
                });

                let f2 = tokio::spawn(async move {
                    loop {
                        match l2.next().await {
                            Ok(v) => match v {
                                Some(v) => {
                                    let uri: String = v.get("url").unwrap();
                                    let user: String = v.get("user").unwrap();
                                    let uri = get_post_uri(user, uri);
                                    p2.insert(PostMsg {
                                        reason: "2ND_DEG_REPOSTS".to_owned(),
                                        uri,
                                    });
                                }
                                None => {
                                    break;
                                }
                            },
                            Err(e) => {
                                println!("{:?}", e);
                                break;
                            }
                        }
                    }
                });

                let f3 = tokio::spawn(async move {
                    loop {
                        match l3.next().await {
                            Ok(v) => match v {
                                Some(v) => {
                                    let uri: String = v.get("url").unwrap();
                                    let user: String = v.get("user").unwrap();
                                    let uri = get_post_uri(user, uri);
                                    p3.insert(PostMsg {
                                        reason: "2ND_DEG_LIKE".to_owned(),
                                        uri,
                                    });
                                }
                                None => {
                                    break;
                                }
                            },
                            Err(e) => {
                                println!("{:?}", e);
                                break;
                            }
                        }
                    }
                });

                let f4 = tokio::spawn(async move {
                    loop {
                        match l4.next().await {
                            Ok(v) => match v {
                                Some(v) => {
                                    let uri: String = v.get("url").unwrap();
                                    let user: String = v.get("user").unwrap();
                                    let uri = get_post_uri(user, uri);
                                    p4.insert(PostMsg {
                                        reason: "2ND_DEG_LIKE".to_owned(),
                                        uri,
                                    });
                                }
                                None => {
                                    break;
                                }
                            },
                            Err(e) => {
                                println!("{:?}", e);
                                break;
                            }
                        }
                    }
                });

                match tokio::try_join!(f1, f2, f3, f4) {
                    Ok(_) => {}
                    Err(e) => return Err(neo4rs::Error::UnexpectedMessage(e.to_string())),
                };
            }
            Err(e) => return Err(e),
        }
        let mut res_vec = Vec::new();
        let mut ctr = 0;
        for p in posts.iter() {
            // TODO - sorting based on ts & reason
            // TODO - Caching based on did
            ctr += 1;
            if ctr >= 31 {
                break;
            }
            res_vec.push(p.key().clone());
        }

        msg.resp
            .send(PostResp {
                posts: res_vec,
                cursor: None,
            })
            .await
            .unwrap();
    }
}
