use std::{collections::HashMap, mem, sync::Arc, time::Duration};

use backoff::{future::retry, Error, ExponentialBackoffBuilder};
use dashmap::{DashMap, DashSet};
use neo4rs::{Graph, Query};
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::{
    bsky,
    common::{FetchMessage, PostMsg, PostResp},
    graph::{
        queries::{self},
        util,
    },
};

macro_rules! process_next {
    ($next_expr:expr, $posts_expr:expr, $reason:expr) => {
        match $next_expr {
            Ok(v) => match v {
                Some(v) => {
                    let uri: String = v.get("url").unwrap();
                    let user: String = v.get("user").unwrap();
                    let reply: String = v.get("isReply").unwrap();
                    let uri = util::get_post_uri(user, uri);
                    let timestamp: u64 = v.get("ts").unwrap();
                    $posts_expr.insert(
                        uri.clone(),
                        PostMsg {
                            reason: format!("{}_{reply}", $reason).to_owned(),
                            uri,
                            timestamp,
                        },
                    );
                }
                None => {
                    break;
                }
            },
            Err(e) => {
                info!("{:?}", e);
                break;
            }
        }
    };
}

pub async fn listen_channel(
    write_lock: Arc<RwLock<()>>,
    conn: Graph,
    read_conn: Graph,
    mut recv: mpsc::Receiver<FetchMessage>,
) -> Result<(), neo4rs::Error> {
    let mut client = reqwest::ClientBuilder::new();
    client = client.timeout(Duration::from_secs(10));
    let client = client.build().unwrap();

    // k: did, v: uint post index
    // TODO - Write here & fetch further back if scrolled; dup queries w/ timestamp gate (format!'d)
    let cached = HashMap::<String, Vec<PostMsg>>::new();

    let seen_map = Arc::new(DashSet::new());
    loop {
        let msg = match recv.recv().await {
            Some(s) => s,
            None => continue,
        };

        if msg.did.is_empty() {
            match msg
                .resp
                .send(PostResp {
                    posts: vec![PostMsg {
                        uri: "".to_owned(),
                        reason: "".to_owned(),
                        timestamp: 0,
                    }],
                    cursor: Some("EMPTY_DID".to_owned()),
                })
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    warn!("Error sending posts back for{}: {}", &msg.did, e);
                    continue;
                }
            };
            continue;
        }
        info!("Got event for {:?}", msg.did);

        // Firstly, check the cache for this user, then check if the cursor is within the last 5 mins, otherwise junk it (if the cleanup job hasnt already)
        // Moreover, change queries
        // See if user has been seen

        // Follows
        let did_follows = msg.did.clone();
        let cl_follows = client.clone();

        let lock = write_lock.clone();
        let second_deg_conn = conn.clone();
        let seen_map = seen_map.clone();

        let bl_seen_map = seen_map.clone();
        seen_map.insert(did_follows.clone());
        match get_follows(&did_follows, cl_follows).await {
            Ok(follows) => {
                tokio::spawn(async move {
                    info!(
                        "Recursively fetching {} follows for {did_follows}",
                        follows.len()
                    );

                    let all_follows = Arc::new(DashSet::new());
                    if !seen_map.contains(&did_follows) {
                        follows.iter().for_each(|f| {
                            info!("adding {} {} {}", f.0, f.1, &did_follows);
                            all_follows.insert((f.0.clone(), f.1.clone(), did_follows.clone()));
                        });
                    }

                    // split into 12 chunks
                    // spin off 12 fetcher threads
                    // the join on them
                    let mut set = JoinSet::new();
                    let all_followers_chunks: Vec<&[(String, String)]> =
                        follows.chunks(follows.len() / 12).collect();
                    for idx in 0..=11 {
                        let c = match all_followers_chunks.get(idx) {
                            Some(c) => *c,
                            None => {
                                continue;
                            }
                        };
                        let mut e = c;
                        // this is so fun i love doing this
                        let yoinked = mem::take(&mut e);
                        let chunk = Vec::from(yoinked);
                        //

                        let seen_map = seen_map.clone();
                        let all_follows = all_follows.clone();

                        set.spawn(async move {
                            for (did, _) in chunk {
                                if seen_map.contains(&did) {
                                    // Already fetched this one, no need to do it again
                                    continue;
                                }

                                let mut cl_2nd_follows = reqwest::ClientBuilder::new();
                                cl_2nd_follows = cl_2nd_follows.timeout(Duration::from_secs(10));
                                let cl_2nd_follows = cl_2nd_follows.build().unwrap();
                                match get_follows(&did, cl_2nd_follows).await {
                                    Ok(f) => {
                                        f.iter().for_each(|f| {
                                            all_follows.insert((
                                                f.0.clone(),
                                                f.1.clone(),
                                                did.clone(),
                                            ));
                                        });
                                        seen_map.insert(did.clone());
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Error getting 2nd degree follows for {did}: {:?}",
                                            e
                                        );
                                        continue;
                                    }
                                };
                            }
                        });
                    }
                    set.join_all().await;

                    info!(
                        "Done Recursively fetching {} for {did_follows}; writing...",
                        follows.len()
                    );

                    match write_follows(all_follows, &second_deg_conn, lock).await {
                        Some(e) => warn!(
                            "Error writing 2nd degree follows for {did_follows}: {:?}",
                            e
                        ),
                        None => {}
                    }
                });
            }
            Err(e) => {
                warn!("Error getting follows for {}: {}", &msg.did, e);
                continue;
            }
        };
        if !bl_seen_map.contains(&msg.did) {
            // Blocks
            let did_blocks = msg.did.clone();
            let cl_blocks = client.clone();
            match get_blocks(&did_blocks, cl_blocks, &conn, write_lock.clone()).await {
                Ok(_) => {}
                Err(e) => {
                    warn!("Error getting blocks for {}: {}", &msg.did, e);
                    continue;
                }
            };
        }

        // Fetch posts
        let qry1 = neo4rs::query(queries::GET_BEST_2ND_DEG_LIKES).param("did", msg.did.clone());
        let qry2 = neo4rs::query(queries::GET_BEST_2ND_DEG_REPOSTS).param("did", msg.did.clone());
        let qry3 = neo4rs::query(queries::GET_FOLLOWING_PLUS_LIKES).param("did", msg.did.clone());
        let qry4 = neo4rs::query(queries::GET_FOLLOWING_PLUS_REPOSTS).param("did", msg.did.clone());

        let res = tokio::try_join!(
            read_conn.execute(qry1),
            read_conn.execute(qry2),
            read_conn.execute(qry3),
            read_conn.execute(qry4)
        );
        let posts = Arc::new(DashMap::new());
        match res {
            Ok((mut l1, mut l2, mut l3, mut l4)) => {
                let p1 = posts.clone();
                let p2 = posts.clone();
                let p3 = posts.clone();
                let p4 = posts.clone();
                let f1 = tokio::spawn(async move {
                    loop {
                        process_next!(l1.next().await, p1, "2ND_DEG_LIKES");
                    }
                });

                let f2 = tokio::spawn(async move {
                    loop {
                        process_next!(l2.next().await, p2, "2ND_DEG_REPOSTS");
                    }
                });

                let f3 = tokio::spawn(async move {
                    loop {
                        process_next!(l3.next().await, p3, "FPLUS_LIKES");
                    }
                });

                let f4 = tokio::spawn(async move {
                    loop {
                        process_next!(l4.next().await, p4, "FPLUS_REPOSTS");
                    }
                });

                match tokio::try_join!(f1, f2, f3, f4) {
                    Ok(_) => {}
                    Err(e) => return Err(neo4rs::Error::UnexpectedMessage(e.to_string())),
                };
            }
            Err(e) => {
                warn!("Error joining post fetches for {}: {}", &msg.did, e);
                continue;
            }
        }
        let mut res_vec = Vec::new();
        let mut _ctr = 0;
        for p in posts.iter() {
            res_vec.push(p.value().clone());
        }

        res_vec.sort_unstable();
        info!("Adding {:?}", res_vec.len());
        for v in res_vec.iter() {
            info!("Adding {:?}", v);
        }

        match msg
            .resp
            .send(PostResp {
                posts: res_vec,
                cursor: None,
            })
            .await
        {
            Ok(_) => {}
            Err(e) => {
                warn!("Error replying to post request for {}: {:?}", msg.did, e);
                continue;
            }
        };
    }
}

async fn get_follows(
    did_follows: &String,
    cl_follows: reqwest::Client,
) -> Result<Arc<Vec<(String, String)>>, Box<dyn std::error::Error>> {
    let follows = match bsky::get_follows(did_follows.clone(), cl_follows).await {
        Ok(f) => f,
        Err(e) => return Err(Box::new(e)),
    };

    let follows = Arc::new(follows);
    Ok(follows)
}

async fn get_blocks(
    did_blocks: &String,
    cl_blocks: reqwest::Client,
    conn: &Graph,
    write_lock: Arc<RwLock<()>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let blocks = match bsky::get_blocks(did_blocks.clone(), cl_blocks).await {
        Ok(f) => f,
        Err(e) => return Err(Box::new(e)),
    };

    let block_chunks: Vec<HashMap<String, String>> = blocks
        .iter()
        .map(|vals| {
            HashMap::from([
                ("out".to_owned(), vals.0.clone()),
                ("did".to_owned(), did_blocks.clone()),
                ("rkey".to_owned(), vals.1.clone()),
            ])
        })
        .collect();
    let block_chunks = block_chunks.chunks(60).collect::<Vec<_>>();
    for block_chunk in block_chunks {
        let qry = neo4rs::query(queries::ADD_BLOCK).param("blocks", block_chunk);
        let l = write_lock.read().await;
        match conn.run(qry).await {
            Ok(_) => {}
            Err(e) => {
                info!("Error on backfilling blocks for {}", &did_blocks);
                drop(l);
                return Err(Box::new(e));
            }
        };
        drop(l);
    }

    info!("Written {} blocks for {}", blocks.len(), &did_blocks);

    Ok(())
}

async fn write_follows(
    follows: Arc<DashSet<(String, String, String)>>,
    conn: &Graph,
    write_lock: Arc<RwLock<()>>,
) -> Option<Box<dyn std::error::Error>> {
    let follow_chunks: Vec<HashMap<String, String>> = follows
        .iter()
        .map(|vals| {
            HashMap::from([
                ("out".to_owned(), vals.0.clone()),
                ("rkey".to_owned(), vals.1.clone()),
                ("did".to_owned(), vals.2.clone()),
            ])
        })
        .collect();
    let len = follow_chunks.len();
    let chunks;
    if len < 12 as usize {
        chunks = follow_chunks.chunks(1).collect::<Vec<_>>();
    } else {
        chunks = follow_chunks.chunks(len / 20).collect::<Vec<_>>();
    }

    let mut qrys = Vec::new();
    for follow_chunk in chunks {
        let qry = neo4rs::query(queries::ADD_FOLLOW).param("follows", follow_chunk);
        qrys.push(qry);
    }
    let conn_cl = conn.clone();
    let l = write_lock.write().await;
    match retry(
        ExponentialBackoffBuilder::default()
            .with_initial_interval(Duration::from_millis(250))
            .with_max_elapsed_time(Some(Duration::from_millis(10000)))
            .build(),
        || async {
            let i: Vec<Query> = qrys.iter().map(|v| v.clone()).collect();
            let ilen = &i.len();
            let mut tx = conn_cl.start_txn().await.unwrap();
            match tx.run_queries(i).await {
                Ok(_) => match tx.commit().await {
                    Ok(_) => {
                        info!("Written {} queries", ilen);
                        Ok(())
                    }
                    Err(e) => Err(Error::Transient {
                        err: e,
                        retry_after: None,
                    }),
                },
                Err(e) => Err(Error::Transient {
                    err: e,
                    retry_after: None,
                }),
            }
        },
    )
    .await
    {
        Ok(_) => {
            info!("Written {} follows", len);
            drop(l);
        }
        Err(e) => {
            drop(l);
            warn!("Error committing: {}", e);
            return Some(Box::new(e));
        }
    }
    info!("Done!");
    None
}
