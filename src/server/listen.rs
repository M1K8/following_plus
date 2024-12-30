use core::error;
use std::collections::{HashMap, HashSet};
use std::time::SystemTime;
use std::{mem, sync::Arc, time::Duration};

use backoff::future::retry;
use backoff::ExponentialBackoffBuilder;
use dashmap::DashSet;
use futures::stream::{FuturesUnordered, StreamExt};
use hyper::StatusCode;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::bsky::types::RecNotFound;
use crate::common::{FetchMessage, PostMsg, PostResp};

use crate::bsky;
use crate::event_database::EventDatabase;
use crate::graph::queries;

pub async fn listen_for_requests<T: EventDatabase<HashMap<String, PostMsg>> + Clone + 'static>(
    write_lock: Arc<RwLock<()>>,
    writer: T,
    fetcher: T,
    mut recv: mpsc::Receiver<FetchMessage>,
) -> Result<(), neo4rs::Error> {
    let mut client = reqwest::ClientBuilder::new();
    client = client.timeout(Duration::from_secs(10));
    let client = client.build().unwrap();

    let in_flight: Arc<DashSet<String>> = Arc::new(DashSet::new());
    let seen_map = Arc::new(DashSet::new());

    loop {
        let mut msg = match recv.recv().await {
            Some(s) => s,
            None => continue,
        };

        if msg.did.is_empty() {
            warn!("Replying blank  empty did {}", &msg.did);
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
                    warn!("Error sending empty did resp back for{}: {}", &msg.did, e);
                    continue;
                }
            };
            continue;
        }

        info!("Got event for {:?}", msg.did);
        let cursor;
        if msg.cursor.is_some() {
            let cur = mem::take(&mut msg.cursor);
            cursor = cur.unwrap();
            info!("cursor is {cursor}");
        } else {
            cursor = now();
        }

        // Follows
        let did = msg.did.clone();
        let cl_follows = client.clone();

        // Blocks
        let did_blocks = msg.did.clone();
        if !seen_map.contains(&did_blocks) {
            let cl_blocks = client.clone();
            match get_blocks(&did_blocks, cl_blocks).await {
                Ok(_b) => {
                    //TODO - Write blocks
                }
                Err(e) => {
                    warn!("Error getting blocks for {}: {}", &msg.did, e);
                }
            };
            seen_map.insert(did_blocks);
        }

        match get_follows(&did, cl_follows).await {
            Ok(follows) => {
                let lock = write_lock.clone();
                let seen_map = seen_map.clone();
                let in_flight = in_flight.clone();
                let writer = writer.clone();
                tokio::spawn(async move {
                    if in_flight.contains(&did) {
                        warn!("Already in flight for {did}, skipping...");
                        return;
                    }

                    in_flight.insert(did.clone());
                    info!("Recursively fetching {} follows for {did}", follows.len());

                    let all_follows_result = Arc::new(DashSet::new());
                    follows.iter().for_each(|f| {
                        all_follows_result.insert((f.0.clone(), f.1.clone(), did.clone()));
                    });

                    let mut filtered_follows = Vec::new();
                    for (mut f, _) in follows {
                        if !seen_map.contains(&f) {
                            let fcl = f.clone();
                            filtered_follows.push(mem::take(&mut f));
                            seen_map.insert(fcl);
                        }
                    }
                    let all_follows_chunks: Vec<&[String]>;
                    if filtered_follows.len() == 0 {
                        in_flight.remove(&did);
                        return;
                    }

                    if filtered_follows.len() < 24 {
                        all_follows_chunks = filtered_follows.chunks(1).collect();
                    } else {
                        all_follows_chunks = filtered_follows
                            .chunks(filtered_follows.len() / 24)
                            .collect();
                    }

                    let mut set = JoinSet::new();

                    for idx in 0..all_follows_chunks.len() {
                        let mut c = match all_follows_chunks.get(idx) {
                            Some(c) => *c,
                            None => {
                                continue;
                            }
                        };

                        let yoinked = mem::take(&mut c);
                        let chunk = Vec::from(yoinked);

                        let seen_map = seen_map.clone();
                        let all_follows_result = all_follows_result.clone();

                        set.spawn(async move {
                             for did in chunk {
                                 let mut web_client = reqwest::ClientBuilder::new();
                                 web_client = web_client.timeout(Duration::from_secs(5));
                                 let web_client = web_client.build().unwrap();
                                 match get_follows(&did, web_client).await {
                                     Ok(mut f) => {
                                         f.iter_mut().for_each(|f| {
                                             all_follows_result.insert((
                                                 mem::take(&mut f.0),
                                                 mem::take(&mut f.1),
                                                 did.clone(),
                                             ));
                                         });
                                     }
                                     Err(e) => {
                                         if e.is::<bsky::types::RecNotFound>() {
                                             info!("{did} probably doesnt exist on this PDS, skipping...")
                                         } else {
                                             warn!(
                                                 "Error getting 2nd degree follows for {did}: {:?}",
                                                 e
                                             );
                                             seen_map.remove(&did); // this aint ever gonna work, so ignore it
                                         }

                                         continue;
                                     }
                                 };
                             }
                         });
                    }
                    set.join_all().await;
                    info!("There are {} chunks", all_follows_chunks.len());
                    info!(
                        "Done Recursively fetching {} for {did}; writing...",
                        filtered_follows.len()
                    );

                    match chunk_and_write_follows(all_follows_result, writer, lock).await {
                        Some(e) => warn!("Error writing 2nd degree follows for {did}: {:?}", e),
                        None => {}
                    };

                    in_flight.remove(&did);
                });
            }
            Err(e) => {
                warn!("Error getting follows for {}: {}", &msg.did, e);
                in_flight.remove(&msg.did);
            }
        };
        match fetch_and_return_posts(fetcher.clone(), msg, &cursor).await {
            Ok(_) => {}
            Err(_) => continue,
        }
    }
}

async fn fetch_and_return_posts(
    fetcher: impl EventDatabase<HashMap<String, PostMsg>> + Clone,
    msg: FetchMessage,
    time: &String,
) -> Result<(), SendError<PostResp>> {
    let mut res_vec = Vec::new();
    let mut timestamp = time.clone();

    let mut first_time = false;
    while res_vec.len() < 40 && !first_time {
        let fetcher = fetcher.clone();
        // max resp size is 30, but pre-fetch the next one a little just for some wiggle room
        if let Some(posts) = fetch_posts(fetcher, &msg, &timestamp).await {
            for p in posts.iter() {
                res_vec.push(p.1.to_owned());
            }

            if let Some(val) = res_vec.last() {
                timestamp = val.timestamp.to_string();
            } else {
                info!("Reached the end");
                break;
            }
            info!("Earliest timestamp is {:?}", timestamp);
            first_time = true; // make sure we call at least once
        }
    }

    if res_vec.len() == 0 {
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
                return Err(e);
            }
        };
        return Ok(());
    } else if res_vec.len() > 30 {
        res_vec.truncate(30);
    }

    let now = SystemTime::now();
    res_vec.sort_unstable();
    info!("Sorted in {}ms", now.elapsed().unwrap().as_millis());

    for v in res_vec.iter() {
        info!("Adding {:?}", v);
    }

    match msg
        .resp
        .send(PostResp {
            posts: res_vec,
            cursor: Some(timestamp),
        })
        .await
    {
        Ok(_) => {
            info!("Done!")
        }
        Err(e) => {
            warn!("Error replying to post request for {}: {:?}", msg.did, e);
            return Err(e);
        }
    };
    Ok(())
}

async fn fetch_posts(
    fetcher: impl EventDatabase<HashMap<String, PostMsg>> + Clone,
    msg: &FetchMessage,
    time: &String,
) -> Option<HashMap<String, PostMsg>> {
    // Fetch posts

    let now = SystemTime::now();
    let q1 = &queries::GET_BEST_2ND_DEG_LIKES
        .to_string()
        .replace("{}", &time);

    let q2 = &queries::GET_BEST_2ND_DEG_REPOSTS
        .to_string()
        .replace("{}", &time);

    let q3 = &queries::GET_FOLLOWING_PLUS_LIKES
        .to_string()
        .replace("{}", &time);

    let q4 = &queries::GET_FOLLOWING_PLUS_REPOSTS
        .to_string()
        .replace("{}", &time);

    let q5 = &queries::GET_BEST_FOLLOWED.to_string().replace("{}", &time);

    // For some reason tokio::join did not play nice with the Trait, but this is also more succinct so :shrug:
    let mut tasks = FuturesUnordered::new();
    tasks.push(fetcher.read(
        "2ND_DEG_LIKES",
        q1,
        Some(HashMap::from([("did".to_string(), msg.did.clone())])),
    ));

    tasks.push(fetcher.read(
        "GET_BEST_2ND_DEG_REPOSTS",
        q2,
        Some(HashMap::from([("did".to_string(), msg.did.clone())])),
    ));

    tasks.push(fetcher.read(
        "GET_FOLLOWING_PLUS_LIKES",
        q3,
        Some(HashMap::from([("did".to_string(), msg.did.clone())])),
    ));

    tasks.push(fetcher.read(
        "GET_FOLLOWING_PLUS_REPOSTS",
        q4,
        Some(HashMap::from([("did".to_string(), msg.did.clone())])),
    ));

    tasks.push(fetcher.read(
        "GET_BEST_FOLLOWED",
        q5,
        Some(HashMap::from([("did".to_string(), msg.did.clone())])),
    ));

    let mut posts: HashMap<String, PostMsg> = HashMap::new();
    while let Some(result) = tasks.next().await {
        match result {
            Ok(value) => posts.extend(value),
            Err(e) => {
                warn!("Error joining post fetches for {}: {}", &msg.did, e);
                return None;
            }
        }
    }

    info!("Took {} ms to get", now.elapsed().unwrap().as_millis());
    Some(posts)
}

fn now() -> String {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_micros()
        .to_string()
}

async fn get_follows(
    did_follows: &String,
    cl_follows: reqwest::Client,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let follows = match bsky::get_follows(did_follows.clone(), cl_follows).await {
        Ok(f) => f,
        Err(e) => {
            let err_str = format!("{:?}", e);
            let status: StatusCode = match e.1 {
                Some(s) => s,
                None => StatusCode::IM_A_TEAPOT,
            };
            if err_str.contains("missing field `records`")
                && status == StatusCode::from_u16(400).unwrap()
            {
                return Err(Box::new(RecNotFound {}));
            }
            return Err(Box::new(e.0));
        }
    };

    Ok(follows)
}

async fn get_blocks(
    did_blocks: &String,
    cl_blocks: reqwest::Client,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let blocks = match bsky::get_blocks(did_blocks.clone(), cl_blocks).await {
        Ok(f) => f,
        Err(e) => {
            let err_str = format!("{:?}", e);
            let status: StatusCode = match e.1 {
                Some(s) => s,
                None => StatusCode::IM_A_TEAPOT,
            };
            if err_str.contains("missing field `records`")
                && status == StatusCode::from_u16(400).unwrap()
            {
                return Err(Box::new(RecNotFound {}));
            }
            return Err(Box::new(e.0));
        }
    };

    Ok(blocks)
}

async fn chunk_and_write_follows(
    follows: Arc<DashSet<(String, String, String)>>,
    mut conn: impl EventDatabase<HashMap<String, PostMsg>> + Clone,
    write_lock: Arc<RwLock<()>>,
) -> Option<Box<dyn error::Error>> {
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
    let l = write_lock.write().await;
    let r = match conn
        .chunk_write(queries::POPULATE_FOLLOW, follow_chunks, 20, "follows")
        .await
    {
        Some(e) => Some(e),
        None => None,
    };
    drop(l);
    r
}
