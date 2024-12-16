use std::time::SystemTime;
use std::{mem, sync::Arc, time::Duration};

use dashmap::DashSet;
use neo4rs::Graph;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::common::{FetchMessage, PostMsg, PostResp};
use crate::graph::{fetcher, first_call};

pub async fn listen_channel(
    write_lock: Arc<RwLock<()>>,
    conn: Graph,
    read_conn: Graph,
    mut recv: mpsc::Receiver<FetchMessage>,
) -> Result<(), neo4rs::Error> {
    let mut client = reqwest::ClientBuilder::new();
    client = client.timeout(Duration::from_secs(10));
    let client = client.build().unwrap();

    let in_flight = Arc::new(DashSet::new());
    let seen_map = Arc::new(DashSet::new());
    let mut fetcher = fetcher::Fetcher::new(read_conn);
    loop {
        let mut msg = match recv.recv().await {
            Some(s) => s,
            None => continue,
        };

        if msg.did.is_empty() || in_flight.contains(&msg.did) {
            warn!("Replying blank - in flight or empty did {}", &msg.did);
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

        let lock = write_lock.clone();
        let second_deg_conn = conn.clone();
        let seen_map_cl = seen_map.clone();
        let in_flight_spawned = in_flight.clone();

        in_flight.insert(msg.did.clone());
        match first_call::get_follows(&did, cl_follows).await {
            Ok(follows) => {
                tokio::spawn(async move {
                    info!("Recursively fetching {} follows for {did}", follows.len());

                    let all_follows_result = Arc::new(DashSet::new());
                    follows.iter().for_each(|f| {
                        all_follows_result.insert((f.0.clone(), f.1.clone(), did.clone()));
                    });

                    let mut filtered_follows = Vec::new();
                    for (mut f, _) in follows {
                        if !seen_map_cl.contains(&f) {
                            let fcl = f.clone();
                            filtered_follows.push(mem::take(&mut f));
                            seen_map_cl.insert(fcl);
                        }
                    }

                    let mut set = JoinSet::new();
                    let all_follows_chunks: Vec<&[String]> = filtered_follows
                        .chunks(filtered_follows.len() / 8)
                        .collect();

                    for idx in 0..all_follows_chunks.len() {
                        let mut c = match all_follows_chunks.get(idx) {
                            Some(c) => *c,
                            None => {
                                continue;
                            }
                        };

                        let yoinked = mem::take(&mut c);
                        let chunk = Vec::from(yoinked);

                        let seen_map = seen_map_cl.clone();
                        let all_follows_result = all_follows_result.clone();

                        set.spawn(async move {
                            for did in chunk {
                                let mut cl_2nd_follows = reqwest::ClientBuilder::new();
                                cl_2nd_follows = cl_2nd_follows.timeout(Duration::from_secs(5));
                                let cl_2nd_follows = cl_2nd_follows.build().unwrap();
                                match first_call::get_follows(&did, cl_2nd_follows).await {
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
                                        if e.is::<first_call::RecNotFound>() {
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

                    match first_call::write_follows(all_follows_result, &second_deg_conn, lock)
                        .await
                    {
                        Some(e) => warn!("Error writing 2nd degree follows for {did}: {:?}", e),
                        None => {}
                    };
                    info!("Cleared from in-flight for {did}");
                    in_flight_spawned.remove(&did);
                });
            }
            Err(e) => {
                warn!("Error getting follows for {}: {}", &msg.did, e);
                in_flight.remove(&msg.did);
                continue;
            }
        };

        // Blocks
        let did_blocks = msg.did.clone();
        let cl_blocks = client.clone();
        match first_call::get_blocks(&did_blocks, cl_blocks, &conn, write_lock.clone()).await {
            Ok(_) => {}
            Err(e) => {
                warn!("Error getting blocks for {}: {}", &msg.did, e);
                continue;
            }
        };
        seen_map.insert(did_blocks);

        match fetcher.fetch_and_return_posts(msg, &cursor).await {
            Ok(_) => {}
            Err(_) => continue,
        }
    }
}

fn now() -> String {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_micros()
        .to_string()
}
