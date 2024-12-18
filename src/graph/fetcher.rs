use core::error;
use std::{
    mem,
    sync::Arc,
    time::{Duration, SystemTime},
};

use dashmap::DashMap;
use neo4rs::Graph;
use tracing::{info, warn};

use crate::common::{FetchMessage, PostMsg, PostResp};

use super::queries;

macro_rules! process_next {
    ($next_expr:expr, $posts_expr:expr, $reason:expr) => {
        match $next_expr {
            Ok(v) => match v {
                Some(v) => {
                    let uri: String = v.get("url").unwrap();
                    let user: String = v.get("user").unwrap();
                    let uri = crate::graph::util::get_post_uri(user, uri);
                    let timestamp: u64 = v.get("ts").unwrap();
                    $posts_expr.insert(
                        uri.clone(),
                        PostMsg {
                            reason: $reason.to_string(),
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
                warn!("{:?}", e);
                break;
            }
        }
    };
}
pub struct Fetcher {
    read_conn: Graph,
    cache: DashMap<String, Vec<PostMsg>>,
}

impl Fetcher {
    pub fn new(read_conn: Graph) -> Self {
        Self {
            read_conn,
            cache: DashMap::new(),
        }
    }
    pub async fn fetch_and_return_posts(
        &mut self,
        msg: FetchMessage,
        time: &String,
    ) -> Result<(), Box<dyn error::Error>> {
        let mut res_vec = Vec::new();
        let mut cursor;

        if let Some(mut cached) = self.cache.get_mut(&msg.did) {
            if cached.len() > 0 {
                info!("Grabbed {} cached posts", cached.len());
                res_vec = mem::take(&mut cached);
                let c = res_vec.last().unwrap().timestamp;
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_micros();

                if now - c as u128 > Duration::from_secs(300).as_micros() {
                    res_vec.clear();
                    cursor = time.clone();
                } else {
                    cursor = c.to_string();
                }
            } else {
                cursor = time.clone();
            }
        } else {
            cursor = time.clone();
        }

        let mut first_time = false;
        while res_vec.len() < 40 && !first_time {
            // max resp size is 30, but pre-fetch the next one a little just for some wiggle room
            if let Ok(posts) = self.fetch_posts(&msg, &cursor).await {
                for p in posts.iter() {
                    res_vec.push(p.value().clone());
                }

                if let Some(val) = res_vec.last() {
                    cursor = val.timestamp.to_string();
                } else {
                    info!("Reached the end");
                    break;
                }
                info!("Earliest Cursor is {:?}", cursor);
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
                    return Err(Box::new(e));
                }
            };
            return Ok(());
        } else if res_vec.len() > 30 {
            let leftover = res_vec.split_off(30);
            self.cache.insert(msg.did.clone(), leftover);
        }

        res_vec.sort_unstable();
        for v in res_vec.iter() {
            info!("Adding {:?}", v);
        }

        match msg
            .resp
            .send(PostResp {
                posts: res_vec,
                cursor: Some(cursor),
            })
            .await
        {
            Ok(_) => {
                info!("Done!")
            }
            Err(e) => {
                warn!("Error replying to post request for {}: {:?}", msg.did, e);
                return Err(Box::new(e));
            }
        };
        Ok(())
    }

    async fn fetch_posts(
        &self,
        msg: &FetchMessage,
        time: &String,
    ) -> Result<Arc<DashMap<String, PostMsg>>, Box<dyn error::Error>> {
        // Fetch posts
        let qry1 = neo4rs::query(
            &queries::GET_BEST_2ND_DEG_LIKES
                .to_string()
                .replace("{}", &time),
        ) //we can 100% macro this
        .param("did", msg.did.clone());
        let qry2 = neo4rs::query(
            &queries::GET_BEST_2ND_DEG_REPOSTS
                .to_string()
                .replace("{}", &time),
        )
        .param("did", msg.did.clone());
        let qry3 = neo4rs::query(
            &queries::GET_FOLLOWING_PLUS_LIKES
                .to_string()
                .replace("{}", &time),
        )
        .param("did", msg.did.clone());
        let qry4 = neo4rs::query(
            &queries::GET_FOLLOWING_PLUS_REPOSTS
                .to_string()
                .replace("{}", &time),
        )
        .param("did", msg.did.clone());

        let qry5 = neo4rs::query(&queries::GET_BEST_FOLLOWED.to_string().replace("{}", &time))
            .param("did", msg.did.clone());

        let res = tokio::try_join!(
            self.read_conn.execute(qry1),
            self.read_conn.execute(qry2),
            self.read_conn.execute(qry3),
            self.read_conn.execute(qry4),
            self.read_conn.execute(qry5)
        );
        let posts = Arc::new(DashMap::new());
        match res {
            Ok((mut l1, mut l2, mut l3, mut l4, mut l5)) => {
                let p1 = posts.clone();
                let p2 = posts.clone();
                let p3 = posts.clone();
                let p4 = posts.clone();
                let p5 = posts.clone();
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

                let f5 = tokio::spawn(async move {
                    loop {
                        process_next!(l5.next().await, p5, "BEST_FOLLOWING");
                    }
                });

                match tokio::try_join!(f1, f2, f3, f4, f5) {
                    Ok(_) => {}
                    Err(e) => {
                        return Err(Box::new(neo4rs::Error::UnexpectedMessage(e.to_string())))
                    }
                };
            }
            Err(e) => {
                warn!("Error joining post fetches for {}: {}", &msg.did, e);
                return Err(Box::new(e));
            }
        }

        Ok(posts)
    }
}
