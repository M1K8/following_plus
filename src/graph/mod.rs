use std::{collections::HashMap, time::Duration};

use backoff::{future::retry, ExponentialBackoffBuilder};
use neo4rs::Graph;
use tracing::{error, info, warn};

use crate::{common::PostMsg, event_database::EventDatabase};

mod graph_test;
pub mod queries;

fn get_post_uri(did: String, rkey: String) -> String {
    format!("at://{did}/app.bsky.feed.post/{rkey}")
}

macro_rules! process_next {
    ($next_expr:expr, $posts_expr:expr, $reason:expr) => {
        match $next_expr {
            Ok(v) => match v {
                Some(v) => {
                    let uri: String = v.get("url").unwrap();
                    let user: String = v.get("user").unwrap();
                    let uri = crate::graph::get_post_uri(user, uri);
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

#[derive(Clone)]
pub struct GraphFetcher {
    conn: Graph,
}

impl EventDatabase<HashMap<String, PostMsg>> for GraphFetcher {
    async fn read(
        &self,
        query_name: &str,
        query: &str,
        params: Option<HashMap<String, String>>,
    ) -> Result<HashMap<String, PostMsg>, Box<dyn std::error::Error>> {
        let mut qry = neo4rs::query(query);
        match params {
            Some(p) => {
                qry = qry.params(p);
            }
            None => {}
        };

        let mut res = HashMap::new();

        match self.conn.execute(qry).await {
            Ok(mut r) => loop {
                process_next!(r.next().await, res, query_name);
            },
            Err(e) => return Err(Box::new(e)),
        };

        Ok(res)
    }

    async fn write(
        &self,
        query: &str,
        params: Option<HashMap<String, String>>,
    ) -> Option<Box<dyn std::error::Error>> {
        let mut qry = neo4rs::query(query);
        match params {
            Some(p) => {
                qry = qry.params(p);
            }
            None => {}
        };

        // This is to be used when we dont want to use explicit transactions
        match self.conn.execute(qry).await {
            Ok(_) => None,
            Err(e) => Some(Box::new(e)),
        }
    }

    async fn chunk_write(
        &self,
        query: &str,
        params: Vec<HashMap<String, String>>,
        chunk_size: usize,
        param_name: &str,
    ) -> Option<Box<dyn std::error::Error>> {
        let len = params.len();
        let chunks;
        if len < chunk_size {
            chunks = params.chunks(1).collect::<Vec<_>>();
        } else {
            chunks = params.chunks(len / chunk_size).collect::<Vec<_>>();
        }

        let mut qrys = Vec::new();
        for follow_chunk in chunks {
            let qry = neo4rs::query(query).param(param_name, follow_chunk);
            qrys.push(qry);
        }

        let conn_cl = self.conn.clone();
        match retry(
            ExponentialBackoffBuilder::default()
                .with_initial_interval(Duration::from_millis(250))
                .with_max_elapsed_time(Some(Duration::from_millis(10000)))
                .build(),
            || async {
                let i: Vec<neo4rs::Query> = qrys.iter().map(|v| v.clone()).collect();
                let ilen = &i.len();
                let mut tx = conn_cl.start_txn().await.unwrap();
                match tx.run_queries(i).await {
                    Ok(_) => match tx.commit().await {
                        Ok(_) => {
                            info!("Written {} queries", ilen);
                            Ok(())
                        }
                        Err(e) => Err(backoff::Error::Transient {
                            err: e,
                            retry_after: None,
                        }),
                    },
                    Err(e) => Err(backoff::Error::Transient {
                        err: e,
                        retry_after: None,
                    }),
                }
            },
        )
        .await
        {
            Ok(_) => None,
            Err(e) => Some(Box::new(e)),
        }
    }

    async fn batch_write(
        &self,
        queries: Vec<&str>,
        params: Vec<Option<HashMap<String, String>>>,
    ) -> Option<Box<dyn std::error::Error>> {
        if queries.len() != params.len() {
            error!("Queries and params must be the same length");
            // TODO - Make an error
            return None;
        }

        let mut qrys = Vec::new();
        for (i, q) in queries.iter().enumerate() {
            let mut qry = neo4rs::query(q);
            match params.get(i) {
                Some(p) => match p {
                    Some(p) => qry = qry.params(p.clone()),
                    None => {}
                },
                None => {}
            };
            qrys.push(qry);
        }
        let conn_cl = self.conn.clone();
        match retry(
            ExponentialBackoffBuilder::default()
                .with_initial_interval(Duration::from_millis(250))
                .with_max_elapsed_time(Some(Duration::from_millis(10000)))
                .build(),
            || async {
                let i: Vec<neo4rs::Query> = qrys.iter().map(|v| v.clone()).collect();
                let ilen = &i.len();
                let mut tx = conn_cl.start_txn().await.unwrap();
                match tx.run_queries(i).await {
                    Ok(_) => match tx.commit().await {
                        Ok(_) => {
                            info!("Written {} queries", ilen);
                            Ok(())
                        }
                        Err(e) => Err(backoff::Error::Transient {
                            err: e,
                            retry_after: None,
                        }),
                    },
                    Err(e) => Err(backoff::Error::Transient {
                        err: e,
                        retry_after: None,
                    }),
                }
            },
        )
        .await
        {
            Ok(_) => None,
            Err(e) => Some(Box::new(e)),
        }
    }

    async fn batch_read(
        &self,
        queries: Vec<&str>,
        params: Vec<Option<HashMap<String, String>>>,
    ) -> Result<Vec<HashMap<String, PostMsg>>, Box<dyn std::error::Error>> {
        let mut res = Vec::new();
        if queries.len() != params.len() {
            error!("Queries and params must be the same length");
            // TODO - Make an error
            return Ok(res);
        }

        for (i, q) in queries.iter().enumerate() {
            match self.read(q, q, params[i].clone()).await {
                Ok(r) => {
                    res.push(r);
                }
                Err(e) => return Err(e),
            };
        }

        Ok(res)
    }
}

impl GraphFetcher {
    pub fn new(conn: Graph) -> Self {
        Self { conn }
    }
}
