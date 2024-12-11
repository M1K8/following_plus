use crate::common::FetchMessage;
use backoff::future::retry;
use backoff::{Error, ExponentialBackoffBuilder};
use dashmap::DashMap;
use neo4rs::{Config, ConfigBuilder, Graph, Query};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, time::Instant};
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

pub mod queries;
pub mod queue;
mod util;

pub struct GraphModel {
    inner: Graph,
    config: Config,
    like_queue: Vec<HashMap<String, String>>,
    post_queue: Vec<HashMap<String, String>>,
    reply_queue: Vec<HashMap<String, String>>,
    repost_queue: Vec<HashMap<String, String>>,
    follow_queue: Vec<HashMap<String, String>>,
    block_queue: Vec<HashMap<String, String>>,

    rm_like_queue: Vec<HashMap<String, String>>,
    rm_post_queue: Vec<HashMap<String, String>>,
    rm_reply_queue: Vec<HashMap<String, String>>,
    rm_repost_queue: Vec<HashMap<String, String>>,
    rm_follow_queue: Vec<HashMap<String, String>>,
    rm_block_queue: Vec<HashMap<String, String>>,

    tx_queue: Arc<DashMap<String, Query>>,
}

const TX_Q_LEN: usize = 70;

impl GraphModel {
    pub async fn enqueue_query(
        &mut self,
        name: String,
        qry: neo4rs::Query,
        prev_recv: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let inner = self.inner.clone();
        let queue = self.tx_queue.clone();
        if queue.len() > TX_Q_LEN {
            let n = Instant::now();
            let (send, recv) = mpsc::channel(1);
            let id = format!("{:?}", &recv);

            if prev_recv.is_some() {
                prev_recv.unwrap().recv().await;
            }

            tokio::spawn(async move {
                match retry(
                    ExponentialBackoffBuilder::default()
                        .with_initial_interval(Duration::from_millis(5))
                        .with_max_elapsed_time(Some(Duration::from_millis(3450)))
                        .with_randomization_factor(0.35)
                        .build(),
                    || async {
                        let q_vals: Vec<Query> = queue.iter().map(|v| v.value().clone()).collect();
                        let mut tx = inner.start_txn().await.unwrap();
                        match tx.run_queries(q_vals).await {
                            //TOTO - Select for timeouts here
                            Ok(_) => {
                                let el: u128 = n.elapsed().as_millis();
                                if el > 200 {
                                    info!(
                                        "Slow queries on tx: {}ms (~{}/s))",
                                        el,
                                        ((1000000000 as f64 / n.elapsed().as_nanos() as f64)
                                            * (queue::Q_LIMIT as f64 * TX_Q_LEN as f64))
                                            .round()
                                    );
                                }
                                match tx.commit().await {
                                    Ok(_) => Ok(()),

                                    Err(e) => Err(Error::Transient {
                                        err: e,
                                        retry_after: None,
                                    }),
                                }
                            }
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
                        queue.clear();
                    }
                    Err(e) => {
                        warn!("Error on commit query for {}: {}", name, e);
                    }
                };
                match send.send(()).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!(
                            "Something has gone very wrong; unable to send completion channel  {id}: {}",
                            e
                        )
                    }
                };
            });

            return Some(recv);
        }
        let key = name + &(queue.len().to_string());
        queue.insert(key.clone(), qry);

        prev_recv
    }

    pub async fn reset_connection(&mut self) -> Result<(), neo4rs::Error> {
        self.inner = Graph::connect(self.config.clone()).await?;
        Ok(())
    }

    pub async fn new(
        uri: &str,
        replica_uri: &str,
        user: &str,
        pass: &str,
        recv: mpsc::Receiver<FetchMessage>,
        lock: Arc<RwLock<()>>,
    ) -> Result<Self, neo4rs::Error> {
        let replica = env::var("REPLICA").unwrap_or("".into());
        let mut replica_conn = None;
        if replica != "" {
            info!("Connecting to replica first");
            let replica_cfg = ConfigBuilder::new()
                .uri(replica_uri)
                .fetch_size(8192)
                .user(user)
                .password(pass)
                .db("memgraph")
                .build()?;
            let replica_inner = Graph::connect(replica_cfg).await?;
            match replica_inner
                .run(neo4rs::query("CREATE INDEX ON :User(did)"))
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    warn!("Unable to index, it has probably already been set: {}", e);
                }
            };
            match replica_inner
                .run(neo4rs::query("CREATE INDEX ON :Post(rkey)"))
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    warn!("Unable to index, it has probably already been set: {}", e);
                }
            };

            match replica_inner
                .run(neo4rs::query("CREATE INDEX ON :Post"))
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    warn!("Unable to index, it has probably already been set: {}", e);
                }
            };

            match replica_inner
                .run(neo4rs::query(
                    "SET REPLICATION ROLE TO REPLICA WITH PORT 10000;",
                ))
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    warn!(
                        "Unable to set replica, it has probably already been set: {}",
                        e
                    );
                }
            };
            replica_conn = Some(replica_inner);
            info!("Done!\nConnecting to main...");
        }

        let config = ConfigBuilder::new()
            .uri(uri)
            .fetch_size(8192)
            .user(user)
            .password(pass)
            .db("memgraph")
            .build()?;
        let inner = Graph::connect(config.clone()).await?;
        inner
            .run(neo4rs::query("CREATE INDEX ON :User(did)"))
            .await?;
        inner
            .run(neo4rs::query("CREATE INDEX ON :Post(rkey)"))
            .await?;

        //inner.run(neo4rs::query("CREATE INDEX ON :Post")).await?;

        // Set off background job to do whatever cleaning we want
        let conn_purge: Graph = inner.clone();

        //inner.run(neo4rs::query("CREATE INDEX ON :User")).await?;
        if replica != "" {
            match inner
                .run(neo4rs::query(
                    "REGISTER REPLICA REP1 ASYNC TO \"172.18.0.3\";",
                ))
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    warn!(
                        "Unable to set replica on main, it has probably already been set: {}",
                        e
                    );
                }
            };
        }
        info!("Done!");

        // We also want a task to listen for first time user requests
        // As we want to fetch all followers & follows
        let write_conn = inner.clone();
        let lclone = lock.clone();
        tokio::spawn(async move {
            match queries::kickoff_purge(lclone, conn_purge).await {
                Ok(_) => {}
                Err(e) => info!("Error purging old posts: {}", e),
            };
        });
        let replica = match replica_conn {
            Some(r) => r,
            None => inner.clone(),
        };

        let res = Self {
            inner,
            config,
            tx_queue: Arc::new(DashMap::new()),
            like_queue: Default::default(),
            post_queue: Default::default(),
            follow_queue: Default::default(),
            repost_queue: Default::default(),
            block_queue: Default::default(),
            reply_queue: Default::default(),

            rm_like_queue: Default::default(),
            rm_post_queue: Default::default(),
            rm_follow_queue: Default::default(),
            rm_repost_queue: Default::default(),
            rm_block_queue: Default::default(),
            rm_reply_queue: Default::default(),
        };

        Ok(res)
    }
}
