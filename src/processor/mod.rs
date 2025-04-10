use crate::at_event_processor::ATEventProcessor;
use crate::bsky::types::ATEventType;
use crate::common::FetchMessage;
use crate::filter::Filter;
use crate::filter::FilterList;
use crate::server;
use backoff::ExponentialBackoffBuilder;
use backoff::future::retry;
use dashmap::DashMap;
use neo4rs::{ConfigBuilder, Graph, Query};
use std::collections::VecDeque;
use std::env;
use std::mem;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, time::Instant};
use tokio::sync::{RwLock, mpsc};
use tracing::{error, info, warn};
use uuid::Uuid;

pub mod listen;

use crate::graph::*;
const Q_LIMIT: usize = 55;

const TX_Q_LEN: usize = 70;

macro_rules! queue_event_write {
    ($self:ident, $query_name:expr_2021, $recv:ident, $( $arg:ident ),+) => {{
        let queue_and_query = match $query_name {
            "reply" =>  (&mut $self.reply_queue,queries::ADD_REPLY),
            "post" =>   (&mut $self.post_queue,queries::ADD_POST),
            "repost" => (&mut $self.repost_queue,queries::ADD_REPOST),
            "follow" => (&mut $self.follow_queue,queries::ADD_FOLLOW),
            "block" =>  (&mut $self.block_queue, queries::ADD_BLOCK),
            "like" =>   (&mut $self.like_queue,queries::ADD_LIKE),
            _ => panic!("unknown query name")
        };
        // HashMap-ify the input params w/ the same name as defined in Ruat
        let mut params = HashMap::<String, String>::new();
        $(
            params.insert(stringify!($arg).to_string(), $arg);
        )*
        queue_and_query.0.push(params);
        // Check if the queue is full
        if queue_and_query.0.len() >= Q_LIMIT {
            // Move queue values without copying
            let q = mem::take(queue_and_query.0);
            let queue = Some(queue_and_query.1);
            let resp = $self.enqueue_query(queue, (&pluralize($query_name), q), $recv).await;
            return resp
        }

        $recv // we havent used the channel, so just pass it back up

    }};
}

macro_rules! queue_event_remove {
    ($query_name:expr_2021,$recv:ident, $self:ident, $( $arg:ident ),+) => {{
        let queue_and_query = match $query_name {
            "reply" =>  (&mut $self.rm_reply_queue,queries::REMOVE_REPLY),
            "post" =>   (&mut $self.rm_post_queue,queries::REMOVE_POST),
            "repost" => (&mut $self.rm_repost_queue,queries::REMOVE_REPOST),
            "follow" => (&mut $self.rm_follow_queue,queries::REMOVE_FOLLOW),
            "block" =>  (&mut $self.rm_block_queue, queries::REMOVE_BLOCK),
            "like" =>   (&mut $self.rm_like_queue,queries::REMOVE_LIKE),
            _ => panic!("unknown query name")
        };
        // Helper to build the argument map with variable names as keys
        let mut params = HashMap::new();
        $(
            params.insert(stringify!($arg).to_string(), $arg);
        )*

        queue_and_query.0.push(params);
        // Check if the queue is full
        if queue_and_query.0.len() >= Q_LIMIT {
            // Move queue values without copying
            let q = mem::take(queue_and_query.0);
            let queue = Some(queue_and_query.1);
            let resp = $self.enqueue_query(queue, (&pluralize($query_name), q), $recv).await;
            return resp
        }
        $recv // we havent used the channel, so just pass it back up
    }};
}

pub struct MemgraphWrapper {
    inner: Graph,
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

    filters: HashMap<ATEventType, VecDeque<Box<dyn Filter + Send>>>,
}

impl MemgraphWrapper {
    pub async fn new(
        uri: &str,
        replica_uri: &str,
        user: &str,
        pass: &str,
        recieve_channel: mpsc::Receiver<FetchMessage>,
        lock: Arc<RwLock<()>>,
        filters: HashMap<ATEventType, FilterList>, //FilterList,
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
            info!("Done, Connecting to main...");
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

        // Set off background job to do whatever cleaning we want
        let conn_purge: Graph = inner.clone();

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

        let write_conn = GraphFetcher::new(write_conn);
        let replica = GraphFetcher::new(replica);
        tokio::spawn(async move {
            match listen::listen_for_requests(lock, write_conn, replica, recieve_channel).await {
                Ok(_) => {}
                Err(e) => panic!("Error listening for requests, aborting: {}", e),
            };
        });

        let res = Self {
            inner,
            filters,
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
    async fn enqueue_query(
        &mut self,
        query_script: Option<&str>,
        mut params: (&str, Vec<HashMap<String, String>>),
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
            let name = (params.0.to_owned()).clone();

            tokio::spawn(async move {
                match retry(
                    ExponentialBackoffBuilder::default()
                        .with_initial_interval(Duration::from_millis(2))
                        .with_max_elapsed_time(Some(Duration::from_millis(350)))
                        .with_randomization_factor(0.35)
                        .build(),
                    || async {
                        let q_vals: Vec<Query> = queue.iter().map(|v| v.value().clone()).collect();
                        let mut tx = inner.start_txn().await.unwrap();
                        match tx.run_queries(q_vals).await {
                            Ok(_) => {
                                let el: u128 = n.elapsed().as_millis();
                                if el > 200 {
                                    info!(
                                        "Slow queries on tx: {}ms (~{}/s))",
                                        el,
                                        ((1000000000 as f64 / n.elapsed().as_nanos() as f64)
                                            * (Q_LIMIT as f64 * TX_Q_LEN as f64))
                                            .round()
                                    );
                                }
                                match tx.commit().await {
                                    Ok(_) => Ok(()),

                                    Err(e) => Err(backoff::Error::Transient {
                                        err: e,
                                        retry_after: None,
                                    }),
                                }
                            }
                            Err(e) => Err(backoff::Error::Transient {
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
        // We're using a Map instead of a set because something about DashSet didnt play nice
        // Construct the query
        match query_script {
            Some(s) => {
                let qry = neo4rs::query(s).param(params.0, mem::take(&mut params.1));
                queue.insert(Uuid::new_v4().to_string(), qry);
            }
            None => {
                error!("Expected a query script but none was provided");
                return None;
            }
        }
        prev_recv
    }
}

impl ATEventProcessor for MemgraphWrapper {
    async fn add_reply(
        &mut self,
        did: String,
        rkey: String,
        parent: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = queue_event_write!(self, "reply", rec, did, rkey, parent);
        resp
    }

    async fn add_post(
        &mut self,
        did: String,
        rkey: String,
        timestamp: &i64,
        is_reply: bool,
        post_type: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let is_reply = if is_reply {
            "y".to_owned()
        } else {
            "n".to_owned()
        };

        let timestamp = format! {"{timestamp}"};
        let resp = queue_event_write!(self, "post", rec, did, rkey, is_reply, post_type, timestamp);
        resp
    }

    async fn add_repost(
        &mut self,
        did: String,
        rkey_parent: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = queue_event_write!(self, "repost", rec, did, rkey, rkey_parent);
        resp
    }

    async fn add_follow(
        &mut self,
        did: String,
        out: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = queue_event_write!(self, "follow", rec, out, rkey, did);
        resp
    }

    async fn add_block(
        &mut self,
        blockee: String,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = queue_event_write!(self, "block", rec, blockee, rkey, did);
        resp
    }

    async fn add_like(
        &mut self,
        did: String,
        rkey_parent: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = queue_event_write!(self, "like", rec, did, rkey, rkey_parent);
        resp
    }

    async fn rm_post(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = queue_event_remove!("post", rec, self, did, rkey);
        resp
    }

    async fn rm_repost(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = queue_event_remove!("repost", rec, self, did, rkey);
        resp
    }

    async fn rm_follow(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = queue_event_remove!("follow", rec, self, did, rkey);
        resp
    }

    async fn rm_like(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = queue_event_remove!("like", rec, self, did, rkey);
        resp
    }

    async fn rm_block(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = queue_event_remove!("block", rec, self, did, rkey);
        resp
    }

    async fn rm_reply(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = queue_event_remove!("reply", rec, self, did, rkey);
        resp
    }

    fn get_filters(&self) -> &HashMap<ATEventType, VecDeque<Box<dyn Filter + Send>>> {
        &self.filters
    }
}

fn pluralize(word: &str) -> String {
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
