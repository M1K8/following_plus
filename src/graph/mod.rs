use neo4rs::{ConfigBuilder, Graph};
use std::sync::Arc;
use std::{collections::HashMap, mem, time::Instant};
use tokio::sync::{mpsc, Mutex};

use crate::common::FetchMessage;
pub mod queries;
mod util;

const Q_LIMIT: usize = 70;

macro_rules! add_to_queue {
    ($query_name:expr, $self:ident, $( $arg:ident ),+) => {{
        let queue = match $query_name {
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

        // Check if the queue is full
        if queue.0.len() >= Q_LIMIT {
            println!("getting lock {}", $query_name);
            let lock = $self.write_lock.lock().await;
            println!("got lock {}", $query_name);
            queue.0.push(params);

            let n = Instant::now();

            // Move queue values without copying
            let q = mem::take(queue.0);
            let qry = neo4rs::query(queue.1).param(&util::pluralize($query_name), q);
            match  $self.inner.run(qry).await{
                Ok(_) => {},
                Err(e) => {
                    println!("Error on query {}", $query_name);
                    return Err(e);
                }
            };
            drop(lock);

            let el = n.elapsed().as_millis();
            if el > 3 {
                println!(
                    "Slow query {}: {}ms (~{}/s))",
                    stringify!($query_name),
                    el,
                    (1000000000 / n.elapsed().as_nanos()) as f64 * Q_LIMIT as f64
                );
                return Ok((true));
            }
            Ok((false))
        } else {
            queue.0.push(params);
            Ok((false))
        }
    }};
}

macro_rules! remove_from_queue {
    ($query_name:expr, $self:ident, $( $arg:ident ),+) => {{
        let queue = match $query_name {
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

        // Check if the queue is full
        if queue.0.len() >= (Q_LIMIT / 2) { //rarer
            println!("getting lock {}", $query_name);
            let lock = $self.write_lock.lock().await;
            println!("got lock {}", $query_name);
            queue.0.push(params);

            let n = Instant::now();


            // Move queue values without copying
            let q = mem::take(queue.0);
            let qry = neo4rs::query(queue.1).param(&util::pluralize($query_name), q);
            match  $self.inner.run(qry).await{
                Ok(_) => {},
                Err(e) => {
                    println!("Error on query rm_{}", $query_name);
                    return Err(e);
                }
            };
            drop(lock);

            let el = n.elapsed().as_millis();
            if el > 3 {
                println!(
                    "Slow query REMOVE {}: {}ms (~{}/s))",
                    stringify!($query_name),
                    el,
                    (1000000000 / n.elapsed().as_nanos()) as f64 * Q_LIMIT as f64
                );
                return Ok((true));
            }
            Ok((false))

        } else {
            queue.0.push(params);
            Ok((false))
        }
    }};
}

pub struct GraphModel {
    inner: Graph,
    write_lock: Arc<Mutex<()>>,
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
}
impl GraphModel {
    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.write_lock.lock().await
    }

    pub fn inner(&self) -> Graph {
        self.inner.clone()
    }

    pub async fn new(
        uri: &str,
        user: &str,
        pass: &str,
        recv: mpsc::Receiver<FetchMessage>,
    ) -> Result<Self, neo4rs::Error> {
        let cfg = ConfigBuilder::new()
            .uri(uri)
            .fetch_size(1024)
            .user(user)
            .password(pass)
            .db("memgraph")
            .build()?;
        let inner = Graph::connect(cfg).await?;
        inner
            .run(neo4rs::query("CREATE INDEX ON :User(did)"))
            .await?;
        inner
            .run(neo4rs::query("CREATE INDEX ON :Post(rkey)"))
            .await?;

        // Set off background job to do whatever cleaning we want
        let write_lock = Arc::new(Mutex::new(()));
        let write_lock_purge = write_lock.clone();
        let conn_purge: Graph = inner.clone();

        // We also want a task to listen for first time user requests
        // As we want to fetch all followers & follows
        let write_lock_new_user = write_lock.clone();
        let conn_new_user = inner.clone();

        tokio::spawn(async move {
            match util::kickoff_purge(write_lock_purge, conn_purge).await {
                Ok(_) => {}
                Err(e) => println!("Error purging old posts: {}", e),
            };
        });

        tokio::spawn(async move {
            match util::listen_channel(write_lock_new_user, conn_new_user, recv).await {
                Ok(_) => {}
                Err(e) => panic!("Error listening for requests, aborting: {}", e),
            };
        });

        let res = Self {
            inner,
            write_lock,
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

    pub async fn add_reply(
        &mut self,
        did: String,
        rkey: String,
        parent: String,
    ) -> Result<bool, neo4rs::Error> {
        add_to_queue!("reply", self, did, rkey, parent)
    }

    pub async fn add_post(
        &mut self,
        did: String,
        rkey: String,
        timestamp: &i64,
        is_reply: bool,
        is_image: bool,
    ) -> Result<bool, neo4rs::Error> {
        let is_reply = if is_reply {
            "y".to_owned()
        } else {
            "n".to_owned()
        };

        let is_image = if is_image {
            "y".to_owned()
        } else {
            "n".to_owned()
        };

        let timestamp = format! {"{timestamp}"};

        add_to_queue!("post", self, did, rkey, is_reply, is_image, timestamp)
    }

    pub async fn add_repost(
        &mut self,
        did: String,
        rkey_parent: String,
        rkey: String,
    ) -> Result<bool, neo4rs::Error> {
        add_to_queue!("repost", self, did, rkey, rkey_parent)
    }

    pub async fn add_follow(
        &mut self,
        out: String,
        did: String,
        rkey: String,
    ) -> Result<bool, neo4rs::Error> {
        add_to_queue!("follow", self, out, rkey, did)
    }

    pub async fn add_block(
        &mut self,
        blockee: String,
        did: String,
        rkey: String,
    ) -> Result<bool, neo4rs::Error> {
        add_to_queue!("block", self, blockee, rkey, did)
    }

    pub async fn add_like(
        &mut self,
        did: String,
        rkey_parent: String,
        rkey: String,
    ) -> Result<bool, neo4rs::Error> {
        add_to_queue!("like", self, did, rkey, rkey_parent)
    }

    pub async fn rm_post(&mut self, did: String, rkey: String) -> Result<bool, neo4rs::Error> {
        remove_from_queue!("post", self, did, rkey)
    }

    pub async fn rm_repost(&mut self, did: String, rkey: String) -> Result<bool, neo4rs::Error> {
        remove_from_queue!("repost", self, did, rkey)
    }

    pub async fn rm_follow(&mut self, did: String, rkey: String) -> Result<bool, neo4rs::Error> {
        remove_from_queue!("follow", self, did, rkey)
    }

    pub async fn rm_like(&mut self, did: String, rkey: String) -> Result<bool, neo4rs::Error> {
        remove_from_queue!("like", self, did, rkey)
    }

    pub async fn rm_block(&mut self, did: String, rkey: String) -> Result<bool, neo4rs::Error> {
        remove_from_queue!("block", self, did, rkey)
    }

    pub async fn rm_reply(&mut self, did: String, rkey: String) -> Result<bool, neo4rs::Error> {
        remove_from_queue!("reply", self, did, rkey)
    }
}
