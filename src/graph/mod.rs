use neo4rs::{ConfigBuilder, Graph};
use std::sync::Arc;
use std::{collections::HashMap, mem, time::Instant};
use tokio::sync::Mutex;
//use whirlwind::ShardSet;
mod queries;

const Q_LIMIT: usize = 75;
const PURGE_TIME: u64 = 60 * 60;

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
        // Helper to build the argument map with variable names as keys
        let mut params = HashMap::new();
        $(
            params.insert(stringify!($arg).to_string(), $arg.clone());
        )*

        // Check if the queue is full
        if queue.0.len() >= Q_LIMIT {
            let _lock = $self.purge_spin.lock().await;
            queue.0.push(params);

            let n = Instant::now();

            // Move queue values without copying
            let q = mem::take(queue.0);
            let qry = neo4rs::query(queue.1).param(&pluralize($query_name), q);
            match  $self.inner.run(qry).await{
                Ok(_) => {},
                Err(e) => {
                    println!("Error on query {}", $query_name);
                    return Err(e);
                }
            };
            drop(_lock);

            let el = n.elapsed().as_millis();
            if el > 3 {
                println!(
                    "Slow query {}: {}ms (~{}/s))",
                    stringify!($query_name),
                    el,
                    (1000000000 / n.elapsed().as_nanos()) as f64 * Q_LIMIT as f64
                );
            }
            Ok(())
        } else {
            queue.0.push(params);
            Ok(())
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
            params.insert(stringify!($arg).to_string(), $arg.clone());
        )*

        // Check if the queue is full
        if queue.0.len() >= (Q_LIMIT / 5) { //rarer
            let _lock = $self.purge_spin.lock().await;
            queue.0.push(params);

            let n = Instant::now();

            // Move queue values without copying
            let q = mem::take(queue.0);
            let qry = neo4rs::query(queue.1).param(&pluralize($query_name), q);
            match  $self.inner.run(qry).await{
                Ok(_) => {},
                Err(e) => {
                    println!("Error on query rm_{}", $query_name);
                    return Err(e);
                }
            };
            drop(_lock);

            let el = n.elapsed().as_millis();
            if el > 3 {
                println!(
                    "Slow query REMOVE {}: {}ms (~{}/s))",
                    stringify!($query_name),
                    el,
                    (1000000000 / n.elapsed().as_nanos()) as f64 * Q_LIMIT as f64
                );
            }

            Ok(())
        } else {
            queue.0.push(params);
            Ok(())
        }
    }};
}

#[derive(Clone)]
pub struct GraphModel {
    inner: Graph,
    purge_spin: Arc<Mutex<()>>,
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

pub async fn kickoff_purge(spin: Arc<Mutex<()>>, conn: Graph) -> Result<(), neo4rs::Error> {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(PURGE_TIME)).await;
        println!("Purging old posts");
        let _lock = spin.lock().await;
        let qry = neo4rs::query(queries::PURGE_OLD_POSTS);
        conn.run(qry).await?;
        println!("Done!");
    }
}

impl GraphModel {
    pub async fn new(uri: &str, user: &str, pass: &str) -> Result<Self, neo4rs::Error> {
        let cfg = ConfigBuilder::new()
            .uri(uri)
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
        let purge_spin = Arc::new(Mutex::new(()));
        let spin = purge_spin.clone();
        let inner_cron = inner.clone();

        tokio::spawn(async move {
            match kickoff_purge(spin, inner_cron).await {
                Ok(_) => {}
                Err(e) => panic!("Error purging old posts, aborting: {}", e),
            };
        });

        let res = Self {
            inner,
            purge_spin,
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
        did: &String,
        rkey: &String,
        parent: &String,
    ) -> Result<(), neo4rs::Error> {
        add_to_queue!("reply", self, did, rkey, parent)
    }

    pub async fn add_post(
        &mut self,
        did: &String,
        rkey: &String,
        timestamp: &i64,
        is_reply: bool,
    ) -> Result<(), neo4rs::Error> {
        let is_reply = if is_reply {
            "y".to_owned()
        } else {
            "n".to_owned()
        };
        let timestamp = format! {"{timestamp}"};
        add_to_queue!("post", self, did, rkey, is_reply, timestamp)
    }

    pub async fn add_repost(
        &mut self,
        did: &String,
        rkey_parent: &String,
        rkey: &String,
    ) -> Result<(), neo4rs::Error> {
        add_to_queue!("repost", self, did, rkey, rkey_parent)
    }

    pub async fn add_follow(
        &mut self,
        out: &String,
        did: &String,
        rkey: &String,
    ) -> Result<(), neo4rs::Error> {
        add_to_queue!("follow", self, out, rkey, did)
    }

    pub async fn add_block(
        &mut self,
        blockee: &String,
        did: &String,
        rkey: &String,
    ) -> Result<(), neo4rs::Error> {
        add_to_queue!("block", self, blockee, rkey, did)
    }

    pub async fn add_like(
        &mut self,
        did: &String,
        rkey_parent: &String,
        rkey: &String,
    ) -> Result<(), neo4rs::Error> {
        add_to_queue!("like", self, did, rkey, rkey_parent)
    }

    pub async fn rm_post(&mut self, did: &String, rkey: &String) -> Result<(), neo4rs::Error> {
        remove_from_queue!("post", self, did, rkey)
    }

    pub async fn rm_repost(&mut self, did: &String, rkey: &String) -> Result<(), neo4rs::Error> {
        remove_from_queue!("repost", self, did, rkey)
    }

    pub async fn rm_follow(&mut self, did: &String, rkey: &String) -> Result<(), neo4rs::Error> {
        remove_from_queue!("follow", self, did, rkey)
    }

    pub async fn rm_like(&mut self, did: &String, rkey: &String) -> Result<(), neo4rs::Error> {
        remove_from_queue!("like", self, did, rkey)
    }

    pub async fn rm_block(&mut self, did: &String, rkey: &String) -> Result<(), neo4rs::Error> {
        remove_from_queue!("block", self, did, rkey)
    }

    pub async fn rm_reply(&mut self, did: &String, rkey: &String) -> Result<(), neo4rs::Error> {
        remove_from_queue!("reply", self, did, rkey)
    }
}

pub fn get_post_uri(did: String, rkey: String) -> String {
    format!("at://{did}/app.bsky.feed.post/{rkey}")
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
