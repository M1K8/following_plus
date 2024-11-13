use neo4rs::{ConfigBuilder, Graph};
use std::{collections::HashMap, mem, time::Instant};
use whirlwind::ShardSet;
mod queries;

const Q_LIMIT: usize = 55;

macro_rules! add_to_queue2 {
    ($query_name:expr, $self:ident, $( $arg:ident ),+) => {{
        let queue = match $query_name {
            "reply" =>  &mut $self.reply_queue,
            "post" =>  &mut $self.post_queue,
            "repost" =>  &mut $self.repost_queue,
            "follow" =>  &mut $self.follow_queue,
            "block" =>  &mut $self.block_queue,
            "like" =>  &mut $self.like_queue,
            _ => panic!("unknown query name"),
        };

        let query = match $query_name {
            "reply" => queries::ADD_REPLY,
            "post" => queries::ADD_POST,
            "repost" => queries::ADD_REPOST,
            "follow" => queries::ADD_FOLLOW,
            "block" => queries::ADD_BLOCK,
            "like" =>queries::ADD_LIKE,
            _ => panic!("unknown query name"),
        };
        // Helper to build the argument map with variable names as keys
        let mut params = HashMap::new();
        $(
            params.insert(stringify!($arg).to_string(), $arg.clone());
        )*

        // Check if the queue is full
        if queue.len() >= Q_LIMIT {
            queue.push(params);

            let n = Instant::now();

            // Move queue values without copying
            let q = mem::take(queue);
            let qry = neo4rs::query(query).param(&pluralize($query_name), q);
            $self.inner.run(qry).await?;

            println!(
                "Executed query {}: {}ms (~{}/s))",
                stringify!($query_name),
                n.elapsed().as_millis(),
                (1000000000 / n.elapsed().as_nanos()) as f64 * Q_LIMIT as f64
            );
            Ok(())
        } else {
            queue.push(params);
            Ok(())
        }
    }};
}

#[derive(Clone)]
pub struct GraphModel {
    inner: Graph,
    like_queue: Vec<HashMap<String, String>>,
    post_queue: Vec<HashMap<String, String>>,
    reply_queue: Vec<HashMap<String, String>>,
    repost_queue: Vec<HashMap<String, String>>,
    follow_queue: Vec<HashMap<String, String>>,
    block_queue: Vec<HashMap<String, String>>,
}

fn get_post_uri(did: String, rkey: String) -> String {
    format!("at://{did}/app.bsky.feed.post/{rkey}")
}
fn pluralize(word: &str) -> String {
    let word_len = word.len();
    let snip = &word[..word_len - 1];
    let last_char = word.chars().nth(word_len - 1).unwrap();

    let irregular_plurals = [
        ("child", "children"),
        ("man", "men"),
        ("tooth", "teeth"),
        ("foot", "feet"),
        ("mouse", "mice"),
        ("ox", "oxen"),
    ];

    for &(singular, plural) in &irregular_plurals {
        if word == singular {
            return plural.to_string();
        }
    }

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
            .run(neo4rs::query("CREATE INDEX ON :Post(cid)"))
            .await?;
        inner
            .run(neo4rs::query("CREATE EDGE INDEX ON :FOLLOWS(rkey)"))
            .await?;
        inner
            .run(neo4rs::query("CREATE EDGE INDEX ON :LIKES(rkey)"))
            .await?;
        inner
            .run(neo4rs::query("CREATE EDGE INDEX ON :POSTED(rkey)"))
            .await?;
        inner
            .run(neo4rs::query("CREATE EDGE INDEX ON :REPOSTED(rkey)"))
            .await?;
        inner
            .run(neo4rs::query("CREATE EDGE INDEX ON :BLOCKED(rkey)"))
            .await?;
        inner
            .run(neo4rs::query("CREATE EDGE INDEX ON :REPLIED_TO(rkey)"))
            .await?;
        Ok(Self {
            inner,
            like_queue: Default::default(),
            post_queue: Default::default(),
            follow_queue: Default::default(),
            repost_queue: Default::default(),
            block_queue: Default::default(),
            reply_queue: Default::default(),
        })
    }

    pub async fn add_reply(
        &mut self,
        did: &String,
        rkey: &String,
        parent: &String,
    ) -> Result<(), neo4rs::Error> {
        add_to_queue2!(
            "reply", self, // The queue name
            did, rkey, parent // List of variables to include in the map
        )
    }

    pub async fn add_post(
        &mut self,
        did: &String,
        cid: String,
        rkey: &String,
    ) -> Result<(), neo4rs::Error> {
        add_to_queue2!(
            "post", self, // The queue name
            did, cid, rkey // List of variables to include in the map
        )
    }

    pub async fn add_repost(
        &mut self,
        out: String,
        cid: String,
        rkey: String,
    ) -> Result<(), neo4rs::Error> {
        add_to_queue2!(
            "repost", self, // The queue name
            out, rkey, cid // List of variables to include in the map
        )
    }

    pub async fn add_follow(
        &mut self,
        out: String,
        _in: String,
        rkey: String,
    ) -> Result<(), neo4rs::Error> {
        add_to_queue2!(
            "follow", self, // The queue name
            out, rkey, _in // List of variables to include in the map
        )
    }

    pub async fn add_block(
        &mut self,
        out: String,
        _in: String,
        rkey: String,
    ) -> Result<(), neo4rs::Error> {
        add_to_queue2!(
            "block", self, // The queue name
            out, rkey, _in // List of variables to include in the map
        )
    }

    pub async fn add_like(
        &mut self,
        out: String,
        cid: String,
        rkey: String,
    ) -> Result<(), neo4rs::Error> {
        add_to_queue2!(
            "like", self, // The queue name
            out, rkey, cid // List of variables to include in the map
        )
    }

    pub async fn rm_post(&self, did: String, cid: String) -> Result<(), neo4rs::Error> {
        Ok(())
    }

    pub async fn rm_repost(&self, did: String, cid: String) -> Result<(), neo4rs::Error> {
        Ok(())
    }

    pub async fn rm_follow(&self, did_out: String, did_in: String) -> Result<(), neo4rs::Error> {
        Ok(())
    }

    pub async fn rm_like(&self, did_out: String, cid: String) -> Result<(), neo4rs::Error> {
        Ok(())
    }
}
