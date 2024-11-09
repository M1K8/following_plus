use neo4rs::{ConfigBuilder, Graph};
use std::{collections::HashMap, mem, sync::Arc, time::Instant};
use whirlwind::ShardSet;
mod queries;

const Q_LIMIT: usize = 55;

#[derive(Clone)]
pub struct GraphModel {
    inner: Graph,
    like_queue: Vec<HashMap<String, String>>,
    post_queue: Vec<HashMap<String, String>>,
    repost_queue: Vec<HashMap<String, String>>,
    follow_queue: Vec<HashMap<String, String>>,
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
        Ok(Self {
            inner,
            like_queue: Default::default(),
            post_queue: Default::default(),
            follow_queue: Default::default(),
            repost_queue: Default::default(),
        })
    }

    pub async fn add_post(&mut self, did: String, cid: String) -> Result<(), neo4rs::Error> {
        // Refac into a queue, have separate runner draining queue continuously
        // Do we even need to though?
        if self.post_queue.len() >= Q_LIMIT {
            let mut params = HashMap::new();
            params.insert("did".to_string(), did.clone());
            params.insert("cid".to_string(), cid.clone());
            self.post_queue.push(params);

            let n = Instant::now();

            // We dont wanna copy this vector, so just yoink its values
            let q = mem::take(&mut self.post_queue);
            let qry = neo4rs::query(queries::ADD_POST).param("posts", q);
            self.inner.run(qry).await?;
            println!(
                "Add Posts: {}ms (~{}/s))",
                n.elapsed().as_millis(),
                (1000000000 / n.elapsed().as_nanos()) as f64 * Q_LIMIT as f64
            );
            return Ok(());
        }
        let mut h = HashMap::new();
        h.insert("did".to_string(), did.clone());
        h.insert("cid".to_string(), cid.clone());
        self.post_queue.push(h);
        Ok(())
    }

    pub async fn add_repost(&mut self, out: String, cid: String) -> Result<(), neo4rs::Error> {
        // Refac into a queue, have separate runner draining queue continuously
        // Do we even need to though?
        if self.repost_queue.len() >= Q_LIMIT {
            let mut params = HashMap::new();
            params.insert("out".to_string(), out.clone());
            params.insert("cid".to_string(), cid.clone());
            self.repost_queue.push(params);

            let n = Instant::now();

            // We dont wanna copy this vector, so just yoink its values
            let q = mem::take(&mut self.repost_queue);
            let qry = neo4rs::query(queries::ADD_REPOST).param("reposts", q);
            self.inner.run(qry).await?;
            println!(
                "Add RePosts: {}ms (~{}/s))",
                n.elapsed().as_millis(),
                (1000000000 / n.elapsed().as_nanos()) as f64 * Q_LIMIT as f64
            );
            return Ok(());
        }
        let mut h = HashMap::new();
        h.insert("out".to_string(), out.clone());
        h.insert("cid".to_string(), cid.clone());
        self.repost_queue.push(h);
        Ok(())
    }

    pub async fn add_follow(
        &mut self,
        did_out: String,
        did_in: String,
    ) -> Result<(), neo4rs::Error> {
        // Refac into a queue, have separate runner draining queue continuously
        // Do we even need to though?
        if self.follow_queue.len() >= Q_LIMIT {
            let mut params = HashMap::new();
            params.insert("out".to_string(), did_out.clone());
            params.insert("in".to_string(), did_in.clone());
            self.follow_queue.push(params);

            let n = Instant::now();

            // We dont wanna copy this vector, so just yoink its values
            let q = mem::take(&mut self.follow_queue);
            let qry = neo4rs::query(queries::ADD_FOLLOW).param("follows", q);
            self.inner.run(qry).await?;
            println!(
                "Add Follows: {}ms (~{}/s))",
                n.elapsed().as_millis(),
                (1000000000 / n.elapsed().as_nanos()) as f64 * Q_LIMIT as f64
            );
            return Ok(());
        }
        let mut h = HashMap::new();
        h.insert("out".to_string(), did_out.clone());
        h.insert("in".to_string(), did_in.clone());
        self.follow_queue.push(h);
        Ok(())
    }

    pub async fn add_like(&mut self, did_out: String, cid: String) -> Result<(), neo4rs::Error> {
        if self.like_queue.len() >= Q_LIMIT {
            let mut params = HashMap::new();
            params.insert("out".to_string(), did_out.clone());
            params.insert("cid".to_string(), cid.clone());
            self.like_queue.push(params);

            let n = Instant::now();

            // We dont wanna copy this vector, so just yoink its values
            let q = mem::take(&mut self.like_queue);
            let qry = neo4rs::query(queries::ADD_LIKE).param("likes", q);

            self.inner.run(qry).await?;
            println!(
                "Add Likes: {}ms (~{}/s))",
                n.elapsed().as_millis(),
                (1000000000 / n.elapsed().as_nanos()) as f64 * Q_LIMIT as f64
            );
            return Ok(());
        }
        let mut h = HashMap::new();
        h.insert("out".to_string(), did_out.clone());
        h.insert("cid".to_string(), cid.clone());
        self.like_queue.push(h);
        Ok(())
    }

    pub async fn rm_post(&self, did: String, cid: String) -> Result<(), neo4rs::Error> {
        Ok(())
    }

    pub async fn rm_follow(&self, did_out: String, did_in: String) -> Result<(), neo4rs::Error> {
        Ok(())
    }

    pub async fn rm_like(
        &self,
        did_out: String,
        did_in: String,
        cid: String,
    ) -> Result<(), neo4rs::Error> {
        Ok(())
    }
}
