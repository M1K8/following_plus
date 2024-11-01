use neo4rs::{query, Graph, Node};
mod queries;

pub struct GraphModel {
    inner: Graph,
}

impl GraphModel {
    pub async fn new(uri: &str, user: &str, pass: &str) -> Result<Self, neo4rs::Error> {
        let inner = Graph::new(uri, user, pass).await?;
        Ok(Self { inner })
    }

    pub async fn add_post(&self, did: String, cid: String) -> Result<(), neo4rs::Error> {
        Ok(())
    }

    pub async fn add_follow(&self, didOut: String, didIn: String) -> Result<(), neo4rs::Error> {
        Ok(())
    }

    pub async fn add_like(
        &self,
        didOut: String,
        didIn: String,
        cid: String,
    ) -> Result<(), neo4rs::Error> {
        Ok(())
    }

    pub async fn rm_post(&self, did: String, cid: String) -> Result<(), neo4rs::Error> {
        Ok(())
    }

    pub async fn rm_follow(&self, didOut: String, didIn: String) -> Result<(), neo4rs::Error> {
        Ok(())
    }

    pub async fn rm_like(
        &self,
        didOut: String,
        didIn: String,
        cid: String,
    ) -> Result<(), neo4rs::Error> {
        Ok(())
    }
}
