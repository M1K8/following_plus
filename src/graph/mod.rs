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
    
    pub async fn add_post(did: String, cid: String) -> Result<(), neo4rs::Error> {

        Ok(())
    }

    // pub async fn add_profile(did: String) -> Result<(), neo4rs::Error> {
    //     Ok(())
    // }

    pub async fn add_follow(didOut: String, didIn: String) -> Result<(), neo4rs::Error> {
        Ok(())
    }

    pub async fn add_like(didOut: String, didIn: String, cid: String) -> Result<(), neo4rs::Error> {
        Ok(())
    }

    pub async fn rm_post(did: String, cid: String) -> Result<(), neo4rs::Error> {
        Ok(())
    }

    // pub async fn rm_profile(did: String) -> Result<(), neo4rs::Error> {
    //     Ok(())
    // }

    pub async fn rm_follow(didOut: String, didIn: String) -> Result<(), neo4rs::Error> {
        Ok(())
    }

    pub async fn rm_like(didOut: String, didIn: String, cid: String) -> Result<(), neo4rs::Error> {
        Ok(())
    }
}


