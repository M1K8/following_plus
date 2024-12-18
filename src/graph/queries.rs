use std::{sync::Arc, time::Duration};

use backoff::{future::retry, Error, ExponentialBackoffBuilder};
use neo4rs::Graph;
use tokio::sync::RwLock;
use tracing::{info, warn};

const PURGE_TIME: u64 = 5 * 60;

pub async fn kickoff_purge(lock: Arc<RwLock<()>>, conn: Graph) -> Result<(), neo4rs::Error> {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(PURGE_TIME)).await;
        info!("Purging");
        let lock = lock.write().await;
        match retry(
            ExponentialBackoffBuilder::default()
                .with_initial_interval(Duration::from_millis(50))
                .with_max_elapsed_time(Some(Duration::from_millis(10000)))
                .build(),
            || async {
                let qry = neo4rs::query(PURGE_OLD_POSTS);
                let qry2: neo4rs::Query = neo4rs::query(PURGE_NO_FOLLOWERS);
                let qry3: neo4rs::Query = neo4rs::query(PURGE_DISCONNECTED);

                let mut tx = conn.start_txn().await.unwrap();
                match tx.run_queries(vec![qry, qry2, qry3]).await {
                    Ok(_) => {
                        let res = match tx.commit().await {
                            Ok(_) => Ok(()),
                            Err(e) => Err(Error::Transient {
                                err: e,
                                retry_after: None,
                            }),
                        };
                        res
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
            Ok(_) => {}
            Err(e) => {
                warn!("Error on purge query: {}", e);
            }
        };
        drop(lock);
        info!("Done!");
    }
}

pub(crate) const ADD_FOLLOW: &str = r#"
UNWIND $follows as follow
MERGE (u:User {did: follow.did})
MERGE (v:User {did: follow.out})
CREATE (u)-[r:FOLLOWS {rkey: follow.rkey }]->(v)
"#;

pub(crate) const POPULATE_FOLLOW: &str = r#"
UNWIND $follows as follow
MERGE (u:User {did: follow.did})
    SET u.tracked = true
MERGE (v:User {did: follow.out})
MERGE (u)-[r:FOLLOWS { rkey: follow.rkey }]->(v)
"#;

pub(crate) const ADD_BLOCK: &str = r#"
UNWIND $blocks as block
MERGE (u:User {did: block.did})
MERGE (v:User {did: block.blockee})
CREATE (u)-[r:BLOCKED {rkey: block.rkey }]->(v)
"#;

pub(crate) const POPULATE_BLOCK: &str = r#"
UNWIND $blocks as block
MERGE (u:User {did: block.did})
MERGE (v:User {did: block.blockee})
MERGE (u)-[r:BLOCKED {rkey: block.rkey }]->(v)
"#;

pub(crate) const ADD_LIKE: &str = r#"
UNWIND $likes as like
MATCH (p:Post) WHERE p.rkey = like.rkey_parent
SET p.likes = p.likes + 1
MERGE (u:User {did: like.did})

CREATE (u)-[r:LIKES {rkey: like.rkey }]->(p)
"#;

pub(crate) const ADD_POST: &str = r#"
UNWIND $posts as post
MERGE (u:User {did: post.did})
CREATE (u)-[:POSTED]->(p: Post { timestamp: post.timestamp, rkey: post.rkey, isReply: post.is_reply , likes: 0, reposts: 0} )
"#;

pub(crate) const ADD_REPOST: &str = r#"
UNWIND $reposts as repost
MATCH (p:Post) WHERE p.rkey = repost.rkey_parent
SET p.reposts = p.reposts + 1
MERGE (u:User {did: repost.did})
CREATE (u)-[r:REPOSTED {rkey: repost.rkey}]->(p)
"#;

pub(crate) const ADD_REPLY: &str = r#"
UNWIND $replies as reply
MATCH (p:Post) WHERE p.rkey = reply.parent
MERGE (u:User {did: reply.did})
CREATE (u)-[r:REPLIED_TO {rkey: reply.rkey }]->(p)
"#;

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const REMOVE_LIKE: &str = r#"
UNWIND $likes as like
MATCH (:User {did: like.did})-[r:LIKES {rkey: like.rkey }]->(p:Post)
SET p.likes = p.likes - 1
DELETE r
"#;

pub(crate) const REMOVE_FOLLOW: &str = r#"
UNWIND $follows as follow
MATCH (:User {did: follow.did})-[r:FOLLOWS {rkey: follow.rkey}]->(:User)
DELETE r
"#;

pub(crate) const REMOVE_BLOCK: &str = r#"
UNWIND $blocks as block
MATCH (:User {did: block.did})-[r:BLOCKED  {rkey: block.rkey} ]->(:User)
DELETE r
"#;

pub(crate) const REMOVE_POST: &str = r#"
UNWIND $posts as post
MATCH (:User {did: post.did})-[r:POSTED ]->(p:Post {rkey: post.rkey})
DETACH DELETE p
"#;

pub(crate) const REMOVE_REPLY: &str = r#"
UNWIND $replies as reply
MATCH (:User {did: reply.did})-[r:REPLIED_TO {rkey: reply.rkey }]->(p:Post) 
with p, r
where p.is_reply == "y"
DELETE r
"#;

pub(crate) const REMOVE_REPOST: &str = r#"
UNWIND $reposts as repost
MATCH (:User {did: repost.did})-[r:REPOSTED {rkey: repost.rkey }]->(p:Post)
SET p.likes = p.reposts - 1
DELETE r
"#;

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const PURGE_OLD_POSTS: &str = r#"
MATCH (p:Post) WHERE toInteger(p.timestamp) < (timestamp() - 7200000000) // 2 hours
DETACH DELETE p
"#;

pub(crate) const PURGE_DISCONNECTED: &str = r#"
MATCH (p:User)-[f:FOLLOWS]->(:User) WHERE p.tracked IS NULL
DETACH DELETE f
"#;

pub(crate) const PURGE_NO_FOLLOWERS: &str = r#"
OPTIONAL MATCH (:User)-[r:FOLLOWS]->(u:User)
  WITH u, count(r) as followers 
WHERE followers = 0
DETACH DELETE u
"#;

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const GET_FOLLOWING_PLUS_LIKES: &str = r#"
MATCH (og:User {did: $did})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(u:User)-[:POSTED]->(p:Post)
WITH u,p,og
// Get all posts 2nd degree follows

WITH og, u, p AS post
OPTIONAL MATCH (og)-[b:BLOCKS]->(u)
with u,b, post, CASE WHEN b IS NULL 
  THEN post ELSE NULL END as p
WHERE p IS NOT NULL AND p.likes >= 100
// Filter off posts from blocked users

WITH p, u, toInteger(p.timestamp) AS ts
WHERE ts < {}

RETURN u.did AS user, p.rkey AS url, ts ORDER BY ts DESC LIMIT 200
"#;

pub(crate) const GET_FOLLOWING_PLUS_REPOSTS: &str = r#"
MATCH (og:User {did: $did})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(u:User)-[:POSTED]->(p:Post)
WITH u,p,og

WITH og, u, p AS post
OPTIONAL MATCH (og)-[b:BLOCKS]->(u)
WITH u,b, post, CASE WHEN b IS NULL 
  THEN post ELSE NULL END as p
WHERE p IS NOT NULL AND p.reposts >= 80
// Filter off posts from blocked users
WITH p, u, toInteger(p.timestamp) AS ts

WHERE ts < {}

RETURN u.did AS user, p.rkey AS url, ts ORDER BY ts DESC LIMIT 200
"#;

pub(crate) const GET_BEST_2ND_DEG_REPOSTS: &str = r#"
MATCH (og:User {did: $did})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(u:User)-[:REPOSTED]->(p:Post)
WITH p,og
 
WHERE p.likes >= 125
MATCH (p)<-[a:POSTED]-(u:User)
WITH DISTINCT p, a, u, og

OPTIONAL MATCH (og)-[b:BLOCKS]->(u)
WITH u, b, p, toInteger(p.timestamp) AS ts, CASE WHEN b IS NULL 
  THEN p ELSE NULL END as post
WHERE post IS NOT NULL AND ts < {}

RETURN u.did AS user, p.rkey AS url, ts ORDER BY ts DESC LIMIT 200
"#;

pub(crate) const GET_BEST_2ND_DEG_LIKES: &str = r#"
MATCH (og:User {did: $did})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(:User)-[:LIKES]->(p:Post)
WITH p,og
WHERE p.likes >= 125

MATCH (p)<-[a:POSTED]-(u:User)
WITH DISTINCT p, a, u, og

OPTIONAL MATCH (og)-[b:BLOCKS]->(u)
WITH u, b, p, toInteger(p.timestamp) AS ts,  CASE WHEN b IS NULL 
  THEN p ELSE NULL END as post
WHERE post IS NOT NULL AND ts < {}

RETURN u.did AS user, p.rkey AS url, ts ORDER BY ts DESC LIMIT 200
"#;
