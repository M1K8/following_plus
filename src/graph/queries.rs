use std::{sync::Arc, time::Duration};

use backoff::{future::retry, Error, ExponentialBackoffBuilder};
use neo4rs::Graph;
use tokio::sync::RwLock;
use tracing::{info, warn};

const PURGE_TIME: u64 = 5 * 60;

pub async fn kickoff_purge(lock: Arc<RwLock<()>>, conn: Graph) -> Result<(), neo4rs::Error> {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(PURGE_TIME)).await;
        info!("Purging & analyzing");
        match retry(
            // this'll only take effect when getting new accounts or pruging
            // TODO - Purging is still failing - take another look at the locking logic & make sure its putting a stopper (very possible that ongoing transactions are still running)
            ExponentialBackoffBuilder::default()
                .with_initial_interval(Duration::from_millis(50))
                .with_max_elapsed_time(Some(Duration::from_millis(10000)))
                .build(),
            || async {
                let anal = neo4rs::query("ANALYZE GRAPH");
                let qry = neo4rs::query(PURGE_OLD_POSTS);
                let qry2: neo4rs::Query = neo4rs::query(PURGE_NO_FOLLOWERS);
                match conn.run(anal).await {
                    Ok(_) => {}
                    Err(e) => {
                        warn!("Error on analyze query: {}", e);
                    }
                };

                let mut tx = conn.start_txn().await.unwrap();
                let lock = lock.write().await;
                match tx.run_queries(vec![qry, qry2]).await {
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
MERGE (v:User {did: follow.out})
MERGE (u)-[r:FOLLOWS {rkey: follow.rkey }]->(v)
"#;

pub(crate) const ADD_BLOCK: &str = r#"
UNWIND $blocks as block
MERGE (u:User {did: block.did})
MERGE (v:User {did: block.blockee})
CREATE (u)-[r:BLOCKED {rkey: block.rkey }]->(v)
"#;

pub(crate) const ADD_LIKE: &str = r#"
UNWIND $likes as like
MATCH (p:Post) WHERE p.rkey = like.rkey_parent
MERGE (u:User {did: like.did})

CREATE (u)-[r:LIKES {rkey: like.rkey }]->(p)
"#;

pub(crate) const ADD_POST: &str = r#"
UNWIND $posts as post
MERGE (u:User {did: post.did})
CREATE (u)-[:POSTED]->(p: Post { timestamp: post.timestamp, rkey: post.rkey, isReply: post.is_reply } )
"#;

pub(crate) const ADD_REPOST: &str = r#"
UNWIND $reposts as repost
MATCH (p:Post) WHERE p.rkey = repost.rkey_parent
MERGE (u:User {did: repost.did})
CREATE (u)-[r:REPOSTED {rkey: repost.rkey }]->(p)
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
MATCH (:User {did: like.did})-[r:LIKES {rkey: like.rkey }]->(:Post)
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
MATCH (:User {did: repost.did})-[r:REPOSTED {rkey: repost.rkey }]->()
DELETE r
"#;

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const PURGE_OLD_POSTS: &str = r#"
MATCH (p:Post) WHERE toInteger(p.timestamp) < (timestamp() - 1440000000) // 4 hours
DETACH DELETE p
"#;

pub(crate) const PURGE_DUPE_FOLLOWS: &str = r#"
MATCH (p:User)-[f:FOLLOWS]->(q:User)
MATCH (p)-[f2:FOLLOWS]->(q)
WHERE f.rkey = f2.rkey AND id(f) <> id(f2)
DETACH DELETE f2

"#;

pub(crate) const PURGE_NO_FOLLOWERS: &str = r#"
MATCH (u:User) WHERE u.seen IS null
OPTIONAL MATCH (:User)-[r:FOLLOWS]->(u)
  WITH u, count(r) as followers 
WHERE followers = 0
OPTIONAL MATCH (u)-[r:FOLLOWS]->(:User)
  WITH u, count(r) as follows 
WHERE follows = 0
OPTIONAL MATCH (u)-[r:POSTED]->(:Post)
  WITH u, count(r) AS posts
WHERE posts = 0
DETACH DELETE u
"#;

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const GET_FOLLOWING_PLUS_LIKES: &str = r#"
MATCH (og:User {did: $did})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(u:User)-[:POSTED]->(p:Post)
WITH u,p,og
WHERE p.isReply != "y"
// Get all posts 2nd degree follows

OPTIONAL MATCH (og)-[f:FOLLOWS]->(u)
WITH *, CASE WHEN f IS NULL 
  THEN p ELSE NULL END as post
WHERE post IS NOT NULL
// Filter off posts from 1st degree follows

WITH og, u, post
OPTIONAL MATCH (og)-[b:BLOCKS]->(u)
with u,b, post, CASE WHEN b IS NULL 
  THEN post ELSE NULL END as p
WHERE p IS NOT NULL
// Filter off posts from blocked users

WITH u, p
OPTIONAL MATCH (p)<-[l:LIKES]-()
WITH p, u, l, toInteger(p.timestamp) AS ts, count(l) AS likes
WHERE l IS NOT NULL
MATCH (p) WHERE likes >= 50
WITH DISTINCT p, u, ts
WHERE ts < {}

RETURN u.did AS user, p.rkey AS url, p.isReply AS isReply, ts ORDER BY ts DESC LIMIT 20
"#;

pub(crate) const GET_FOLLOWING_PLUS_REPOSTS: &str = r#"
MATCH (og:User {did: $did})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(u:User)-[:POSTED]->(p:Post)
WITH u,p,og
WHERE p.isReply != "y"
// Get all posts 2nd degree follows

OPTIONAL MATCH (og)-[f:FOLLOWS]->(u)
WITH *, CASE WHEN f IS NULL 
  THEN p ELSE NULL END as post
WHERE post IS NOT NULL
// Filter off posts from 1st degree follows

WITH og, u, post
OPTIONAL MATCH (og)-[b:BLOCKS]->(u)
WITH u,b, post, CASE WHEN b IS NULL 
  THEN post ELSE NULL END as p
WHERE p IS NOT NULL
// Filter off posts from blocked users

WITH u, p
OPTIONAL MATCH (p)<-[rp:REPOSTED]-()
WITH p, u, rp, toInteger(p.timestamp) AS ts, count (rp) AS reposts
WHERE rp IS NOT NULL
MATCH (p) WHERE reposts >= 50
// Filter off posts older than 5 mins that have < 10 reposts
WITH DISTINCT p, u, ts
WHERE ts < {}

RETURN u.did AS user, p.rkey AS url, p.isReply AS isReply, ts ORDER BY ts DESC LIMIT 20
"#;

pub(crate) const GET_BEST_2ND_DEG_REPOSTS: &str = r#"
MATCH (og:User {did: $did})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(u:User)-[:REPOSTED]->(p:Post)
WITH p,og
WHERE p.isReply != "y"
// Get all reposts 2nd degree follows

WITH p, og
OPTIONAL MATCH (p)<-[l:LIKES]-()
WITH p, og,  l, count(l) AS likes 
WHERE l IS NOT NULL AND likes >= 75
MATCH (p)<-[a:POSTED]-(u:User)
WITH DISTINCT p, a, u, og

OPTIONAL MATCH (og)-[b:BLOCKS]->(u)
WITH u, b, p, toInteger(p.timestamp) AS ts, CASE WHEN b IS NULL 
  THEN p ELSE NULL END as post
WHERE post IS NOT NULL AND ts < {}

RETURN u.did AS user, p.rkey AS url, p.isReply AS isReply,  ts ORDER BY ts DESC LIMIT 20
"#;

pub(crate) const GET_BEST_2ND_DEG_LIKES: &str = r#"
MATCH (og:User {did: $did})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(:User)-[:LIKES]->(p:Post)
WITH p,og
WHERE p.isReply != "y"
// Get all reposts 2nd degree follows

WITH p, og
OPTIONAL MATCH (p)<-[l:LIKES]-()
WITH p, og,  l, count(l) AS likes 
WHERE l IS NOT NULL AND likes >= 75
MATCH (p)<-[a:POSTED]-(u:User)
WITH DISTINCT p, a, u, og

OPTIONAL MATCH (og)-[b:BLOCKS]->(u)
WITH u, b, p, toInteger(p.timestamp) AS ts,  CASE WHEN b IS NULL 
  THEN p ELSE NULL END as post
WHERE post IS NOT NULL AND ts < {}

RETURN u.did AS user, p.rkey AS url, p.isReply AS isReply, ts ORDER BY ts DESC LIMIT 20
"#;
