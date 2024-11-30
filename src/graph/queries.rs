pub(crate) const ADD_FOLLOW: &str = r#"
UNWIND $follows as follow
MERGE (u:User {did: follow.did})
MERGE (v:User {did: follow.out})
CREATE (u)-[r:FOLLOWS {rkey: follow.rkey }]->(v)
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
CREATE (u)-[:POSTED {rkey : post.rkey}]->(p: Post { timestamp: post.timestamp, rkey: post.rkey, isReply: post.is_reply } )
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
MATCH (:User {did: post.did})-[r:POSTED {rkey: post.rkey}]->(p:Post)
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
MATCH (p:Post) where toInteger(p.timestamp) < timestamp() - (3600000000) // 1 hour
DETACH DELETE p
"#;

pub(crate) const PURGE_DUPES: &str = r#"
MATCH (u1:User)-[r:FOLLOWS]->(u2:User), (u1)-[r2:FOLLOWS]->(u2)
WHERE id(r) <> id(r2) AND r1.rkey = r2.rkey
DELETE r2
"#;

pub(crate) const PURGE_NO_FOLLOWERS: &str = r#"
MATCH (u:User)
OPTIONAL MATCH (:User)-[r:FOLLOWS]->(u)
WITH u, count(r) as followers 
WHERE followers = 0
DETACH DELETE u
"#;

pub(crate) const PURGE_NO_FOLLOWING: &str = r#"
MATCH (u:User)
OPTIONAL MATCH (u:User)-[r:FOLLOWS]->(:User)
WITH u, count(r) as follows 
WHERE follows = 0
DETACH DELETE u
"#;

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const GET_FOLLOWING_PLUS_LIKES: &str = r#"
MATCH (og:User {did: $did})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(u:User)-[:POSTED]->(p:Post)
WITH u,p,og
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
WITH u, p, toInteger(p.timestamp) AS ts, count(l) AS likes
MATCH (p) WHERE timestamp() - ts < 162500000 OR likes >= 10
// Filter off posts older than 5 mins that have < 5 likes

RETURN u.did AS user, p.rkey AS url ORDER BY toInteger(p.timestamp) DESC LIMIT 30
"#;

pub(crate) const GET_FOLLOWING_PLUS_REPOSTS: &str = r#"
MATCH (og:User {did: $did})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(u:User)-[:POSTED]->(p:Post)
WITH u,p,og
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
OPTIONAL MATCH (p)<-[rp:REPOSTED]-()
WITH u, p, toInteger(p.timestamp) AS ts, count (rp) AS reposts
MATCH (p) WHERE timestamp() - ts < 162500000 OR reposts >= 10
// Filter off posts older than 5 mins that have < 10 reposts

RETURN u.did AS user, p.rkey AS url ORDER BY toInteger(p.timestamp) DESC LIMIT 30
"#;

pub(crate) const GET_BEST_2ND_DEG_REPOSTS: &str = r#"
MATCH (og:User {did: $did})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(u:User)-[:REPOSTED]->(p:Post)
WITH u,p,og
// Get all reposts 2nd degree follows

OPTIONAL MATCH (og)-[b:BLOCKS]->(u)
with u,b, post, CASE WHEN b IS NULL 
  THEN post ELSE NULL END as p
WHERE p IS NOT NULL
// Filter off posts from blocked users

WITH u, p
OPTIONAL MATCH (p)<-[l:LIKES]-()
WITH u, p, count(l) AS likes
MATCH (p) WHERE likes >= 100


RETURN u.did AS user, p.rkey AS url ORDER BY toInteger(p.timestamp) DESC LIMIT 15
"#;

pub(crate) const GET_BEST_2ND_DEG_LIKES: &str = r#"
MATCH (og:User {did: $did})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(u:User)-[:LIKED]->(p:Post)
WITH u,p,og
// Get all reposts 2nd degree follows

OPTIONAL MATCH (og)-[b:BLOCKS]->(u)
with u,b, post, CASE WHEN b IS NULL 
  THEN post ELSE NULL END as p
WHERE p IS NOT NULL
// Filter off posts from blocked users

WITH u, p
OPTIONAL MATCH (p)<-[l:LIKES]-()
WITH u, p, count(l) AS likes
MATCH (p) WHERE likes >= 100

RETURN u.did AS user, p.rkey AS url ORDER BY toInteger(p.timestamp) DESC LIMIT 15
"#;
