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
CREATE (u)-[:POSTED {rkey : post.rkey}]->(p: Post { timestamp: post.timestamp, rkey: post.rkey, isReply: post.isReply } )
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
MATCH (:User {did: like.out})-[r:LIKES {rkey: like.rkey }]->(:Post)
DELETE r
"#;

pub(crate) const REMOVE_FOLLOW: &str = r#"
UNWIND $follows as follow
MATCH (:User {did: follow.out})-[r:FOLLOWS {rkey: follow.rkey }]->(:User)
DELETE r
"#;

pub(crate) const REMOVE_BLOCK: &str = r#"
UNWIND $blocks as block
MATCH (:User {did: block.out})-[r:BLOCKED  {rkey: block.rkey} ]->(:User)
DELETE r
"#;

pub(crate) const REMOVE_POST: &str = r#"
UNWIND $posts as post
MATCH (:User {did: post.out})-[r:POSTED {rkey: post.rkey}]->(p:Post)
DETACH DELETE p
"#;

pub(crate) const REMOVE_REPLY: &str = r#"
UNWIND $replies as reply
MATCH (:User {did: reply.out})-[r:REPLIED_TO {rkey: reply.rkey }]->(p:Post) 
with p, r
where p.is_reply == "y"
DELETE r
"#;

pub(crate) const REMOVE_REPOST: &str = r#"
UNWIND $reposts as repost
MATCH (:User {did: repost.out})-[r:REPOSTED {rkey: repost.rkey }]->()
DELETE r
"#;

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const PURGE_OLD_POSTS: &str = r#"
MATCH (p:Post) where toInteger(p.timestamp) < timestamp() - (86400000000) // 24 hours
DETACH DELETE p

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

pub(crate) const GET_FOLLOW_POSTS: &str = r#"
MATCH (og:User {did: $did})-[:FOLLOWS]->(u:User)-[:POSTED]->(p:Post)
WITH u,p,og
OPTIONAL MATCH (og)-[f:FOLLOWS]->(u)
WITH u, p, CASE WHEN f IS NULL 
  THEN p ELSE NULL END AS posts
WHERE posts IS NOT NULL
RETURN u, posts
"#;

pub(crate) const GET_2ND_DEG_FOLLOW_POSTS: &str = r#"
MATCH (og:User {did: $did})-[:FOLLOWS]->(:User)-[:FOLLOWS]->(u:User)-[:POSTED]->(p:Post)
WITH u,p,og
OPTIONAL MATCH (og)-[f:FOLLOWS]->(u)
WITH u, p, CASE WHEN f IS NULL 
  THEN p ELSE NULL END AS posts
WHERE posts IS NOT NULL
RETURN u, posts
"#;
