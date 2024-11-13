pub(crate) const ADD_FOLLOW: &str = r#"
UNWIND $follows as follow
MERGE (u:User {did: follow.out})
MERGE (v:User {did: follow._in})
MERGE (u)-[r:FOLLOWS {rkey: follow.rkey }]->(v)
"#;

pub(crate) const ADD_BLOCK: &str = r#"
UNWIND $blocks as block
MERGE (u:User {did: block.out})
MERGE (v:User {did: block._in})
MERGE (u)-[r:BLOCKED {rkey: block.rkey }]->(v)
"#;

pub(crate) const ADD_LIKE: &str = r#"
UNWIND $likes as like
MATCH (p:Post) WHERE p.cid = like.cid
MERGE (v:User {did: like.out})
MERGE (v)-[r:LIKES {rkey: like.rkey }]->(p)
"#;

pub(crate) const ADD_POST: &str = r#"
UNWIND $posts as post
MERGE (u:User {did: post.did})
CREATE (u)-[:POSTED]->(p: Post {cid: post.cid, timestamp: timestamp(), rkey: post.rkey } )
"#; // we dont need to store the URI, because we can infer it from the posted rKey & the did

pub(crate) const ADD_REPOST: &str = r#"
UNWIND $reposts as repost
MATCH (p:Post) WHERE p.cid = repost.cid
MERGE (u:User {did: repost.out})

CREATE (u)-[r:REPOSTED {rkey: repost.rkey }]->(p)
"#;

pub(crate) const ADD_REPLY: &str = r#"
UNWIND $replies as reply
MATCH (p:Post) WHERE p.cid = reply.parent
MERGE (u:User {did: reply.did})

CREATE (u)-[r:REPLIED_TO {rkey: reply.rkey }]->(p)
"#;

pub(crate) const RM_FOLLOW: &str = r#"
UNWIND $follows as follow
MATCH (u:User {did: follow.out})
MATCH (v:User {did: follow.in})
MATCH (u)-[r:FOLLOWS]->(v)
DELETE r
"#;

pub(crate) const RM_LIKE: &str = r#"
UNWIND $likes as like
MATCH (u:User {did: like.out})
MATCH (v:User {did: like.in})
MATCH (p:Post ) WHERE p.cid = like.cid
MATCH (v)-[:POSTED]->(p)
MATCH (u)-[r:LIKES]->(p)
DELETE r
"#;

pub(crate) const RM_REPOST: &str = r#"
UNWIND $reposts as repost
MATCH (u:User {did: repost.out})
MATCH (v:User {did: repost.in})
MATCH (p:Post ) WHERE p.cid = repost.cid
MATCH (v)-[:POSTED]->(p)
MATCH (u)-[r:REPOSTED]->(p)
DELETE r
"#;

pub(crate) const RM_POST: &str = r#"
UNWIND $posts as post
MATCH (u:User {did: post.out})
MATCH (u)-[r:POSTED]->(p) WHERE p.cid = post.cid
DETACH DELETE p
"#;
