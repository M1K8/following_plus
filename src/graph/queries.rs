pub(crate) const ADD_FOLLOW: &str = r#"
UNWIND $follows as follow
MERGE (u:User {did: follow.out})
MERGE (v:User {did: follow.in})
MERGE (u)-[r:FOLLOWS]->(v)
"#;

pub(crate) const ADD_LIKE: &str = r#"
UNWIND $likes as like
MATCH (p:Post) WHERE p.cid = like.cid
MERGE (v:User {did: like.out})
MERGE (v)-[r:LIKES]->(p)
"#;

pub(crate) const ADD_POST: &str = r#"
UNWIND $posts as post
MERGE (u:User {did: post.did})
CREATE (u)-[:POSTED]->(p: Post {cid: post.cid, epoch: timestamp() } )
"#;

pub(crate) const ADD_REPOST: &str = r#"
UNWIND $reposts as repost
MATCH (p:Post) WHERE p.cid = repost.cid
MERGE (u:User {did: repost.out})

CREATE (u)-[r:REPOSTED]->(p)

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
