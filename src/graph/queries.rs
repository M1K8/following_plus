pub(crate) const ADD_FOLLOW: &str = r#"
UNWIND $follows as follow
MERGE (u: User {did: follow.out})
MERGE (v: User {did: follow.in})
CREATE (u)-[r:FOLLOWS]->(v)
"#;

pub(crate) const ADD_LIKE: &str = r#"
UNWIND $likes as like
MERGE (p: Post {cid: like.cid})
MERGE (v: User {did: like.in})
CREATE (u)-[r:LIKES]->(p)
"#;

pub(crate) const ADD_POST: &str = r#"
UNWIND $posts as post
MERGE (u:User {did: post.did})
CREATE (p: Post {cid: post.cid, epoch: timestamp() } )
CREATE (u)-[:POSTED]->(p)
"#;

pub(crate) const ADD_REPOST: &str = r#"
UNWIND $reposts as repost
MERGE (u:User {did: repost.out})
MERGE (v: User {did: repost.in})

MERGE (p: Post {cid: repost.cid})
MATCH (v)-[:POSTED]->(p)
CREATE (u)-[r:REPOSTED]->(p)

"#;

pub(crate) const RM_FOLLOW: &str = r#"
UNWIND $follows as follow
MERGE (u:User {did: follow.out})
MERGE (v: User {did: follow.in})
MATCH (u)-[r:FOLLOWS]->(v)
DELETE r
"#;

pub(crate) const RM_LIKE: &str = r#"
UNWIND $likes as like
MERGE (u:User {did: like.out})
MERGE (v: User {did: like.in})
MATCH (p: Post {cid: like.cid})
MATCH (v)-[:POSTED]->(p)
MATCH (u)-[r:LIKES]->(p)
DELETE r
"#;

pub(crate) const RM_REPOST: &str = r#"
UNWIND $reposts as repost
MERGE (u:User {did: repost.out})
MERGE (v: User {did: repost.in})
MERGE (p: Post {cid: repost.cid})
MATCH (v)-[:POSTED]->(p)
MATCH (u)-[r:REPOSTED]->(p)
DELETE r
"#;

pub(crate) const RM_POST: &str = r#"
UNWIND $posts as post
MERGE (u:User {did: post.out})
MATCH (u)-[r:POSTED]->(p) WHERE p.cid = post.cid
DETACH DELETE p
"#;
