const ADD_FOLLOW: &str = r#"
UNWIND $follows as follow
MERGE((u:User {did: follow.out})
MERGE (v: User {did: follow.in})
MERGE (u)-[r:FOLLOWS]->(v)

RETURN u,r,v
"#;

const ADD_LIKE: &str = r#"
UNWIND $likes as like
MERGE((u:User {did: like.out})
MERGE (v: User {did: like.in})
MERGE (p: Post {cid: like.cid})
MATCH (v)-[:POSTED]->(p)
MERGE (u)-[r:LIKES]->(p)

RETURN u,r,v
"#;

const ADD_POST: &str = r#"
UNWIND $posts as post
MERGE((u:User {did: post.out})
MERGE (p: Post {cid: post.cid, epoch: timestamp()})
MERGE (u)-[:POSTED]->(p)

RETURN u,p
"#;



const RM_FOLLOW: &str = r#"
UNWIND $follows as follow
MERGE((u:User {did: follow.out})
MERGE (v: User {did: follow.in})
MATCH (u)-[r:FOLLOWS]->(v)
DELETE r

RETURN u
"#;

const RM_LIKE: &str = r#"
UNWIND $likes as like
MERGE((u:User {did: like.out})
MERGE (v: User {did: like.in})
MERGE (p: Post {cid: like.cid})
MATCH (v)-[:POSTED]->(p)
MATCH (u)-[r:LIKES]->(p)
DELETE r

RETURN u
"#;


const RM_POST: &str = r#"
UNWIND $posts as post
MERGE((u:User {did: post.out})
MATCH (u)-[r:POSTED]->(p) WHERE p.cid = post.cid
DETACH DELETE p
RETURN u,p
"#;