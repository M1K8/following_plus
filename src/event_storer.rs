use tokio::sync::mpsc;

#[trait_variant::make(Send)]
pub trait EventStorer {
    async fn enqueue_query(
        &mut self,
        name: String,
        qry: neo4rs::Query,
        prev_recv: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>>;

    async fn add_reply(
        &mut self,
        did: String,
        rkey: String,
        parent: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>>;

    async fn add_post(
        &mut self,
        did: String,
        rkey: String,
        timestamp: &i64,
        is_reply: bool,
        is_image: bool,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>>;

    async fn add_repost(
        &mut self,
        did: String,
        rkey_parent: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>>;

    async fn add_follow(
        &mut self,
        did: String,
        out: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>>;

    async fn add_like(
        &mut self,
        did: String,
        rkey_parent: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>>;

    async fn add_block(
        &mut self,
        blockee: String,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>>;

    //////
    async fn rm_post(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>>;

    async fn rm_repost(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>>;

    async fn rm_follow(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>>;

    async fn rm_like(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>>;

    async fn rm_block(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>>;

    async fn rm_reply(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>>;
}
