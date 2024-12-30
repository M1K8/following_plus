use std::collections::HashMap;
use tokio::sync::mpsc;

pub type MaybeSemaphore = Option<mpsc::Receiver<()>>;

#[trait_variant::make(Send)]
pub trait ATEventProcessor {
    async fn enqueue_query(
        &mut self,
        name: String,
        query_script: Option<&str>,
        params: (&str, Vec<HashMap<String, String>>),
        prev_recv: MaybeSemaphore,
    ) -> MaybeSemaphore;

    async fn add_reply(
        &mut self,
        did: String,
        rkey: String,
        parent: String,
        rec: MaybeSemaphore,
    ) -> MaybeSemaphore;

    async fn add_post(
        &mut self,
        did: String,
        rkey: String,
        timestamp: &i64,
        is_reply: bool,
        is_image: bool,
        rec: MaybeSemaphore,
    ) -> MaybeSemaphore;

    async fn add_repost(
        &mut self,
        did: String,
        rkey_parent: String,
        rkey: String,
        rec: MaybeSemaphore,
    ) -> MaybeSemaphore;

    async fn add_follow(
        &mut self,
        did: String,
        out: String,
        rkey: String,
        rec: MaybeSemaphore,
    ) -> MaybeSemaphore;

    async fn add_like(
        &mut self,
        did: String,
        rkey_parent: String,
        rkey: String,
        rec: MaybeSemaphore,
    ) -> MaybeSemaphore;

    async fn add_block(
        &mut self,
        blockee: String,
        did: String,
        rkey: String,
        rec: MaybeSemaphore,
    ) -> MaybeSemaphore;

    //////
    async fn rm_post(&mut self, did: String, rkey: String, rec: MaybeSemaphore) -> MaybeSemaphore;

    async fn rm_repost(&mut self, did: String, rkey: String, rec: MaybeSemaphore)
        -> MaybeSemaphore;

    async fn rm_follow(&mut self, did: String, rkey: String, rec: MaybeSemaphore)
        -> MaybeSemaphore;

    async fn rm_like(&mut self, did: String, rkey: String, rec: MaybeSemaphore) -> MaybeSemaphore;

    async fn rm_block(&mut self, did: String, rkey: String, rec: MaybeSemaphore) -> MaybeSemaphore;

    async fn rm_reply(&mut self, did: String, rkey: String, rec: MaybeSemaphore) -> MaybeSemaphore;
}
