use std::collections::{HashMap, VecDeque};
use tokio::sync::mpsc;

use crate::{bsky::types::ATEventType, filter::Filter};

pub type MaybeSemaphore = Option<mpsc::Receiver<()>>;

#[trait_variant::make(Send)]
pub trait ATEventProcessor {
    fn get_filters(&self) -> &HashMap<ATEventType, VecDeque<Box<dyn Filter + Send>>>;
    ///
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
        post_type: String,
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
