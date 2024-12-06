use tokio::sync::mpsc;

use super::{queries, GraphModel};
use crate::graph::util;
use std::collections::HashMap;
use std::mem;

pub const Q_LIMIT: usize = 60;

macro_rules! add_to_queue {
    ($query_name:expr, $recv:ident, $self:ident, $( $arg:ident ),+) => {{
        let queue = match $query_name {
            "reply" =>  (&mut $self.reply_queue,queries::ADD_REPLY),
            "post" =>   (&mut $self.post_queue,queries::ADD_POST),
            "repost" => (&mut $self.repost_queue,queries::ADD_REPOST),
            "follow" => (&mut $self.follow_queue,queries::ADD_FOLLOW),
            "block" =>  (&mut $self.block_queue, queries::ADD_BLOCK),
            "like" =>   (&mut $self.like_queue,queries::ADD_LIKE),
            _ => panic!("unknown query name")
        };
        // HashMap-ify the input params w/ the same name as defined in Ruat
        let mut params = HashMap::<String, String>::new();
        $(
            params.insert(stringify!($arg).to_string(), $arg);
        )*
        queue.0.push(params);
        // Check if the queue is full
        if queue.0.len() >= Q_LIMIT {
            // Move queue values without copying
            let qry = neo4rs::query(queue.1).param(&util::pluralize($query_name), mem::take(queue.0));
            let resp = $self.enqueue_query($query_name.to_string(), qry, $recv).await;
            return resp
        }

        $recv // we havent used the channel, so just pass it back up

    }};
}

macro_rules! remove_from_queue {
    ($query_name:expr,$recv:ident, $self:ident, $( $arg:ident ),+) => {{
        let queue = match $query_name {
            "reply" =>  (&mut $self.rm_reply_queue,queries::REMOVE_REPLY),
            "post" =>   (&mut $self.rm_post_queue,queries::REMOVE_POST),
            "repost" => (&mut $self.rm_repost_queue,queries::REMOVE_REPOST),
            "follow" => (&mut $self.rm_follow_queue,queries::REMOVE_FOLLOW),
            "block" =>  (&mut $self.rm_block_queue, queries::REMOVE_BLOCK),
            "like" =>   (&mut $self.rm_like_queue,queries::REMOVE_LIKE),
            _ => panic!("unknown query name")
        };
        // Helper to build the argument map with variable names as keys
        let mut params = HashMap::new();
        $(
            params.insert(stringify!($arg).to_string(), $arg);
        )*

        queue.0.push(params);
        // Check if the queue is full
        if queue.0.len() >= Q_LIMIT {
            // Move queue values without copying
            let qry = neo4rs::query(queue.1).param(&util::pluralize($query_name), mem::take(queue.0));
            let resp = $self.enqueue_query($query_name.to_string(), qry, $recv).await;
            return resp
        }

        $recv // we havent used the channel, so just pass it back up
    }};
}

impl GraphModel {
    pub async fn add_reply(
        &mut self,
        did: String,
        rkey: String,
        parent: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = add_to_queue!("reply", rec, self, did, rkey, parent);
        resp
    }

    pub async fn add_post(
        &mut self,
        did: String,
        rkey: String,
        timestamp: &i64,
        is_reply: bool,
        is_image: bool,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let is_reply = if is_reply {
            "y".to_owned()
        } else {
            "n".to_owned()
        };

        let is_image = if is_image {
            "y".to_owned()
        } else {
            "n".to_owned()
        };

        let timestamp = format! {"{timestamp}"};

        let resp = add_to_queue!("post", rec, self, did, rkey, is_reply, is_image, timestamp);
        resp
    }

    pub async fn add_repost(
        &mut self,
        did: String,
        rkey_parent: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = add_to_queue!("repost", rec, self, did, rkey, rkey_parent);
        resp
    }

    pub async fn add_follow(
        &mut self,
        did: String,
        out: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = add_to_queue!("follow", rec, self, out, rkey, did);
        resp
    }

    pub async fn add_block(
        &mut self,
        blockee: String,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = add_to_queue!("block", rec, self, blockee, rkey, did);
        resp
    }

    pub async fn add_like(
        &mut self,
        did: String,
        rkey_parent: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = add_to_queue!("like", rec, self, did, rkey, rkey_parent);
        resp
    }

    pub async fn rm_post(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = remove_from_queue!("post", rec, self, did, rkey);
        resp
    }

    pub async fn rm_repost(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = remove_from_queue!("repost", rec, self, did, rkey);
        resp
    }

    pub async fn rm_follow(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = remove_from_queue!("follow", rec, self, did, rkey);
        resp
    }

    pub async fn rm_like(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = remove_from_queue!("like", rec, self, did, rkey);
        resp
    }

    pub async fn rm_block(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = remove_from_queue!("block", rec, self, did, rkey);
        resp
    }

    pub async fn rm_reply(
        &mut self,
        did: String,
        rkey: String,
        rec: Option<mpsc::Receiver<()>>,
    ) -> Option<mpsc::Receiver<()>> {
        let resp = remove_from_queue!("reply", rec, self, did, rkey);
        resp
    }
}
