#[cfg(test)]
mod graph_test {
    use std::collections::{HashMap, VecDeque};

    use tokio::select;
    use uuid::Uuid;

    use crate::{
        at_event_processor::{ATEventProcessor, MaybeSemaphore},
        bsky::types::ATEventType,
        filter::Filter,
        graph::queries,
    };

    // These just check the calls call enqueue_query porperly
    #[tokio::test]
    async fn check_single_query() {
        let mut tg = TestGraph::new();

        tg.add_block(
            "did:blockee".to_owned(),
            "did:user1".to_owned(),
            "rkey_block".to_owned(),
            None,
        )
        .await;

        let mut queue = tg.get_queue();
        let v = queue.pop_front().unwrap();

        let mut was_found = false;
        assert_eq!(v.len(), 1);
        for (_, val) in v {
            if val.0 == "blocks" {
                was_found = true;

                assert_eq!(val.1.len(), 1);
                assert_eq!(val.1[0].get("blockee").unwrap(), "did:blockee");
                assert_eq!(val.1[0].get("rkey").unwrap(), "rkey_block");
                assert_eq!(val.1[0].get("did").unwrap(), "did:user1");

                break;
            }
        }

        assert!(was_found);
    }

    #[tokio::test]
    async fn check_multiple_query() {
        let mut tg = TestGraph::new();

        tg.add_block(
            "did:blockee".to_owned(),
            "did:user1".to_owned(),
            "rkey_block".to_owned(),
            None,
        )
        .await;
        tg.add_block(
            "did:blockee2".to_owned(),
            "did:user1".to_owned(),
            "rkey_block2".to_owned(),
            None,
        )
        .await;
        tg.add_block(
            "did:blockee3".to_owned(),
            "did:user1".to_owned(),
            "rkey_block3".to_owned(),
            None,
        )
        .await;

        let queue = tg.get_queue();
        assert_eq!(queue.len(), 3);
        for v in queue {
            assert_eq!(v.len(), 1);
            for (_, val) in v {
                if val.0 == "blocks" {
                    assert_eq!(val.1.len(), 1);
                    if val.1[0].get("rkey").unwrap() == "rkey_block" {
                        assert_eq!(val.1[0].get("blockee").unwrap(), "did:blockee");
                        assert_eq!(val.1[0].get("did").unwrap(), "did:user1");
                    } else if val.1[0].get("rkey").unwrap() == "rkey_block2" {
                        assert_eq!(val.1[0].get("blockee").unwrap(), "did:blockee2");
                        assert_eq!(val.1[0].get("did").unwrap(), "did:user1");
                    } else if val.1[0].get("rkey").unwrap() == "rkey_block3" {
                        assert_eq!(val.1[0].get("blockee").unwrap(), "did:blockee3");
                        assert_eq!(val.1[0].get("did").unwrap(), "did:user1");
                    }
                }
            }
        }
    }

    struct TestGraph {
        filters: HashMap<ATEventType, VecDeque<Box<dyn Filter + Send>>>,
        queue: VecDeque<HashMap<String, (String, Vec<HashMap<String, String>>)>>,
    }

    impl TestGraph {
        fn get_queue(&self) -> VecDeque<HashMap<String, (String, Vec<HashMap<String, String>>)>> {
            self.queue.clone()
        }

        fn new() -> Self {
            Self {
                filters: HashMap::new(),
                queue: VecDeque::new(),
            }
        }

        async fn enqueue_query(
            &mut self,
            query_script: Option<&str>,
            params: (&str, Vec<HashMap<String, String>>),
            sem: MaybeSemaphore,
        ) -> MaybeSemaphore {
            assert!(query_script.is_some());
            let script = params.0.to_owned();
            let inner_params = (script, params.1);
            let mut inner = HashMap::new();
            inner.insert(Uuid::new_v4().to_string(), inner_params);
            match sem {
                Some(mut s) => {
                    select! {
                        _ = s.recv() => {},
                        _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                            panic!("empty channel")
                        }
                    }
                }
                None => {}
            };

            self.queue.push_back(inner);
            None
        }
    }

    impl ATEventProcessor for TestGraph {
        fn get_filters(&self) -> &HashMap<ATEventType, VecDeque<Box<dyn Filter + Send>>> {
            &self.filters
        }

        async fn add_reply(
            &mut self,
            did: String,
            rkey: String,
            parent: String,
            sem: MaybeSemaphore,
        ) -> MaybeSemaphore {
            self.enqueue_query(
                Some(queries::ADD_REPLY),
                (
                    "replies",
                    vec![HashMap::from([
                        ("parent".to_owned(), parent),
                        ("rkey".to_owned(), rkey),
                        ("did".to_owned(), did),
                    ])],
                ),
                sem,
            )
            .await
        }

        async fn add_post(
            &mut self,
            did: String,
            rkey: String,
            timestamp: &i64,
            is_reply: bool,
            is_image: bool,
            sem: MaybeSemaphore,
        ) -> MaybeSemaphore {
            self.enqueue_query(
                Some(queries::ADD_POST),
                (
                    "posts",
                    vec![HashMap::from([
                        ("timestamp".to_owned(), format!("{timestamp}")),
                        ("is_reply".to_owned(), format!("{is_reply}")),
                        ("is_image".to_owned(), format!("{is_image}")),
                        ("rkey".to_owned(), rkey),
                        ("did".to_owned(), did),
                    ])],
                ),
                sem,
            )
            .await
        }

        async fn add_repost(
            &mut self,
            did: String,
            rkey_parent: String,
            rkey: String,
            sem: MaybeSemaphore,
        ) -> MaybeSemaphore {
            self.enqueue_query(
                Some(queries::ADD_REPOST),
                (
                    "reposts",
                    vec![HashMap::from([
                        ("rkey_parent".to_owned(), rkey_parent),
                        ("rkey".to_owned(), rkey),
                        ("did".to_owned(), did),
                    ])],
                ),
                sem,
            )
            .await
        }

        async fn add_follow(
            &mut self,
            did: String,
            out: String,
            rkey: String,
            sem: MaybeSemaphore,
        ) -> MaybeSemaphore {
            self.enqueue_query(
                Some(queries::ADD_FOLLOW),
                (
                    "follows",
                    vec![HashMap::from([
                        ("out".to_owned(), out),
                        ("rkey".to_owned(), rkey),
                        ("did".to_owned(), did),
                    ])],
                ),
                sem,
            )
            .await
        }

        async fn add_like(
            &mut self,
            did: String,
            rkey_parent: String,
            rkey: String,
            sem: MaybeSemaphore,
        ) -> MaybeSemaphore {
            self.enqueue_query(
                Some(queries::ADD_LIKE),
                (
                    "likes",
                    vec![HashMap::from([
                        ("rkey_parent".to_owned(), rkey_parent),
                        ("rkey".to_owned(), rkey),
                        ("did".to_owned(), did),
                    ])],
                ),
                sem,
            )
            .await
        }

        async fn add_block(
            &mut self,
            blockee: String,
            did: String,
            rkey: String,
            sem: MaybeSemaphore,
        ) -> MaybeSemaphore {
            self.enqueue_query(
                Some(queries::ADD_BLOCK),
                (
                    "blocks",
                    vec![HashMap::from([
                        ("blockee".to_owned(), blockee),
                        ("rkey".to_owned(), rkey),
                        ("did".to_owned(), did),
                    ])],
                ),
                sem,
            )
            .await
        }

        async fn rm_post(
            &mut self,
            did: String,
            rkey: String,
            sem: MaybeSemaphore,
        ) -> MaybeSemaphore {
            self.enqueue_query(
                Some(queries::REMOVE_POST),
                (
                    "posts",
                    vec![HashMap::from([
                        ("rkey".to_owned(), rkey),
                        ("did".to_owned(), did),
                    ])],
                ),
                sem,
            )
            .await
        }

        async fn rm_repost(
            &mut self,
            did: String,
            rkey: String,
            sem: MaybeSemaphore,
        ) -> MaybeSemaphore {
            self.enqueue_query(
                Some(queries::REMOVE_REPOST),
                (
                    "reposts",
                    vec![HashMap::from([
                        ("rkey".to_owned(), rkey),
                        ("did".to_owned(), did),
                    ])],
                ),
                sem,
            )
            .await
        }

        async fn rm_follow(
            &mut self,
            did: String,
            rkey: String,
            sem: MaybeSemaphore,
        ) -> MaybeSemaphore {
            self.enqueue_query(
                Some(queries::REMOVE_FOLLOW),
                (
                    "follows",
                    vec![HashMap::from([
                        ("rkey".to_owned(), rkey),
                        ("did".to_owned(), did),
                    ])],
                ),
                sem,
            )
            .await
        }

        async fn rm_like(
            &mut self,
            did: String,
            rkey: String,
            sem: MaybeSemaphore,
        ) -> MaybeSemaphore {
            self.enqueue_query(
                Some(queries::REMOVE_LIKE),
                (
                    "likes",
                    vec![HashMap::from([
                        ("rkey".to_owned(), rkey),
                        ("did".to_owned(), did),
                    ])],
                ),
                sem,
            )
            .await
        }

        async fn rm_block(
            &mut self,
            did: String,
            rkey: String,
            sem: MaybeSemaphore,
        ) -> MaybeSemaphore {
            self.enqueue_query(
                Some(queries::REMOVE_BLOCK),
                (
                    "blocks",
                    vec![HashMap::from([
                        ("rkey".to_owned(), rkey),
                        ("did".to_owned(), did),
                    ])],
                ),
                sem,
            )
            .await
        }

        async fn rm_reply(
            &mut self,
            did: String,
            rkey: String,
            sem: MaybeSemaphore,
        ) -> MaybeSemaphore {
            self.enqueue_query(
                Some(queries::REMOVE_REPLY),
                (
                    "replies",
                    vec![HashMap::from([
                        ("rkey".to_owned(), rkey),
                        ("did".to_owned(), did),
                    ])],
                ),
                sem,
            )
            .await
        }
    }
}
