use std::{collections::HashMap, sync::Arc, time::Duration};

use backoff::{future::retry, Error, ExponentialBackoffBuilder};
use dashmap::DashSet;
use hyper::StatusCode;
use neo4rs::{Graph, Query};
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::{
    bsky,
    graph::queries::{self},
};

pub async fn get_follows(
    did_follows: &String,
    cl_follows: reqwest::Client,
) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let follows = match bsky::get_follows(did_follows.clone(), cl_follows).await {
        Ok(f) => f,
        Err(e) => {
            let err_str = format!("{:?}", e);
            let status: StatusCode = match e.1 {
                Some(s) => s,
                None => StatusCode::IM_A_TEAPOT,
            };
            if err_str.contains("missing field `records`")
                && status == StatusCode::from_u16(400).unwrap()
            {
                return Err(Box::new(RecNotFound {}));
            }
            return Err(Box::new(e.0));
        }
    };

    Ok(follows)
}

pub async fn get_blocks(
    did_blocks: &String,
    cl_blocks: reqwest::Client,
    conn: &Graph,
    write_lock: Arc<RwLock<()>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let blocks = match bsky::get_blocks(did_blocks.clone(), cl_blocks).await {
        Ok(f) => f,
        Err(e) => return Err(Box::new(e)),
    };

    let block_chunks: Vec<HashMap<String, String>> = blocks
        .iter()
        .map(|vals| {
            HashMap::from([
                ("out".to_owned(), vals.0.clone()),
                ("did".to_owned(), did_blocks.clone()),
                ("rkey".to_owned(), vals.1.clone()),
            ])
        })
        .collect();
    let block_chunks = block_chunks.chunks(60).collect::<Vec<_>>();
    for block_chunk in block_chunks {
        let qry = neo4rs::query(queries::POPULATE_BLOCK).param("blocks", block_chunk);
        let l = write_lock.read().await;
        match conn.run(qry).await {
            Ok(_) => {}
            Err(e) => {
                info!("Error on backfilling blocks for {}", &did_blocks);
                drop(l);
                return Err(Box::new(e));
            }
        };
        drop(l);
    }

    info!("Written {} blocks for {}", blocks.len(), &did_blocks);

    Ok(())
}

pub async fn write_follows(
    follows: Arc<DashSet<(String, String, String)>>,
    conn: &Graph,
    write_lock: Arc<RwLock<()>>,
) -> Option<Box<dyn std::error::Error>> {
    let follow_chunks: Vec<HashMap<String, String>> = follows
        .iter()
        .map(|vals| {
            HashMap::from([
                ("out".to_owned(), vals.0.clone()),
                ("rkey".to_owned(), vals.1.clone()),
                ("did".to_owned(), vals.2.clone()),
            ])
        })
        .collect();
    let len = follow_chunks.len();
    let chunks;
    if len < 20 as usize {
        chunks = follow_chunks.chunks(1).collect::<Vec<_>>();
    } else {
        chunks = follow_chunks.chunks(len / 20).collect::<Vec<_>>();
    }

    let mut qrys = Vec::new();
    for follow_chunk in chunks {
        let qry = neo4rs::query(queries::POPULATE_FOLLOW).param("follows", follow_chunk);
        qrys.push(qry);
    }
    let conn_cl = conn.clone();
    let l = write_lock.write().await;
    match retry(
        ExponentialBackoffBuilder::default()
            .with_initial_interval(Duration::from_millis(250))
            .with_max_elapsed_time(Some(Duration::from_millis(10000)))
            .build(),
        || async {
            let i: Vec<Query> = qrys.iter().map(|v| v.clone()).collect();
            let ilen = &i.len();
            let mut tx = conn_cl.start_txn().await.unwrap();
            match tx.run_queries(i).await {
                Ok(_) => match tx.commit().await {
                    Ok(_) => {
                        info!("Written {} queries", ilen);
                        Ok(())
                    }
                    Err(e) => Err(Error::Transient {
                        err: e,
                        retry_after: None,
                    }),
                },
                Err(e) => Err(Error::Transient {
                    err: e,
                    retry_after: None,
                }),
            }
        },
    )
    .await
    {
        Ok(_) => {
            info!("Written {} follows", len);
            drop(l);
        }
        Err(e) => {
            drop(l);
            warn!("Error committing: {}", e);
            return Some(Box::new(e));
        }
    }
    info!("Done!");
    None
}

#[derive(Debug)]
pub struct RecNotFound {}

impl std::fmt::Display for RecNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordNotFound")
    }
}

impl core::error::Error for RecNotFound {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}
