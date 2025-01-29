use crate::{
    at_event_processor::{ATEventProcessor, MaybeSemaphore},
    bsky::types::*,
};
use chrono::Utc;
use hyper::StatusCode;
use once_cell::sync::Lazy;
use serde::de::DeserializeOwned;
use std::mem;
use tracing::{error, info, warn};
use zstd::bulk::Decompressor;

pub mod types;

const DICT: &'static [u8; 112640] = include_bytes!("./dictionary");
static mut DECOMP: Lazy<Decompressor<'static>> =
    Lazy::new(|| zstd::bulk::Decompressor::with_dictionary(DICT).unwrap());

unsafe fn decompress_fast(m: &[u8]) -> Option<BskyEvent> {
    // Dont _have_ to do this, but https://doc.rust-lang.org/nightly/edition-guide/rust-2024/static-mut-references.html
    let dec_ptr = &raw mut DECOMP;
    let dec_ptr = match dec_ptr.as_mut() {
        Some(p) => p,
        None => panic!("Decompressor is undefined?"),
    };
    let msg = dec_ptr.decompress(m, 819200);
    match msg {
        Ok(m) => {
            match serde_json::from_slice(m.as_slice()) {
                Ok(m) => return Some(m),
                Err(err) => {
                    error!(
                        "Error decompressing payload {:?}: {err}",
                        String::from_utf8(m)
                    );
                    return None;
                }
            };
        }
        Err(err) => panic!("Error getting payload: {err}"),
    };
}

pub async fn handle_event_fast(
    evt: &[u8],
    g: &mut impl ATEventProcessor,
    mut rec: MaybeSemaphore,
    compressed: bool,
) -> Result<(i64, MaybeSemaphore), Box<dyn std::error::Error>> {
    let mut deser_evt: BskyEvent;
    if compressed {
        unsafe {
            deser_evt = decompress_fast(&evt).unwrap();
        }
    } else {
        println!("{:?}", evt);
        match serde_json::from_slice(&evt) {
            Ok(m) => {
                deser_evt = m;
            }
            Err(err) => {
                panic!("unable to marhsal event: {:?}", err)
            }
        };
    }

    // Missing or unrecognised type
    if deser_evt.commit.get_type() == ATEventType::Unknown {
        return Ok((0, rec));
    }

    let filters = g.get_filters();
    // Run global filters first, then those for the given event type
    match filters.get(&ATEventType::Global) {
        Some(f) => {
            for func in f {
                if !func.check(&deser_evt) {
                    return Ok((0, rec));
                }
            }
        }
        None => {}
    };

    let evt_type = deser_evt.commit.get_type();
    match filters.get(&evt_type) {
        Some(f) => {
            for func in f {
                if !func.check(&deser_evt) {
                    return Ok((0, rec));
                }
            }
        }
        None => {}
    };

    // We know commit isnt None as get_type() already does the check
    let mut commit;
    unsafe {
        commit = mem::take(&mut deser_evt.commit).unwrap_unchecked();
    }
    let rkey = mem::take(&mut commit.rkey); //yoinky sploinky
    let now = Utc::now().timestamp_micros();
    let drift = (now - deser_evt.time_us) / 1000;

    if commit.operation == "create" {
        let mut is_reply = false;
        let mut created_at = 0;

        match evt_type {
            ATEventType::Post => {
                match &commit.record {
                    Some(r) => {
                        created_at = match chrono::DateTime::parse_from_rfc3339(&r.created_at) {
                            Ok(t) => t.timestamp_micros(),
                            Err(_) => deser_evt.time_us, // if we cant find this field, just use the time the event was emitted
                        };
                        match &r.reply {
                            Some(r) => {
                                let did_clone = deser_evt.did.clone();
                                let rkey_clone = rkey.clone();
                                let rkey_parent = parse_rkey(&r.parent.uri);
                                rec = g.add_reply(did_clone, rkey_clone, rkey_parent, rec).await;
                                is_reply = true;
                            }
                            _ => {}
                        }

                        match &r.embed {
                            Some(v) => {
                                match &v.video {
                                    Some(_) => panic!("{:?}", evt),
                                    None => {}
                                };
                            }
                            None => {}
                        };
                    }
                    _ => {}
                }

                let recv = g
                    .add_post(
                        deser_evt.did,
                        rkey,
                        &created_at,
                        is_reply,
                        "".to_string(),
                        rec,
                    )
                    .await;

                return Ok((drift, recv));
            }

            ATEventType::Repost => {
                let rkey_out = get_rkey(&commit);

                if rkey_out.is_empty() {
                    panic!("empty rkey");
                }

                let recv = g.add_repost(deser_evt.did, rkey_out, rkey, rec).await;
                return Ok((drift, recv));
            }

            ATEventType::Like => {
                let rkey_out = get_rkey(&commit);

                if rkey_out.is_empty() {
                    panic!("empty rkey");
                }

                let recv = g.add_like(deser_evt.did, rkey_out, rkey, rec).await;
                return Ok((drift, recv));
            }

            ATEventType::Follow => {
                let mut did_out = String::new();
                match &commit.record {
                    Some(r) => {
                        did_out = match &r.subject {
                            Some(s) => match s {
                                Subj::T1(s) => s.to_owned(),
                                Subj::T2(_) => return Ok((0, rec)),
                            },
                            None => return Ok((0, rec)),
                        };
                    }
                    None => {}
                }
                if did_out.is_empty() {
                    panic!("empty did_out");
                }
                let recv = g.add_follow(deser_evt.did, did_out, rkey, rec).await;
                return Ok((drift, recv));
            }

            ATEventType::Block => {
                let mut blockee = String::new();
                match &commit.record {
                    Some(r) => {
                        blockee = match &r.subject {
                            Some(s) => match s {
                                Subj::T1(s) => s.to_owned(),
                                Subj::T2(_) => return Ok((0, rec)),
                            },
                            None => return Ok((0, rec)),
                        };
                    }
                    None => {}
                }
                if blockee.is_empty() {
                    panic!("empty blockee");
                }
                let recv = g.add_block(blockee, deser_evt.did, rkey, rec).await;
                return Ok((drift, recv));
            }
            _ => {}
        }
    } else if commit.operation == "delete" {
        match commit.get_type() {
            ATEventType::Post => {
                let recv = g.rm_post(deser_evt.did, rkey, rec).await;
                return Ok((drift, recv));
            }
            ATEventType::Repost => {
                let recv = g.rm_repost(deser_evt.did, rkey, rec).await;
                return Ok((drift, recv));
            }

            ATEventType::Like => {
                let recv = g.rm_like(deser_evt.did, rkey, rec).await;
                return Ok((drift, recv));
            }
            ATEventType::Follow => {
                let recv = g.rm_follow(deser_evt.did, rkey, rec).await;
                return Ok((drift, recv));
            }
            ATEventType::Block => {
                let recv = g.rm_block(deser_evt.did, rkey, rec).await;
                return Ok((drift, recv));
            }
            _ => {}
        }
    }

    return Ok((0, rec));
}

fn parse_rkey(uri: &str) -> String {
    // the rkey are the last 13 characters
    uri.chars()
        .rev()
        .take(13)
        .collect::<Vec<_>>()
        .iter()
        .rev()
        .collect()
}

fn get_rkey(commit: &Commit) -> String {
    let rkey_out;
    match &commit.record {
        Some(r) => {
            rkey_out = match &r.subject {
                Some(s) => match s {
                    Subj::T1(_) => "".to_owned(),
                    Subj::T2(subject) => parse_rkey(&subject.uri),
                },
                None => "".to_owned(),
            };
        }
        None => rkey_out = "".to_owned(),
    }
    rkey_out
}

pub trait Recordable<V: Subjectable> {
    fn records(&self) -> &Vec<V>;
    fn cursor(&self) -> &Option<String>;
}

pub trait Subjectable {
    fn subject(&self) -> &str;
    fn uri(&self) -> &str;
}

async fn get<V: Subjectable, T: DeserializeOwned + Recordable<V>>(
    uri: &str,
    did: String,
    client: reqwest::Client,
) -> Result<Vec<(String, String)>, (reqwest::Error, Option<StatusCode>)> {
    let mut res: Vec<(String, String)> = Vec::new();
    let mut req = match client.get(uri).build() {
        Ok(r) => r,
        Err(e) => {
            return Err((e, None));
        }
    };
    let mut resp: T = match client.execute(req).await {
        Ok(resp) => {
            let status = resp.status();
            match resp.json().await {
                Ok(r) => r,
                Err(e) => {
                    warn!("resp returned {}: {:?}", status, e);
                    return Err((e, Some(status)));
                }
            }
        }
        Err(e) => {
            let status = e.status();
            return Err((e, status));
        }
    };

    loop {
        for f in resp.records() {
            let subject = f.subject();
            let rkey = parse_rkey(&f.uri());
            res.push((subject.to_owned(), rkey));
        }
        match &resp.cursor() {
            Some(c) => {
                let url = uri.to_owned() + format!("&cursor={}", c).as_str();
                req = match client.get(&url).build() {
                    Ok(r) => r,
                    Err(ee) => return Err((ee, None)),
                };
                let r = match client.execute(req).await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(
                            "Error fetching {} for {} {:?}",
                            std::any::type_name::<T>(),
                            &did,
                            e
                        );
                        continue;
                    }
                };

                let rr = r.status();
                resp = match r.json().await {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(
                            "Error getting {} for {}: {} : {:?}",
                            std::any::type_name::<T>(),
                            did,
                            rr,
                            e
                        );
                        break;
                    }
                };
            }
            None => {
                break;
            }
        }
    }

    Ok(res)
}

pub async fn get_follows(
    did: String,
    client: reqwest::Client,
) -> Result<Vec<(String, String)>, (reqwest::Error, Option<StatusCode>)> {
    info!("Getting follows for {:?}", did);
    let base_url = format!(
        "https://bsky.social/xrpc/com.atproto.repo.listRecords?repo={did}&collection=app.bsky.graph.follow&limit=100"
    );
    get::<Follow, FollowsResp>(&base_url, did, client).await
}

pub async fn get_blocks(
    did: String,
    client: reqwest::Client,
) -> Result<Vec<(String, String)>, (reqwest::Error, Option<StatusCode>)> {
    info!("Getting blocks for {:?}", did);
    let base_url = format!(
        "https://bsky.social/xrpc/com.atproto.repo.listRecords?repo={did}&collection=app.bsky.graph.block&limit=100"
    );
    get::<Block, BlocksResp>(&base_url, did, client).await
}
