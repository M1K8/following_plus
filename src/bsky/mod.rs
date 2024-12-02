use crate::bsky::types::*;
use crate::graph::GraphModel;
use chrono::Utc;
use once_cell::sync::Lazy;

use std::mem;
use std::{collections::HashSet, time::SystemTime};
use tracing::{error, info};
use zstd::bulk::Decompressor;

mod types;

const DICT: &'static [u8; 112640] = include_bytes!("./dictionary");
static mut DECOMP: Lazy<Decompressor<'static>> =
    Lazy::new(|| zstd::bulk::Decompressor::with_dictionary(DICT).unwrap());

unsafe fn decompress_fast(m: &[u8]) -> Option<BskyEvent> {
    let msg = DECOMP.decompress(m, 51200); // 5kb
    match msg {
        Ok(m) => {
            match serde_json::from_slice(m.as_slice()) {
                Ok(m) => return Some(m),
                Err(err) => {
                    error!("{:?}", SystemTime::now());
                    panic!("Error decompressing payload: {err}")
                }
            };
        }
        Err(err) => panic!("Error getting payload: {err}"),
    };
}

pub async fn handle_event_fast(
    evt: &[u8],
    g: &mut GraphModel,
    compressed: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut spam = HashSet::new();
    spam.insert("did:plc:xdx2v7gyd5dmfqt7v77gf457".to_owned());
    spam.insert("did:plc:a56vfzkrxo2bh443zgjxr4ix".to_owned());
    spam.insert("did:plc:cov6pwd7ajm2wgkrgbpej2f3".to_owned());

    // this ones fucking weird
    spam.insert("did:plc:fcnbisw7xl6lmtcnvioocffz".to_owned());
    // no hate, but bro...
    spam.insert("did:plc:ss7fj6p6yfirwq2hnlkfuntt".to_owned());

    let deser_evt: BskyEvent;
    if compressed {
        unsafe {
            deser_evt = decompress_fast(&evt).unwrap();
        }
    } else {
        match serde_json::from_slice(&evt) {
            Ok(m) => {
                deser_evt = m;
            }
            Err(err) => {
                panic!("unable to marhsal event: {:?}", err)
            }
        };
    }

    let commit: &Commit = match &deser_evt.commit {
        Some(m) => m,
        None => {
            return Ok(());
        }
    };
    let rkey = commit.rkey.clone();

    let drift = (Utc::now().naive_utc().and_utc().timestamp_micros() - deser_evt.time_us) / 1000;
    // if drift > 35000 {
    //     panic!("{drift}ms late (probably need to speed up ingest)!!!");
    // }
    //info!("{drift} ms");
    if spam.contains(&deser_evt.did) {
        return Ok(());
    }
    let now = Utc::now().timestamp_micros();

    if commit.operation == "create" {
        let mut is_reply = false;
        let mut is_image = false;
        let mut created_at = 0;
        match commit.collection.as_str() {
            "app.bsky.feed.post" => {
                match &commit.record {
                    Some(r) => {
                        is_image = r.images.is_some();
                        created_at = match chrono::DateTime::parse_from_rfc3339(&r.created_at) {
                            Ok(t) => {
                                if now - t.timestamp_micros()
                                    > chrono::Duration::hours(24).num_microseconds().unwrap()
                                {
                                    return Ok(());
                                }
                                t.timestamp_micros()
                            }
                            Err(_) => deser_evt.time_us,
                        };
                        match &r.reply {
                            Some(r) => {
                                let did_clone = deser_evt.did.clone();
                                let rkey_clone = rkey.clone();
                                let rkey_parent = parse_rkey(&r.parent.uri);
                                g.add_reply(did_clone, rkey_clone, rkey_parent).await?;
                                is_reply = true;
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }

                let res = g
                    .add_post(deser_evt.did, rkey, &created_at, is_reply, is_image)
                    .await?;
                match res {
                    true => {
                        info!("{drift}ms late")
                    }
                    false => {}
                }
            }

            "app.bsky.feed.repost" => {
                let rkey_out = get_rkey(&commit);

                if rkey_out.is_empty() {
                    panic!("empty rkey");
                }

                let res = g
                    .add_repost(deser_evt.did, rkey_out.to_string(), rkey)
                    .await?;
                match res {
                    true => {
                        info!("{drift}ms late")
                    }
                    false => {}
                }
            }

            "app.bsky.feed.like" => {
                let rkey_out = get_rkey(&commit);

                if rkey_out.is_empty() {
                    panic!("empty rkey");
                }

                let res = g
                    .add_like(deser_evt.did, rkey_out.to_string(), rkey)
                    .await?;
                match res {
                    true => {
                        info!("{drift}ms late")
                    }
                    false => {}
                }
            }

            "app.bsky.graph.follow" => {
                let mut did_out = String::new();
                match &commit.record {
                    Some(r) => {
                        did_out = match &r.subject {
                            Some(s) => match s {
                                Subj::T1(s) => s.to_owned(),
                                Subj::T2(_) => return Ok(()),
                            },
                            None => return Ok(()),
                        };
                    }
                    None => {}
                }
                if did_out.is_empty() {
                    panic!("empty did_out");
                }
                let res = g.add_follow(deser_evt.did, did_out, rkey).await?;
                match res {
                    true => {
                        info!("{drift}ms late")
                    }
                    false => {}
                }
            }

            "app.bsky.graph.block" => {
                let mut vblockee = String::new();
                match &commit.record {
                    Some(r) => {
                        vblockee = match &r.subject {
                            Some(s) => match s {
                                Subj::T1(s) => s.to_owned(),
                                Subj::T2(_) => return Ok(()),
                            },
                            None => return Ok(()),
                        };
                    }
                    None => {}
                }
                if vblockee.is_empty() {
                    panic!("empty vblockee");
                }
                g.add_block(vblockee, deser_evt.did, rkey).await?;
            }
            _ => {
                //info!("{:?}", mm);
            }
        }
    } else if commit.operation == "delete" {
        match commit.collection.as_str() {
            "app.bsky.feed.post" => {
                g.rm_post(deser_evt.did, rkey).await?;
            }
            "app.bsky.feed.repost" => {
                g.rm_repost(deser_evt.did, rkey).await?;
            }

            "app.bsky.feed.like" => {
                g.rm_like(deser_evt.did, rkey).await?;
            }
            "app.bsky.graph.follow" => {
                g.rm_follow(deser_evt.did, rkey).await?;
                info!("{drift}ms late")
            }
            "app.bsky.graph.block" => {
                g.rm_block(deser_evt.did, rkey).await?;
            }
            _ => {
                //info!("{:?}", deser_evt);
            }
        }
    } else {
        // TODO - Handle Updates (lists, starterpacks?)
        //info!("{:?}", deser_evt.commit.unwrap().collection);
    }

    return Ok(());
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

////// https://bsky.social/xrpc/com.atproto.repo.listRecords?repo=did:plc:xq3lwzdpijivr5buiizezlni&collection=app.bsky.graph.follow
pub async fn get_follows(
    did: String,
    client: reqwest::Client,
) -> Result<Vec<(String, String)>, reqwest::Error> {
    info!("Getting follows for {:?}", did);
    let base_url = format!(
        "https://bsky.social/xrpc/com.atproto.repo.listRecords?repo={did}&collection=app.bsky.graph.follow&limit=100"
    );
    let mut follows: Vec<(String, String)> = Vec::new();
    let mut req = match client.get(&base_url).build() {
        Ok(r) => r,
        Err(e) => {
            info!("req {:?}", e);
            return Err(e);
        }
    };
    let mut resp: FollowsResp = match client.execute(req).await?.json().await {
        Ok(r) => r,
        Err(e) => {
            info!("resp {:?}", e);
            return Err(e);
        }
    };

    loop {
        for f in &mut resp.records {
            let subject = mem::take(&mut f.value.subject); // yoink the string, not gonna need it anymore in the vec anyway
            let rkey = parse_rkey(&f.uri);
            follows.push((subject, rkey));
        }
        match &resp.cursor {
            Some(c) => {
                info!("Processing {:?}", c);
                let url = base_url.clone() + format!("&cursor={}", c).as_str();
                req = client.get(&url).build()?;
                let r = match client.execute(req).await {
                    Ok(r) => r,
                    Err(e) => panic!("{:?}", e),
                };
                resp = match r.json().await {
                    Ok(r) => r,
                    Err(e) => {
                        info!("{:?}", e);
                        break;
                    }
                };
            }
            None => {
                break;
            }
        }
    }

    Ok(follows)
}

pub async fn get_blocks(
    did: String,
    client: reqwest::Client,
) -> Result<Vec<(String, String)>, reqwest::Error> {
    info!("Getting blocks for {:?}", did);
    let base_url = format!(
        "https://bsky.social/xrpc/com.atproto.repo.listRecords?repo={did}&collection=app.bsky.graph.block&limit=100"
    );
    let mut blocks: Vec<(String, String)> = Vec::new();
    let mut req = match client.get(&base_url).build() {
        Ok(r) => r,
        Err(e) => {
            info!("req {:?}", e);
            return Err(e);
        }
    };
    let mut resp: FollowsResp = match client.execute(req).await?.json().await {
        Ok(r) => r,
        Err(e) => {
            info!("resp {:?}", e);
            return Err(e);
        }
    };

    loop {
        for f in &mut resp.records {
            let subject = mem::take(&mut f.value.subject); // yoink the string, not gonna need it anymore in the vec anyway
            let rkey = parse_rkey(&f.uri);
            blocks.push((subject, rkey));
        }
        match &resp.cursor {
            Some(c) => {
                info!("Processing {:?}", c);
                let url = base_url.clone() + format!("&cursor={}", c).as_str();
                req = client.get(&url).build()?;
                let r = match client.execute(req).await {
                    Ok(r) => r,
                    Err(e) => panic!("{:?}", e),
                };
                resp = match r.json().await {
                    Ok(r) => r,
                    Err(e) => {
                        info!("{:?}", e);
                        break;
                    }
                };
            }
            None => {
                break;
            }
        }
    }

    Ok(blocks)
}
