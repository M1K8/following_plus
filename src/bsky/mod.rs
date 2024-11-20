use crate::bsky::types::*;
use crate::graph::GraphModel;
use chrono::Utc;
use once_cell::sync::Lazy;
use std::collections::HashSet;
use std::mem;
use zstd::bulk::Decompressor;
mod types;

const DICT: &'static [u8; 112640] = include_bytes!("./dictionary");
static mut DECOMP: Lazy<Decompressor<'static>> =
    Lazy::new(|| zstd::bulk::Decompressor::with_dictionary(DICT).unwrap());

fn decompress(m: tokio_tungstenite::tungstenite::Message) -> Option<BskyEvent> {
    unsafe {
        let msg = DECOMP.decompress(m.into_data().as_slice(), 1024000);
        match msg {
            Ok(m) => {
                match serde_json::from_slice(m.as_slice()) {
                    Ok(m) => return Some(m),
                    Err(_err) => {
                        panic!("1")
                    }
                };
            }
            Err(_) => panic!("2"),
        };
    }
}

pub async fn handle_event(
    evt: Result<tokio_tungstenite::tungstenite::Message, tokio_tungstenite::tungstenite::Error>,
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
    match evt {
        Ok(msg) => {
            let deser_evt: BskyEvent;
            if compressed {
                deser_evt = decompress(msg).unwrap();
            } else {
                match serde_json::from_slice(msg.into_data().as_slice()) {
                    Ok(m) => {
                        deser_evt = m;
                    }
                    Err(_err) => {
                        panic!("1")
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

            let drift =
                (Utc::now().naive_utc().and_utc().timestamp_micros() - deser_evt.time_us) / 1000;
            if drift > 30000 {
                panic!("{drift}ms late (probably need to speed up ingest)!!!");
            }
            //println!("{drift}ms late");
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
                                created_at =
                                    match chrono::DateTime::parse_from_rfc3339(&r.created_at) {
                                        Ok(t) => {
                                            if now - t.timestamp_micros()
                                                > chrono::Duration::hours(24)
                                                    .num_microseconds()
                                                    .unwrap()
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
                                println!("{drift}ms late")
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
                                println!("{drift}ms late")
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
                                println!("{drift}ms late")
                            }
                            false => {}
                        }
                    }

                    "app.bsky.graph.follow" => {
                        let mut did_in = String::new();
                        match &commit.record {
                            Some(r) => {
                                did_in = match &r.subject {
                                    Some(s) => match s {
                                        Subj::T1(s) => s.to_owned(),
                                        Subj::T2(_) => return Ok(()),
                                    },
                                    None => return Ok(()),
                                };
                            }
                            None => {}
                        }
                        if did_in.is_empty() {
                            panic!("empty did_in");
                        }
                        let res = g.add_follow(deser_evt.did, did_in, rkey).await?;
                        match res {
                            true => {
                                println!("{drift}ms late")
                            }
                            false => {}
                        }
                    }

                    "app.bsky.graph.block" => {
                        let mut did_in = String::new();
                        match &commit.record {
                            Some(r) => {
                                did_in = match &r.subject {
                                    Some(s) => match s {
                                        Subj::T1(s) => s.to_owned(),
                                        Subj::T2(_) => return Ok(()),
                                    },
                                    None => return Ok(()),
                                };
                            }
                            None => {}
                        }
                        if did_in.is_empty() {
                            panic!("empty did_in");
                        }
                        g.add_block(deser_evt.did, did_in, rkey).await?;
                    }
                    _ => {
                        //println!("{:?}", mm);
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
                    }
                    "app.bsky.graph.block" => {
                        g.rm_block(deser_evt.did, rkey).await?;
                    }
                    _ => {
                        //println!("{:?}", deser_evt);
                    }
                }
            } else {
                // TODO - Handle Updates (lists, starterpacks?)
                //println!("{:?}", deser_evt.commit.unwrap().collection);
            }

            return Ok(());
        }
        Err(err) => {
            return Err(err.into());
        }
    }
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

pub async fn get_followers(
    did: String,
    client: &reqwest::Client,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut url = format!(
        "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollowers?limit=100&actor={}",
        did
    );
    let mut followers: Vec<String> = Vec::new();
    let mut req = client.get(&url).build()?;
    let mut resp: FollowersResp = client.execute(req).await?.json().await?;

    loop {
        for f in &mut resp.followers {
            let str = mem::take(&mut f.did); // yoink the string, not gonna need it anymore in the vec anyway
            followers.push(str);
        }
        match &resp.cursor {
            Some(c) => {
                url = url + format!("&cursor={}", c).as_str();
                req = client.get(&url).build()?;
                resp = client.execute(req).await?.json().await?;
            }
            None => {
                break;
            }
        }
    }

    Ok(followers)
}

pub async fn get_follows(
    did: String,
    client: &reqwest::Client,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut url = format!(
        "https://public.api.bsky.app/xrpc/app.bsky.graph.getFollows?limit=100&actor={}",
        did
    );
    let mut follows: Vec<String> = Vec::new();
    let mut req = client.get(&url).build()?;
    let mut resp: FollowsResp = client.execute(req).await?.json().await?;

    loop {
        for f in &mut resp.follows {
            let str = mem::take(&mut f.did); // yoink the string, not gonna need it anymore in the vec anyway
            follows.push(str);
        }
        match &resp.cursor {
            Some(c) => {
                url = url + format!("&cursor={}", c).as_str();
                req = client.get(&url).build()?;
                resp = client.execute(req).await?.json().await?;
            }
            None => {
                break;
            }
        }
    }

    Ok(follows)
}
