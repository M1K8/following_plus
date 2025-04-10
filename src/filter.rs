use crate::bsky::types::BskyEvent;
use chrono::Utc;
use std::collections::{HashSet, VecDeque};

pub type FilterList = VecDeque<Box<dyn Filter + Send>>;

pub trait Filter {
    fn check(&self, msg: &BskyEvent) -> bool;
}

impl<F> Filter for F
where
    F: Fn(&BskyEvent) -> bool,
{
    fn check(&self, msg: &BskyEvent) -> bool {
        self(msg)
    }
}

pub fn spam_filter(m: &BskyEvent) -> bool {
    let mut spam = HashSet::new();
    spam.insert("did:plc:xdx2v7gyd5dmfqt7v77gf457".to_owned());
    spam.insert("did:plc:a56vfzkrxo2bh443zgjxr4ix".to_owned());
    spam.insert("did:plc:cov6pwd7ajm2wgkrgbpej2f3".to_owned());
    spam.insert("did:plc:fcnbisw7xl6lmtcnvioocffz".to_owned());
    spam.insert("did:plc:ss7fj6p6yfirwq2hnlkfuntt".to_owned());

    return !spam.contains(&m.did);
}

pub fn date_filter(m: &BskyEvent) -> bool {
    match &m.commit {
        Some(c) => {
            match &c.record {
                Some(r) => {
                    let created_at = match &r.created_at {
                        crate::bsky::types::StringOrInt::T1(s) => s,
                        crate::bsky::types::StringOrInt::T2(i) => &format!("{i}"),
                    };
                    match chrono::DateTime::parse_from_rfc3339(&created_at) {
                        Ok(t) => {
                            return Utc::now().timestamp_micros() - t.timestamp_micros()
                                < chrono::Duration::hours(24).num_microseconds().unwrap();
                        }
                        Err(_) => {
                            return Utc::now().timestamp_micros() - m.time_us
                                < chrono::Duration::hours(24).num_microseconds().unwrap();
                        } // if we cant find this field, just use the time the event was emitted
                    };
                }
                None => return true,
            }
        }
        None => true,
    }
}
