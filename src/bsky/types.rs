use std::collections::HashMap;

use serde_derive::Deserialize;
use serde_derive::Serialize;

use super::Recordable;
use super::Subjectable;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FollowsResp {
    pub cursor: Option<String>,
    pub records: Vec<Follow>,
}
impl Recordable<Follow> for FollowsResp {
    fn records(&self) -> &Vec<Follow> {
        &self.records
    }

    fn cursor(&self) -> &Option<String> {
        &self.cursor
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Follow {
    pub uri: String,
    pub cid: String,
    pub value: FollowVal,
}

impl Subjectable for Follow {
    fn subject(&self) -> &str {
        &self.value.subject
    }
    fn uri(&self) -> &str {
        &self.uri
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub uri: String,
    pub cid: String,
    pub value: FollowVal,
}

impl Subjectable for Block {
    fn subject(&self) -> &str {
        &self.value.subject
    }
    fn uri(&self) -> &str {
        &self.uri
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BlocksResp {
    pub cursor: Option<String>,
    pub records: Vec<Block>,
}
impl Recordable<Block> for BlocksResp {
    fn records(&self) -> &Vec<Block> {
        &self.records
    }

    fn cursor(&self) -> &Option<String> {
        &self.cursor
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FollowVal {
    #[serde(rename = "$type")]
    pub type_field: Option<String>,
    pub subject: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BskyEvent {
    pub did: String,
    #[serde(rename = "time_us")]
    pub time_us: i64,
    pub kind: String,
    #[serde(rename = "type")]
    pub type_field: Option<String>,
    pub commit: Option<Commit>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Commit {
    pub rev: String,
    pub operation: String,
    pub collection: String,
    pub rkey: String,
    pub record: Option<Record>,
    pub cid: Option<String>,
}

pub trait CommitTypeable {
    fn get_type(&self) -> ATEventType;
}

impl CommitTypeable for Commit {
    fn get_type(&self) -> ATEventType {
        match self.collection.as_str() {
            "app.bsky.feed.post" => ATEventType::Post,
            "app.bsky.feed.repost" => ATEventType::Repost,
            "app.bsky.feed.like" => ATEventType::Like,
            "app.bsky.graph.follow" => ATEventType::Follow,
            "app.bsky.graph.block" => ATEventType::Block,
            _ => ATEventType::Unknown,
        }
    }
}

impl CommitTypeable for Option<Commit> {
    fn get_type(&self) -> ATEventType {
        match self {
            Some(c) => c.get_type(),
            None => ATEventType::Unknown,
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Record {
    #[serde(rename = "$type")]
    pub type_field: Option<String>,
    pub created_at: String,
    pub subject: Option<Subj>,
    pub lang: Option<String>,
    pub langs: Option<Vec<String>>,
    pub facets: Option<Vec<Facet>>,
    pub text: Option<String>,
    pub reply: Option<Reply>,
    pub embed: Option<Embed>,
    pub images: Option<Vec<Image>>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Facet {
    pub index: Option<Index>,
    pub features: Option<Vec<Feature>>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Index {
    pub byte_start: u64,
    pub byte_end: u64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Feature {
    #[serde(rename = "$type")]
    pub type_field: Option<String>,
    pub uri: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Image {
    pub alt: Option<String>,
    pub aspect_ratio: Option<HashMap<i64, i64>>,
    pub image: Option<Img>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImageInternal {
    #[serde(rename = "$type")]
    pub type_field: Option<String>,
    #[serde(rename = "ref")]
    pub reff: Option<Ref>,
    pub mime_type: String,
    pub size: u64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ref {
    #[serde(rename = "$link")]
    pub link: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Embed {
    #[serde(rename = "$type")]
    pub type_field: Option<String>,
    pub uri: Option<String>,
    pub embedded: Option<Embd>,
    pub external: Option<External>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct External {
    pub title: Option<String>,
    pub uri: Option<String>,
    pub description: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reply {
    pub parent: Parent,
    pub root: Parent,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Parent {
    pub cid: String,
    pub uri: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subject {
    pub cid: Option<String>,
    pub uri: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Subj {
    T1(String),
    T2(Subject),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Img {
    T1(String),
    T2(ImageInternal),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Embd {
    T1(String),
    T2(Subject),
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

#[derive(Hash, Eq, PartialEq, Clone)]
pub enum ATEventType {
    Post,
    Repost,
    Follow,
    Like,
    Block,
    Reply,
    Global,
    Unknown,
}
