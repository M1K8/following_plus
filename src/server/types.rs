use axum::{Json, response::IntoResponse};
use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct Response {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    pub feed: Vec<Post>,
}

impl IntoResponse for Response {
    fn into_response(self) -> axum::response::Response {
        Json(self).into_response()
    }
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct Post {
    pub post: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KnownService {
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(rename = "serviceEndpoint")]
    pub service_endpoint: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WellKnown {
    #[serde(rename = "@context")]
    pub context: Vec<String>,
    #[serde(rename = "id")]
    pub id: String,
    #[serde(rename = "service")]
    pub service: Vec<KnownService>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Describe {
    pub did: String,
    pub feeds: Vec<Feed>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Feed {
    pub uri: String,
}
