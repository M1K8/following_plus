use crate::common::FetchMessage;
use axum::{
    Json, Router,
    extract::{Query, State},
    http::Method,
    routing::get,
};
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};

use hyper::{HeaderMap, StatusCode};
use std::{collections::HashMap, env, net::SocketAddr, sync::Arc};
use tokio::sync::mpsc::Sender;
use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};
use tracing::error;
use urlencoding::decode;

pub mod auth;
pub mod types;
struct StateStruct {
    send_chan: Sender<FetchMessage>,
}

pub async fn serve(chan: Sender<FetchMessage>) -> Result<(), Box<dyn std::error::Error>> {
    let cors = CorsLayer::new()
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_origin(Any);
    let state = StateStruct {
        send_chan: chan.clone(),
    };
    let router = Router::new()
        .route("/chicken_jockey", get(poke))
        .route("/describe", get(describe))
        .route("/.well-known/did.json", get(well_known))
        .route("/get_feed", get(index))
        .layer(ServiceBuilder::new().layer(cors))
        .with_state(Arc::new(state));

    let addr = SocketAddr::from(([0, 0, 0, 0], 29064));
    axum_server::bind(addr)
        .serve(router.into_make_service())
        .await
        .unwrap();
    Ok(())
}

async fn poke(
    _headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<StateStruct>>,
) -> Result<Json<types::Response>, axum::http::StatusCode> {
    let cursor;
    if let Some(c) = params.get("cursor") {
        cursor = Some(c.clone());
    } else {
        cursor = None;
    }

    get_feed(
        "did:plc:bsaboe3fhue4d3yyz5pziaed".to_string(),
        cursor,
        state,
    )
    .await
}

async fn get_feed(
    did: String,
    cursor: Option<String>,
    state: Arc<StateStruct>,
) -> Result<Json<types::Response>, axum::http::StatusCode> {
    let (resp, mut recv) = tokio::sync::mpsc::channel(1);
    state
        .send_chan
        .send(FetchMessage { did, cursor, resp })
        .await
        .unwrap();

    let resp;
    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            error!("timed out waiting for response from graph worker");
            return Err(StatusCode::REQUEST_TIMEOUT)
        }
        r = recv.recv() => {
            resp = r;
        }
    }

    let resp = match resp {
        Some(r) => r,
        None => {
            error!("nil response from channel");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let posts: Vec<types::Post> = resp.posts.iter().map(|p| p.into()).collect();

    Ok(Json(types::Response {
        cursor: resp.cursor,
        feed: posts,
    }))
}

async fn index(
    _headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    bearer: Option<TypedHeader<Authorization<Bearer>>>,
    State(state): State<Arc<StateStruct>>,
) -> Result<Json<types::Response>, axum::http::StatusCode> {
    let did = match bearer {
        Some(s) => {
            let s = decode(s.0.0.token()).unwrap().into_owned();
            auth::verify_jwt(&s, &"did:web:feed.m1k.sh".to_owned()).unwrap()
        }
        None => {
            error!("No Header - cant do auth!");
            return Err(axum::http::StatusCode::NOT_FOUND);
        }
    };
    let cursor;
    if let Some(c) = params.get("cursor") {
        cursor = Some(c.clone());
    } else {
        cursor = None;
    }

    get_feed(did, cursor, state).await
}

// TODO - handle video feed
async fn well_known() -> Result<Json<types::WellKnown>, ()> {
    match env::var("FEEDGEN_SERVICE_DID") {
        Ok(service_did) => {
            let hostname = env::var("FEEDGEN_HOSTNAME").unwrap_or("".into());
            if !service_did.ends_with(hostname.as_str()) {
                error!("service_did does not end with hostname");
                return Err(());
            } else {
                let known_service = types::KnownService {
                    id: "#bsky_fg".to_owned(),
                    r#type: "BskyFeedGenerator".to_owned(),
                    service_endpoint: format!("https://{}", hostname),
                };
                let result = types::WellKnown {
                    context: vec!["https://www.w3.org/ns/did/v1".into()],
                    id: service_did,
                    service: vec![known_service],
                };
                return Ok(Json(result));
            }
        }
        Err(_) => {
            error!("service_did not found");
            return Err(());
        }
    }
}

// TODO - handle video feed
async fn describe() -> Result<Json<types::Describe>, ()> {
    let hostname = env::var("FEEDGEN_HOSTNAME").unwrap();
    let dezscribe = types::Describe {
        did: format!("did:web:{hostname}"),
        feeds: vec![
            types::Feed {
                uri: format!("at://did:web:{hostname}/app.bsky.feed.generator/following_plus"),
            },
            types::Feed {
                uri: format!("at://did:web:{hostname}/app.bsky.feed.generator/videos_plus"),
            },
        ],
    };

    Ok(Json(dezscribe))
}
