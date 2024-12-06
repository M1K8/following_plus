use crate::common::FetchMessage;
use axum::{
    extract::{Query, State},
    http::Method,
    routing::get,
    Json, Router,
};
use axum_extra::{
    headers::{authorization::Bearer, Authorization},
    TypedHeader,
};

use hyper::{HeaderMap, StatusCode};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::mpsc::Sender;
use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};
use tracing::info;
use urlencoding::decode;
mod auth;
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

    // Todo - not working on box?
    let router = Router::new()
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

async fn index(
    _headers: HeaderMap,
    Query(_params): Query<HashMap<String, String>>,
    bearer: Option<TypedHeader<Authorization<Bearer>>>,
    State(state): State<Arc<StateStruct>>,
) -> Result<Json<types::Response>, axum::http::StatusCode> {
    let iss = match bearer {
        Some(s) => {
            let s = decode(s.0 .0.token()).unwrap().into_owned();
            auth::verify_jwt(&s, &"did:web:feed.m1k.sh".to_owned()).unwrap()
        }
        None => {
            info!("No Header!");
            return Err(axum::http::StatusCode::NOT_FOUND);
        }
    };
    info!("user id {}", iss);

    let (send, mut recv) = tokio::sync::mpsc::channel(1);
    state
        .send_chan
        .send(FetchMessage {
            did: iss,
            cursor: None, //TODO - From Params (and likely handled here anyway)
            resp: send,
        })
        .await
        .unwrap();

    let resp;
    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_secs(10)) => {
            return Err(StatusCode::REQUEST_TIMEOUT)
        }
        r = recv.recv() => {
            resp = r;
        }
    }

    let resp = match resp {
        Some(r) => r,
        None => {
            info!("nil response from channel");
            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let posts: Vec<types::Post> = resp.posts.iter().map(|p| p.into()).collect();

    Ok(Json(types::Response {
        cursor: resp.cursor,
        feed: posts,
    }))
}
