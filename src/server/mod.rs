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
use hyper::StatusCode;

use crate::common::FetchMessage;
use axum_server::tls_rustls::RustlsConfig;
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::sync::mpsc::Sender;
use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};
mod auth;
pub mod types;
struct StateStruct {
    send_chan: Sender<FetchMessage>,
    //cl: reqwest::Client,
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
    let config = RustlsConfig::from_pem_file(
        PathBuf::from(".").join("cert.pem"),
        PathBuf::from(".").join("key.pem"),
    )
    .await
    .unwrap();

    let state = StateStruct {
        send_chan: chan.clone(),
        //cl: reqwest::Client::new(),
    };

    let router = Router::new()
        .route("/get_feed", get(index))
        .layer(ServiceBuilder::new().layer(cors))
        .with_state(Arc::new(state));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    axum_server::bind_rustls(addr, config)
        .serve(router.into_make_service())
        .await
        .unwrap();
    Ok(())
}

async fn index(
    Query(params): Query<HashMap<String, String>>,
    bearer: Option<TypedHeader<Authorization<Bearer>>>,
    State(state): State<Arc<StateStruct>>,
) -> Result<Json<types::Response>, axum::http::StatusCode> {
    println!("{:?}", params);
    let iss = match bearer {
        Some(s) => auth::verify_jwt(s.0 .0.token(), &"did:web:feed.m1k.sh".to_owned()).unwrap(),
        None => "".into(),
    };
    println!("user id {}", iss);
    //

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
        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
            return Err(StatusCode::REQUEST_TIMEOUT)
        }
        r = recv.recv() => {
            resp = r;
        }
    }

    let resp = match resp {
        Some(r) => r,
        None => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    let posts: Vec<types::Post> = resp.posts.iter().map(|p| p.into()).collect();

    Ok(Json(types::Response {
        cursor: resp.cursor,
        feed: posts,
    }))
}
