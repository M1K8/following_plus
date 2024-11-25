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

use crate::common::FetchMessage;
use axum_server::tls_rustls::RustlsConfig;
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::sync::mpsc::Sender;
use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};
mod auth;
mod types;
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
    let config = RustlsConfig::from_pem_file(
        PathBuf::from(".").join("cert.pem"),
        PathBuf::from(".").join("key.pem"),
    )
    .await
    .unwrap();

    let state = StateStruct {
        send_chan: chan.clone(),
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
    State(_state): State<Arc<StateStruct>>,
) -> Json<types::Response> {
    let auth;
    println!("{:?}", params);
    let iss = match bearer {
        Some(s) => {
            auth = s.0 .0.token();
            auth::verify_jwt(auth, &"did:web:feed.m1k.sh".to_owned()).unwrap()
        }
        None => "".into(),
    };
    println!("user id {}", iss);
    // TODO - Send request to channel to (maybe) prefetch all follow{er}s
    // Then make the read call to the DB ourselves


    Json(types::Response {
        cursor: Some("123".to_owned()),
        feed: vec![types::Post {
            post: "at://did:plc:zxs7h3vusn4chpru4nvzw5sj/app.bsky.feed.post/3lbdbqqdxxc2w"
                .to_owned(),
        }],
    })
}
