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

use neo4rs::{ConfigBuilder, Graph};
use std::{collections::HashMap, env, net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, sync::mpsc::Sender};
use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};

use crate::common::FetchMessage;
mod auth;
mod types;
struct StateStruct {
    send_chan: Sender<FetchMessage>,
    inner: Graph,
}

pub async fn serve_slim() -> Result<(), Box<dyn std::error::Error>> {
    let cors = CorsLayer::new()
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_origin(Any);
    let router = Router::new()
        .route("/xrpc/app.bsky.feed.getFeedSkeleton", get(index))
        .route("/xrpc/app.bsky.feed.describeFeedGenerator", get(describe))
        .route("/.well-known/did.json", get(well_known))
        .layer(ServiceBuilder::new().layer(cors));

    let addr = SocketAddr::from(([0, 0, 0, 0], 8000));
    let tcp = TcpListener::bind(&addr).await.unwrap();

    axum::serve(tcp, router).await.unwrap();
    Ok(())
}

async fn index(
    Query(params): Query<HashMap<String, String>>,
    bearer: Option<TypedHeader<Authorization<Bearer>>>,
) -> Result<types::Response, ()> {
    let auth;
    println!("{:?}", params);
    match bearer {
        Some(s) => {
            auth = s.0 .0.token();
            auth::verify_jwt(auth, &"".to_owned()).unwrap();
        }
        None => {}
    }

    Ok(types::Response {
        cursor: None,
        feed: vec![types::Post {
            post: "at://did:plc:zxs7h3vusn4chpru4nvzw5sj/app.bsky.feed.post/3lbdbqqdxxc2w"
                .to_owned(),
        }],
    })
}

// This needs to be exposed on port 443 too
async fn well_known() -> Result<Json<types::WellKnown>, ()> {
    match env::var("FEEDGEN_SERVICE_DID") {
        Ok(service_did) => {
            let hostname = env::var("FEEDGEN_HOSTNAME").unwrap_or("".into());
            if !service_did.ends_with(hostname.as_str()) {
                println!("service_did does not end with hostname");
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
            println!("service_did not found");
            return Err(());
        }
    }
}

async fn describe() -> Result<Json<types::Describe>, ()> {
    let hostname = env::var("FEEDGEN_HOSTNAME").unwrap();
    let dezscribe = types::Describe {
        did: format!("did:web:{hostname}"),
        feeds: vec![types::Feed {
            uri: format!("at://did:web:{hostname}/app.bsky.feed.generator/m1k_test_feed"),
        }],
    };

    Ok(Json(dezscribe))
}
