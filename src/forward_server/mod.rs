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
use std::{collections::HashMap, env, net::SocketAddr, path::PathBuf};
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

    let _state = StateStruct {
        send_chan: chan.clone(),
    };
    let router = Router::new()
        .route("/", get(base))
        .route("/xrpc/app.bsky.feed.getFeedSkeleton", get(index))
        .route("/xrpc/app.bsky.feed.describeFeedGenerator", get(describe))
        .route("/.well-known/did.json", get(well_known))
        .layer(ServiceBuilder::new().layer(cors));

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

    // TODO - Call _the backend_
    // This'll also be spun out as a separate executable anyway (though we'll still)
    // need another web server to accept the DB calls - will use Go for that

    Json(types::Response {
        cursor: Some("123".to_owned()),
        feed: vec![types::Post {
            post: "at://did:plc:zxs7h3vusn4chpru4nvzw5sj/app.bsky.feed.post/3lbdbqqdxxc2w"
                .to_owned(),
        }],
    })
}

async fn base(
    Query(_): Query<HashMap<String, String>>,
    _: Option<TypedHeader<Authorization<Bearer>>>,
) -> Result<String, ()> {
    println!("base");
    Ok("Hello!".into())
}

// This needs to be exposed on port 443 too
async fn well_known() -> Result<Json<types::WellKnown>, ()> {
    println!("e");
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
    println!("a");
    let hostname = env::var("FEEDGEN_HOSTNAME").unwrap();
    let dezscribe = types::Describe {
        did: format!("did:web:{hostname}"),
        feeds: vec![types::Feed {
            uri: format!("at://did:web:{hostname}/app.bsky.feed.generator/feed.m1k.sh"),
        }],
    };

    Ok(Json(dezscribe))
}
