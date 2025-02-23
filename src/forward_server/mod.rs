use axum::{
    Json, Router,
    body::Body,
    extract::{Query, State},
    http::Method,
    response::Response,
    routing::get,
};
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};

use axum_server::tls_rustls::RustlsConfig;
use std::{collections::HashMap, env, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info};

use crate::server::types;

struct StateStruct {
    client: reqwest::Client,
    edpt: String,
}
pub async fn serve(edpt: String) -> Result<(), Box<dyn std::error::Error>> {
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

    let cl = reqwest::ClientBuilder::new()
        .connect_timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    let state = Arc::new(StateStruct { client: cl, edpt });

    let router = Router::new()
        .route("/", get(base))
        .route("/xrpc/app.bsky.feed.getFeedSkeleton", get(forward))
        .route("/xrpc/app.bsky.feed.describeFeedGenerator", get(describe))
        .route("/.well-known/did.json", get(well_known))
        .fallback(base)
        .layer(ServiceBuilder::new().layer(cors))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    axum_server::bind_rustls(addr, config)
        .serve(router.into_make_service())
        .await
        .unwrap();
    Ok(())
}

// TODO - handle video feed
async fn forward(
    Query(params): Query<HashMap<String, String>>,
    bearer: Option<TypedHeader<Authorization<Bearer>>>,
    State(state): axum::extract::State<Arc<StateStruct>>,
) -> Response<Body> {
    let iss;
    match &bearer {
        Some(s) => {
            iss = crate::server::auth::verify_jwt(s.0.0.token(), &"did:web:feed.m1k.sh".to_owned())
                .unwrap();
        }
        None => {
            return Response::builder()
                .status(401)
                .body(Body::from("unauthorized"))
                .unwrap();
        }
    };
    info!("Forwarding for user id {}", iss);
    let tok = bearer.unwrap();
    let tok = tok.0.0.token();

    let resp = match state
        .client
        .request(reqwest::Method::GET, state.edpt.as_str())
        .query(&params)
        .bearer_auth(tok)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("{:?}", e.to_string());
            return Response::builder()
                .status(403)
                .body(Body::from(e.to_string()))
                .unwrap();
        }
    };
    let mut axum_res = Response::builder();
    *axum_res.headers_mut().unwrap() = resp.headers().clone();

    let status = resp.status();
    let byt = resp.bytes().await.unwrap();
    info!("Got resp {:?}", byt);

    axum_res.status(status).body(Body::from(byt)).unwrap()
}

async fn base(
    Query(_): Query<HashMap<String, String>>,
    _: Option<TypedHeader<Authorization<Bearer>>>,
) -> Result<String, ()> {
    info!("Hey!");
    Ok("Hello!".into())
}

// TODO - handle video feed
async fn well_known() -> Result<Json<types::WellKnown>, ()> {
    match env::var("FEEDGEN_SERVICE_DID") {
        Ok(service_did) => {
            info!(service_did);
            let hostname = env::var("FEEDGEN_HOSTNAME").unwrap_or("".into());
            if !service_did.ends_with(hostname.as_str()) {
                error!("service_did does not end with hostname");
                return Err(());
            } else {
                info!(hostname);
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
