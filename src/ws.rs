// Shamelessly yoinked from https://github.com/skyfeed-dev/indexer-rust/blob/main/src/websocket/conn.rs

use std::{future::Future, sync::Arc};

use fastwebsockets::{handshake, WebSocket};
use hyper::{
    header::{CONNECTION, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE},
    rt::Executor,
    upgrade::Upgraded,
    Request,
};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_rustls::{
    rustls::{
        pki_types::{pem::PemObject, CertificateDer, ServerName},
        ClientConfig, RootCertStore,
    },
    TlsConnector,
};

const DEFAULT_CERT: &str = "/etc/ssl/certs/ISRG_Root_X1.pem";

pub async fn connect(
    domain: &str,
    uri: String,
) -> Result<WebSocket<TokioIo<Upgraded>>, Box<dyn std::error::Error>> {
    // prepare tls store
    let mut tls_store = RootCertStore::empty();
    let tls_cert = CertificateDer::from_pem_file(DEFAULT_CERT)?;
    tls_store.add(tls_cert)?;

    // create tcp conn to server
    let addr = format!("{}:443", domain);
    let tcp_stream = TcpStream::connect(&addr).await?;

    // tls encrypt the tcp stream
    let tls_config = ClientConfig::builder()
        .with_root_certificates(tls_store)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(tls_config));
    let tls_domain = ServerName::try_from(String::from(domain))?;
    let tls_stream = connector.connect(tls_domain, tcp_stream).await?;

    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .header("Host", domain)
        .header(UPGRADE, "websocket")
        .header(CONNECTION, "upgrade")
        .header(SEC_WEBSOCKET_KEY, handshake::generate_key())
        .header(SEC_WEBSOCKET_VERSION, "13")
        .body(String::new())?;

    let (ws, _) = handshake::client(&TokioExecutor, req, tls_stream).await?;

    Ok(ws)
}

struct TokioExecutor;
impl<F> Executor<F> for TokioExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        tokio::spawn(fut);
    }
}
