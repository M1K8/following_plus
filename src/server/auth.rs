use base64::{engine::general_purpose, Engine as _};
use serde_derive::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn verify_jwt(jwtstr: &str, service_did: &String) -> Option<String> {
    let parts = jwtstr.split(".").map(String::from).collect::<Vec<_>>();

    if parts.len() != 3 {
        return None;
    }

    let bytes = general_purpose::STANDARD_NO_PAD.decode(&parts[1]).unwrap();

    if let Ok(payload) = serde_json::from_slice::<JWT>(&bytes) {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        if since_the_epoch.as_millis() / 1000 > payload.exp {
            return None;
        }
        if service_did != &payload.aud {
            return None;
        }
        Some(payload.iss)
    } else {
        None
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JWT {
    pub iss: String,
    pub aud: String,
    pub exp: u128,
}
