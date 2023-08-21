use std::time::{Duration, SystemTime};

use actix_web::{http::StatusCode, App, HttpServer};
use anyhow::anyhow;
use awc::Client;
use clap::Parser;
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::time::sleep;

mod meeting_point;

#[derive(Debug, Serialize, Deserialize)]
struct Participant {
    peer_id: [u8; 32],
    peer_key: ed25519_dalek::VerifyingKey,
    uri: String,
}

#[derive(clap::Parser)]
struct Cli {
    #[clap(long)]
    meeting_point: Option<usize>,
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if let Some(expect_number) = cli.meeting_point {
        let state = meeting_point::State::new(expect_number, serde_json::to_value(()).unwrap());
        HttpServer::new(move || App::new().configure(|c| state.config(c)))
            .bind(("127.0.0.1", 8080))?
            .run()
            .await?;
        return Ok(());
    }

    let signing_key = SigningKey::generate(&mut OsRng);
    let verifying_key = signing_key.verifying_key();
    let participant = Participant {
        uri: String::new(),
        peer_id: Sha256::digest(&verifying_key).into(),
        peer_key: verifying_key,
    };

    let client = Client::new();
    let response = client
        .post("http://localhost:8080/join")
        .send_json(&participant)
        .await
        .map_err(|_| anyhow!("send request error"))?;
    assert_eq!(response.status(), StatusCode::OK);

    #[derive(Debug, Deserialize)]
    struct RunConfig {
        ready: bool,
        participants: Option<Vec<Participant>>,
        assemble_time: Option<SystemTime>,
        shared: Option<()>,
    }

    loop {
        sleep(Duration::from_secs(1)).await;
        let response = client
            .get("http://localhost:8080/run")
            .send()
            .await
            .map_err(|_| anyhow!("send request_error"))?
            .json::<RunConfig>()
            .await?;
        if response.ready {
            println!("{response:?}");
            break;
        }
    }

    Ok(())
}
