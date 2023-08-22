use std::time::Duration;

use actix_web::{http::StatusCode, App, HttpServer};
use actix_web_opentelemetry::ClientExt;
use anyhow::anyhow;
use awc::Client;
use clap::Parser;
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::time::sleep;

mod common;
mod meeting_point;

#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
enum Participant {
    Peer(Peer),
}

#[derive(Debug, Serialize, Deserialize)]
struct Peer {
    peer_id: [u8; 32],
    peer_key: ed25519_dalek::VerifyingKey,
    uri: String,
}

#[derive(clap::Parser)]
struct Cli {
    #[clap(long)]
    meeting_point: Option<usize>,
    #[clap(long, default_value_t = 1)]
    peer_num: usize,
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if let Some(expect_number) = cli.meeting_point {
        common::setup_tracing("entropy.meeting-point");
        let state = meeting_point::State::new(expect_number, serde_json::to_value(()).unwrap());
        HttpServer::new(move || {
            App::new()
                .wrap(actix_web_opentelemetry::RequestTracing::new())
                .configure(|c| state.config(c))
        })
        .bind(("127.0.0.1", 8080))?
        .run()
        .await?;
        common::shutdown_tracing().await;
        return Ok(());
    }

    common::setup_tracing("entropy.participant");
    let signing_key = SigningKey::generate(&mut OsRng);
    let verifying_key = signing_key.verifying_key();
    let participant = Participant::Peer(Peer {
        uri: String::new(),
        peer_id: Sha256::digest(&verifying_key).into(),
        peer_key: verifying_key,
    });

    let client = Client::new();
    let response = client
        .post("http://localhost:8080/join")
        .trace_request()
        .send_json(&participant)
        .await
        .map_err(|_| anyhow!("send request error"))?;
    assert_eq!(response.status(), StatusCode::OK);

    let mut retry_interval = Duration::ZERO;
    loop {
        sleep(retry_interval).await;
        let response = client
            .get("http://localhost:8080/run")
            .trace_request()
            .send()
            .await
            .map_err(|_| anyhow!("send request_error"))?
            .json::<meeting_point::Run<Participant, ()>>()
            .await?;
        if response.ready {
            println!("{response:?}");
            break;
        }
        retry_interval = response.retry_interval.unwrap();
    }

    common::shutdown_tracing().await;
    Ok(())
}
