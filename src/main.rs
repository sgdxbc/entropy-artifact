use std::time::Duration;

use actix_web::{http::StatusCode, App, HttpServer};
use actix_web_opentelemetry::ClientExt;
use anyhow::anyhow;
use awc::Client;
use clap::Parser;
use ed25519_dalek::SigningKey;
use peer::Peer;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, task::spawn_local, time::sleep};

use crate::peer::Store;

mod common;
mod meeting_point;
mod peer;

#[derive(Debug, Serialize, Deserialize)]
enum Participant {
    Peer(Peer),
    Invalid,
}

#[derive(clap::Parser)]
struct Cli {
    host: String,
    #[clap(long)]
    serve_meeting_point: Option<usize>,
    #[clap(long)]
    meeting_point: Option<String>,
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if let Some(expect_number) = cli.serve_meeting_point {
        common::setup_tracing("entropy.meeting-point");

        let state = meeting_point::State::new(expect_number, serde_json::to_value(()).unwrap());
        HttpServer::new(move || {
            App::new()
                .wrap(actix_web_opentelemetry::RequestTracing::new())
                .configure(|c| state.config(c))
        })
        .bind((cli.host, 8080))?
        .run()
        .await?;

        common::shutdown_tracing().await;
        return Ok(());
    }

    common::setup_tracing("entropy.participant");

    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let signing_key = SigningKey::generate(&mut OsRng);
    let verifying_key = signing_key.verifying_key();

    let foreground = spawn_local(foreground_work(
        Peer {
            uri: format!("http://{}:{}", cli.host, listener.local_addr()?.port()),
            id: Sha256::digest(&verifying_key).into(),
            key: verifying_key,
        },
        cli,
    ));

    HttpServer::new(move || App::new().wrap(actix_web_opentelemetry::RequestTracing::new()))
        .listen(listener.into_std()?)?
        .run()
        .await?;
    foreground.abort();

    common::shutdown_tracing().await;
    Ok(())
}

async fn foreground_work(peer: Peer, cli: Cli) -> anyhow::Result<()> {
    let client = Client::new();
    let response = client
        .post(format!(
            "http://{}:8080/join",
            cli.meeting_point.as_ref().unwrap()
        ))
        .trace_request()
        .send_json(&Participant::Peer(peer.clone()))
        .await
        .map_err(|_| anyhow!("send request error"))?;
    assert_eq!(response.status(), StatusCode::OK);

    let mut retry_interval = Duration::ZERO;
    let response = loop {
        sleep(retry_interval).await;
        let response = client
            .get(format!(
                "http://{}:8080/run",
                cli.meeting_point.as_ref().unwrap()
            ))
            .trace_request()
            .send()
            .await
            .map_err(|_| anyhow!("send request_error"))?
            .json::<meeting_point::Run<Participant, ()>>()
            .await?;
        if response.ready {
            break response;
        }
        retry_interval = response.retry_interval.unwrap();
    };
    // println!("{response:?}");

    let store = Store::new(Vec::from_iter(
        response
            .participants
            .unwrap()
            .into_iter()
            .filter_map(|participant| {
                if let Participant::Peer(peer) = participant {
                    Some(peer)
                } else {
                    None
                }
            }),
    ));
    assert_eq!(store.closest_peers(&peer.id, 1)[0].id, peer.id);

    Ok(())
}
