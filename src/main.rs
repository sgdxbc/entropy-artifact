use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use actix_web::{http::StatusCode, web::Data, App, HttpServer};
use actix_web_opentelemetry::ClientExt;
use anyhow::anyhow;
use awc::Client;
use clap::Parser;
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, time::sleep};

use crate::peer::Peer;
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

#[derive(Debug, Serialize, Deserialize)]
struct Shared {
    fragment_size: usize,
    inner_k: u32,
    inner_n: u32,
    outer_k: u32,
    outer_n: u32,
}

struct ReadyRun {
    participants: Vec<Participant>,
    assemble_time: SystemTime,
    shared: Shared,
    join_id: u32,
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if let Some(expect_number) = cli.serve_meeting_point {
        common::setup_tracing("entropy.meeting-point");

        let state = meeting_point::State::new(
            expect_number,
            serde_json::to_value(Shared {
                fragment_size: 1024,
                inner_k: 32,
                inner_n: 80,
                outer_k: 8,
                outer_n: 10,
            })
            .unwrap(),
        );
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

    common::setup_tracing("entropy.peer");

    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let signing_key = SigningKey::generate(&mut OsRng);
    let verifying_key = signing_key.verifying_key();
    let peer = Peer {
        uri: format!("http://{}:{}", cli.host, listener.local_addr()?.port()),
        id: Sha256::digest(&verifying_key).into(),
        key: verifying_key,
    };

    let run = join_network(&peer, &cli).await?;
    let store = Arc::new(Store::new(Vec::from_iter(
        run.participants.into_iter().filter_map(|participant| {
            if let Participant::Peer(peer) = participant {
                Some(peer)
            } else {
                None
            }
        }),
    )));
    assert_eq!(store.closest_peers(&peer.id, 1)[0].id, peer.id);

    println!("READY");
    HttpServer::new(move || {
        App::new()
            .wrap(actix_web_opentelemetry::RequestTracing::new())
            .app_data(Data::new(store.clone()))
    })
    .listen(listener.into_std()?)?
    .run()
    .await?;

    leave_network(&cli, run.join_id).await?;
    common::shutdown_tracing().await;
    Ok(())
}

async fn join_network(peer: &Peer, cli: &Cli) -> anyhow::Result<ReadyRun> {
    let client = Client::new();
    let join_id = client
        .post(format!(
            "http://{}:8080/join",
            cli.meeting_point.as_ref().unwrap()
        ))
        .trace_request()
        .send_json(&Participant::Peer(peer.clone()))
        .await
        .map_err(|_| anyhow!("send request error"))?
        .json::<serde_json::Value>()
        .await?["id"]
        .to_string()
        .parse()?;

    let mut retry_interval = Duration::ZERO;
    let run = loop {
        sleep(retry_interval).await;
        let run = client
            .get(format!(
                "http://{}:8080/run",
                cli.meeting_point.as_ref().unwrap()
            ))
            .trace_request()
            .send()
            .await
            .map_err(|_| anyhow!("send request_error"))?
            .json::<meeting_point::Run<Participant, Shared>>()
            .await?;
        if run.ready {
            break run;
        }
        retry_interval = run.retry_interval.unwrap();
    };
    // println!("{response:?}");

    Ok(ReadyRun {
        participants: run.participants.unwrap(),
        assemble_time: run.assemble_time.unwrap(),
        shared: run.shared.unwrap(),
        join_id,
    })
}

async fn leave_network(cli: &Cli, join_id: u32) -> anyhow::Result<()> {
    let client = Client::new();
    let response = client
        .post(format!(
            "http://{}:8080/leave/{join_id}",
            cli.meeting_point.as_ref().unwrap()
        ))
        .trace_request()
        .send()
        .await
        .map_err(|_| anyhow!("send request error"))?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}
