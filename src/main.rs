use std::{
    path::PathBuf,
    time::{Duration, SystemTime},
};

use actix_web::{http::StatusCode, App, HttpServer};
use actix_web_opentelemetry::ClientExt;
use anyhow::{anyhow, bail};
use awc::Client;
use clap::Parser;
use ed25519_dalek::SigningKey;
use meeting_point::Run;
use opentelemetry::trace::Tracer;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, spawn, time::sleep};

use crate::peer::Peer;

mod app;
mod chunk;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Shared {
    fragment_size: u32,
    inner_k: u32,
    inner_n: u32,
    outer_k: u32,
    outer_n: u32,
    chunk_root: PathBuf,
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

        let (state_handle, configure) = meeting_point::State::spawn::<Participant>(
            expect_number,
            Shared {
                fragment_size: 1024,
                inner_k: 32,
                inner_n: 80,
                outer_k: 8,
                outer_n: 10,
                chunk_root: "/local/cowsay/_entropy_chunk".into(),
            },
        );
        HttpServer::new(move || {
            App::new()
                .wrap(actix_web_opentelemetry::RequestTracing::new())
                .configure(configure.clone())
        })
        .bind((cli.host, 8080))?
        .run()
        .await?;

        state_handle.await??; // inspect returned state if necessary
        common::shutdown_tracing().await;
        return Ok(());
    }

    common::setup_tracing("entropy.peer");

    let listener = TcpListener::bind("0.0.0.0:0").await?;
    let signing_key = SigningKey::generate(&mut OsRng);
    let verifying_key = signing_key.verifying_key();
    let peer = Peer {
        uri: format!("http://{}:{}", cli.host, listener.local_addr()?.port()),
        id: Sha256::digest(verifying_key).into(),
        key: verifying_key,
    };

    let span = opentelemetry::global::tracer("").start("join network");
    let active = opentelemetry::trace::mark_span_as_active(span);
    let run = join_network(&peer, &cli).await?;
    drop(active);

    let peer_store = peer::Store::new(Vec::from_iter(run.participants.into_iter().filter_map(
        |participant| {
            if let Participant::Peer(peer) = participant {
                Some(peer)
            } else {
                None
            }
        },
    )));
    assert_eq!(peer_store.closest_peers(&peer.id, 1)[0].id, peer.id);

    let chunk_path = run.shared.chunk_root.join(common::hex_string(&peer.id));
    tokio::fs::create_dir_all(&chunk_path).await?;
    let chunk_store = chunk::Store::new(
        chunk_path.clone(),
        run.shared.fragment_size,
        run.shared.inner_k,
        run.shared.inner_n,
    );

    println!("READY");
    let assemble_time = run.assemble_time;
    let user_task = spawn(async move {
        let wait_duration =
            Duration::from_millis(100).saturating_sub(assemble_time.elapsed().unwrap_or_default());
        sleep(wait_duration).await;

        //
    });

    HttpServer::new(move || {
        App::new().wrap(actix_web_opentelemetry::RequestTracing::new())
        //
    })
    .listen(listener.into_std()?)?
    .run()
    .await?;

    user_task.await?;
    tokio::fs::remove_dir_all(&chunk_path).await?;
    leave_network(&cli, run.join_id).await?;
    common::shutdown_tracing().await;
    Ok(())
}

// #[instrument(skip_all, fields(peer = common::hex_string(&peer.id)))]
async fn join_network(peer: &Peer, cli: &Cli) -> anyhow::Result<ReadyRun> {
    let client = Client::new();
    let mut response = client
        .post(format!(
            "http://{}:8080/join",
            cli.meeting_point.as_ref().unwrap()
        ))
        .trace_request()
        .send_json(&Participant::Peer(peer.clone()))
        .await
        .map_err(|_| anyhow!("send request error"))?;
    if response.status() != StatusCode::OK {
        bail!(String::from_utf8(response.body().await?.to_vec())?)
    }
    let join_id = response.json::<serde_json::Value>().await?["id"]
        .to_string()
        .parse()?;

    let mut retry_interval = Duration::ZERO;
    let run = loop {
        sleep(retry_interval).await;
        match client
            .get(format!(
                "http://{}:8080/run",
                cli.meeting_point.as_ref().unwrap()
            ))
            .trace_request()
            .send()
            .await
            .map_err(|_| anyhow!("send request_error"))?
            .json::<meeting_point::Run<Participant, Shared>>()
            .await?
        {
            Run::Retry(interval) => retry_interval = interval,
            Run::Ready {
                participants,
                assemble_time,
                shared,
            } => {
                break ReadyRun {
                    participants,
                    assemble_time,
                    shared,
                    join_id,
                }
            }
        }
    };
    // println!("{response:?}");

    Ok(run)
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
