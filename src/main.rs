use std::{error::Error, future::Future, path::PathBuf, time::Duration};

use actix_web::{http::StatusCode, App, HttpServer};
use actix_web_opentelemetry::ClientExt;
use awc::Client;
use clap::Parser;
use ed25519_dalek::SigningKey;
use opentelemetry::{
    trace::{FutureExt, TraceContextExt, Tracer},
    Context,
};
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, spawn, sync::mpsc, task::spawn_local, time::sleep};

use crate::{
    app::ShutdownServer,
    common::LocalResult,
    peer::Peer,
    plaza::{News, Run},
};

mod app;
mod chunk;
mod common;
mod peer;
mod plaza;

#[derive(Debug, Serialize, Deserialize)]
enum Participant {
    Peer(Peer),
    Invalid,
}

#[derive(clap::Parser)]
struct Cli {
    host: String,
    #[clap(long)]
    plaza_service: Option<usize>,
    #[clap(long)]
    plaza: Option<String>,
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
    shared: Shared,
    join_id: u32,
}

#[actix_web::main]
async fn main() -> LocalResult<()> {
    let cli = Cli::parse();

    if let Some(expect_number) = cli.plaza_service {
        common::setup_tracing("entropy.plaza");

        let (state_handle, configure) = plaza::State::spawn::<Participant>(
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

        state_handle.await?; // inspect returned state if necessary
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
    let run = join_network(&peer, &cli)
        .with_context(Context::current_with_span(span))
        .await?;

    let mut shutdown = mpsc::unbounded_channel();
    spawn_local(poll_network(&cli, shutdown.0));

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

    let (app_handle, configuration) = app::State::spawn(peer, signing_key, peer_store, chunk_store);

    let server = HttpServer::new(move || {
        App::new()
            .wrap(actix_web_opentelemetry::RequestTracing::new())
            .configure(configuration.clone())
    })
    .listen(listener.into_std()?)?
    .run();
    let server_handle = server.handle();
    spawn(async move {
        shutdown.1.recv().await;
        server_handle.stop(true).await;
    });

    println!("READY");
    server.await?;

    app_handle.await?.map_err(|err| err as Box<dyn Error>)?;
    tokio::fs::remove_dir_all(&chunk_path).await?;
    leave_network(&cli, run.join_id).await?;
    common::shutdown_tracing().await;
    Ok(())
}

// #[instrument(skip_all, fields(peer = common::hex_string(&peer.id)))]
async fn join_network(peer: &Peer, cli: &Cli) -> LocalResult<ReadyRun> {
    let client = Client::new();
    let mut response = client
        .post(format!("http://{}:8080/join", cli.plaza.as_ref().unwrap()))
        .trace_request()
        .send_json(&Participant::Peer(peer.clone()))
        .await?;
    if response.status() != StatusCode::OK {
        return Err(String::from_utf8(response.body().await?.to_vec())?.into());
    }
    let join_id = response.json::<serde_json::Value>().await?["id"]
        .to_string()
        .parse()?;

    let mut retry_interval = Duration::ZERO;
    let run = loop {
        sleep(retry_interval).await;
        match client
            .get(format!("http://{}:8080/run", cli.plaza.as_ref().unwrap()))
            .trace_request()
            .send()
            .await?
            .json::<plaza::Run<Participant, Shared>>()
            .await?
        {
            Run::Retry(interval) => retry_interval = interval,
            Run::Ready {
                participants,
                assemble_time: _,
                shared,
            } => {
                break ReadyRun {
                    participants,
                    shared,
                    join_id,
                }
            }
        }
    };
    // println!("{response:?}");

    Ok(run)
}

async fn leave_network(cli: &Cli, join_id: u32) -> LocalResult<()> {
    let client = Client::new();
    let response = client
        .post(format!(
            "http://{}:8080/leave/{join_id}",
            cli.plaza.as_ref().unwrap()
        ))
        .trace_request()
        .send()
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    Ok(())
}

fn poll_network(cli: &Cli, shutdown: ShutdownServer) -> impl Future<Output = LocalResult<()>> {
    let endpoint = format!("http://{}:8080/news", cli.plaza.as_ref().unwrap());
    async move {
        let client = Client::new();
        loop {
            sleep(Duration::from_secs(1)).await; //
            let news = client
                .get(&endpoint)
                .trace_request()
                .send()
                .await?
                .json::<News>()
                .await?;
            if news.shutdown {
                shutdown.send(()).map_err(|_| "receiver dropped")?;
                break Ok(());
            }
        }
    }
}
