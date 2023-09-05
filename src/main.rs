use std::{future::Future, panic::panic_any, path::PathBuf, time::Duration};

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
async fn main() {
    let cli = Cli::parse();

    if let Some(expect_number) = cli.plaza_service {
        common::setup_tracing("entropy.plaza");

        let (run, configure) = plaza::State::spawn::<Participant>(
            expect_number,
            Shared {
                fragment_size: 64,
                inner_k: 2,
                inner_n: 3,
                outer_k: 2,
                outer_n: 3,
                chunk_root: "/local/cowsay/_entropy_chunk".into(),
            },
        );
        let state_handle = spawn(run);
        HttpServer::new(move || {
            App::new()
                .wrap(actix_web_opentelemetry::RequestTracing::new())
                .configure(configure.clone())
        })
        .bind((cli.host, 8080))
        .unwrap()
        .run()
        .await
        .unwrap();

        state_handle.await.unwrap(); // inspect returned state if necessary
        common::shutdown_tracing().await;
        return;
    }

    common::setup_tracing("entropy.peer");

    let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
    let signing_key = SigningKey::generate(&mut OsRng);
    let verifying_key = signing_key.verifying_key();
    let peer = Peer {
        uri: format!(
            "http://{}:{}",
            cli.host,
            listener.local_addr().unwrap().port()
        ),
        id: Sha256::digest(verifying_key).into(),
        key: verifying_key,
    };

    let span = opentelemetry::global::tracer("").start("join network");
    let run = join_network(&peer, &cli)
        .with_context(Context::current_with_span(span))
        .await;

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
    tokio::fs::create_dir_all(&chunk_path).await.unwrap();
    let chunk_store = chunk::Store::new(
        chunk_path.clone(),
        run.shared.fragment_size,
        run.shared.inner_k,
    );

    let (run_app, configuration) = app::State::spawn(
        peer,
        signing_key,
        run.shared.fragment_size,
        run.shared.inner_n,
        run.shared.inner_k,
        run.shared.outer_n,
        run.shared.outer_k,
        peer_store,
        chunk_store,
    );
    let app_handle = spawn_local(run_app);

    let server = HttpServer::new(move || {
        App::new()
            .wrap(actix_web_opentelemetry::RequestTracing::new())
            .configure(configuration.clone())
    })
    .listen(listener.into_std().unwrap())
    .unwrap()
    .run();
    let server_handle = server.handle();
    spawn(async move {
        shutdown.1.recv().await;
        server_handle.stop(true).await;
    });

    println!("READY");
    server.await.unwrap();

    app_handle.await.unwrap();
    tokio::fs::remove_dir_all(&chunk_path).await.unwrap();
    leave_network(&cli, run.join_id).await;
    common::shutdown_tracing().await;
}

// #[instrument(skip_all, fields(peer = common::hex_string(&peer.id)))]
async fn join_network(peer: &Peer, cli: &Cli) -> ReadyRun {
    let client = Client::new();
    let mut response = client
        .post(format!("{}/join", cli.plaza.as_ref().unwrap()))
        .trace_request()
        .send_json(&Participant::Peer(peer.clone()))
        .await
        .unwrap();
    if response.status() != StatusCode::OK {
        panic_any(String::from_utf8(response.body().await.unwrap().to_vec()).unwrap());
    }
    let join_id = response.json::<serde_json::Value>().await.unwrap()["id"]
        .to_string()
        .parse()
        .unwrap();

    let mut retry_interval = Duration::ZERO;
    loop {
        sleep(retry_interval).await;
        match client
            .get(format!("{}/run", cli.plaza.as_ref().unwrap()))
            .trace_request()
            .send()
            .await
            .unwrap()
            .json::<plaza::Run<Participant, Shared>>()
            .await
            .unwrap()
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
    }
    // println!("{response:?}");
    // run
}

async fn leave_network(cli: &Cli, join_id: u32) {
    let client = Client::new();
    let response = client
        .post(format!("{}/leave/{join_id}", cli.plaza.as_ref().unwrap()))
        .trace_request()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

fn poll_network(cli: &Cli, shutdown: ShutdownServer) -> impl Future<Output = ()> {
    let endpoint = format!("{}/news", cli.plaza.as_ref().unwrap());
    async move {
        let client = Client::new();
        loop {
            sleep(Duration::from_secs(1)).await; //
            let news = client
                .get(&endpoint)
                .trace_request()
                .send()
                .await
                .unwrap()
                .json::<News>()
                .await
                .unwrap();
            if news.shutdown {
                shutdown.send(()).unwrap();
                break;
            }
        }
    }
}
