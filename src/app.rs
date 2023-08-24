use std::{collections::HashMap, ops::Range};

use actix_web::{
    post,
    web::{Bytes, Data, Json, Path, ServiceConfig},
    HttpResponse,
};
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{spawn, sync::mpsc, task::JoinHandle};
use tracing::Span;

use crate::{
    chunk::{self, ChunkKey},
    common::HandlerResult,
    peer::{self, Peer},
};

fn fragment_id(key: &ChunkKey, index: u32) -> [u8; 32] {
    Sha256::new()
        .chain_update(key)
        .chain_update(index.to_le_bytes())
        .finalize()
        .into()
}

fn parse_key(s: &str) -> ChunkKey {
    let mut key = Vec::new();
    for i in (0..s.len()).step_by(2) {
        key.push(u8::from_str_radix(&s[i..i + 2], 16).unwrap())
    }
    key.try_into().unwrap()
}

enum AppCommand {
    Invite(ChunkKey, u32, InviteMessage),
    QueryFragment(ChunkKey, QueryFragmentMessage),
    AcceptFragment(ChunkKey, u32, Bytes),
    Ping(ChunkKey, u32, PingMessage),
}

struct StateMessage {
    command: AppCommand,
    span: Span,
}

impl From<AppCommand> for StateMessage {
    fn from(value: AppCommand) -> Self {
        Self {
            command: value,
            span: Span::current(),
        }
    }
}

type AppState = mpsc::UnboundedSender<StateMessage>;

#[derive(Debug, Serialize, Deserialize)]
struct InviteMessage {
    members: Vec<ChunkMember>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChunkMember {
    index: u32,
    peer: Peer,
    proof: (),
}

#[post("/invite/{key}/{index}")]
async fn invite(
    data: Data<AppState>,
    path: Path<(String, u32)>,
    message: Json<InviteMessage>,
) -> HandlerResult<HttpResponse> {
    data.send(AppCommand::Invite(parse_key(&path.0), path.1, message.0).into())?;
    Ok(HttpResponse::Ok().finish())
}

#[derive(Debug, Serialize, Deserialize)]
struct QueryFragmentMessage {
    peer: Peer,
    index: Option<u32>, // sender is egligible to store, or `None` for GET operation
    proof: (),
}

#[post("/query-fragment/{key}")]
async fn query_fragment(
    data: Data<AppState>,
    path: Path<String>,
    message: Json<QueryFragmentMessage>,
) -> HandlerResult<HttpResponse> {
    data.send(AppCommand::QueryFragment(parse_key(&path), message.0).into())?;
    Ok(HttpResponse::Ok().finish())
}

#[post("/fragment/{key}/{index}")]
async fn accept_fragment(
    data: Data<AppState>,
    path: Path<(String, u32)>,
    fragment: Bytes,
) -> HandlerResult<HttpResponse> {
    data.send(AppCommand::AcceptFragment(parse_key(&path.0), path.1, fragment).into())?;
    Ok(HttpResponse::Ok().finish())
}

#[derive(Debug, Serialize, Deserialize)]
struct PingMessage {
    peer: Peer,
    proof: (),
}

#[post("/ping/{key}/{index}")]
async fn ping(
    data: Data<AppState>,
    path: Path<(String, u32)>,
    message: Json<PingMessage>,
) -> HandlerResult<HttpResponse> {
    data.send(AppCommand::Ping(parse_key(&path.0), path.1, message.0).into())?;
    Ok(HttpResponse::Ok().finish())
}

pub struct State {
    local_peer: Peer,
    local_secret: SigningKey,
    peer_store: peer::Store,
    chunk_store: chunk::Store,
    chunk_states: HashMap<ChunkKey, ChunkState>,
}

struct ChunkState {
    local_index: u32,
    members: Vec<ChunkMember>,
    indexes: Range<u32>,
}

impl State {
    pub fn spawn(
        local_peer: Peer,
        local_secret: SigningKey,
        peer_store: peer::Store,
        chunk_store: chunk::Store,
    ) -> (
        JoinHandle<anyhow::Result<Self>>,
        impl FnOnce(&mut ServiceConfig) + Clone,
    ) {
        let mut state = State {
            local_peer,
            local_secret,
            peer_store,
            chunk_store,
            chunk_states: Default::default(),
        };
        let messages = mpsc::unbounded_channel();
        let handle = spawn(async move {
            state.run(messages.1).await?;
            Ok(state)
        });
        (handle, |config| Self::config(config, messages.0))
    }

    async fn run(&mut self, messages: mpsc::UnboundedReceiver<StateMessage>) -> anyhow::Result<()> {
        Ok(())
    }

    fn config(config: &mut ServiceConfig, app_data: AppState) {
        config
            .app_data(Data::new(app_data))
            .service(invite)
            .service(query_fragment)
            .service(accept_fragment)
            .service(ping);
    }
}
