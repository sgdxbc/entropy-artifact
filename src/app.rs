use std::{collections::HashMap, ops::Range};

use actix_web::{
    post,
    web::{Bytes, Data, Json, Path, ServiceConfig},
    HttpResponse,
};
use actix_web_opentelemetry::ClientExt;
use awc::Client;
use ed25519_dalek::SigningKey;
use rand::seq::index;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    spawn,
    sync::mpsc,
    task::{spawn_local, JoinHandle},
};
use tracing::Span;

use crate::{
    chunk::{self, ChunkKey},
    common::{hex_string, HandlerResult},
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    members: Vec<ChunkMember>,
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
    fragment_present: bool,
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

    async fn run(
        &mut self,
        mut messages: mpsc::UnboundedReceiver<StateMessage>,
    ) -> anyhow::Result<()> {
        while let Some(message) = messages.recv().await {
            match message.command {
                AppCommand::Invite(key, index, message) => {
                    self.handle_invite(&key, index, message).await
                }
                AppCommand::QueryFragment(key, message) => {
                    self.handle_query_fragment(&key, message).await
                }
                AppCommand::AcceptFragment(key, index, fragment) => {
                    self.handle_accept_fragment(&key, index, fragment).await
                }
                _ => todo!(),
            }
        }
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

    async fn handle_invite(&mut self, key: &ChunkKey, index: u32, message: InviteMessage) {
        let chunk_state = self.chunk_states.entry(*key).or_insert(ChunkState {
            local_index: index,
            members: message.members.clone(),
            indexes: 0..1,
            fragment_present: false,
        });
        if chunk_state.fragment_present {
            return;
        }

        // TODO generate proof
        self.chunk_store.recover_chunk(key);
        let key = hex_string(key);
        for member in message.members {
            // TODO skip query for already-have fragments
            let local_peer = self.local_peer.clone();
            let key = key.clone();
            spawn_local(async move {
                let _ = Client::new()
                    .post(format!("http://{}/query-fragment/{key}", member.peer.uri))
                    .trace_request()
                    .send_json(&QueryFragmentMessage {
                        peer: local_peer,
                        index: Some(index),
                        proof: (),
                    })
                    .await;
            });
        }
    }

    async fn handle_query_fragment(&mut self, key: &ChunkKey, message: QueryFragmentMessage) {
        // TODO verify proof
        let Some(chunk_state) = self.chunk_states.get(key) else {
            //
            return;
        };
        assert!(chunk_state.fragment_present);
        let index = chunk_state.local_index;
        let fragment = self.chunk_store.get_fragment(key, index);
        let key = hex_string(key);
        spawn_local(async move {
            let Ok(fragment) = fragment.await else {
                //
                return;
            };
            let _ = Client::new()
                .post(format!(
                    "http://{}/fragment/{key}/{index}",
                    message.peer.uri
                ))
                .trace_request()
                .send_body(fragment)
                .await;
        });
    }

    async fn handle_accept_fragment(&mut self, key: &ChunkKey, index: u32, fragment: Bytes) {
        let Some(chunk_state) = self.chunk_states.get(key) else {
            //
            return;
        };
        if chunk_state.fragment_present {
            //
            return;
        }
        if index == chunk_state.local_index {
            let task = self.chunk_store.put_fragment(key, index, fragment.to_vec());
            spawn(async move {
                if task.await.is_err() {
                    //
                }
            });
        } else {
            let task = self.chunk_store.accept_fragment(
                key,
                index,
                fragment.to_vec(),
                chunk_state.local_index,
            );
            spawn(async move {
                let Ok(fragment) = task.await else {
                    //
                    return;
                };
                if let Some(fragment) = fragment {
                    //
                }
            });
        }
    }
}
