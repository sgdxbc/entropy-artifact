use std::{
    collections::{HashMap, HashSet},
    ops::Range,
    time::SystemTime,
};

use actix_web::{
    get, post,
    web::{Bytes, Data, Json, Path, ServiceConfig},
    HttpResponse,
};
use actix_web_opentelemetry::ClientExt;
use awc::Client;
use ed25519_dalek::SigningKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    spawn,
    sync::{mpsc, oneshot},
    task::{spawn_local, JoinHandle},
};
use tracing::{info_span, Instrument, Span};

use crate::{
    chunk::{self, ChunkKey},
    common::hex_string,
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
    Put,
    PutStatus(oneshot::Sender<PutState>),
    Get(ChunkKey),
    GetStatus(oneshot::Sender<()>),

    UploadInvite(ChunkKey, u32, Peer),
    UploadQueryFragment(ChunkKey, ChunkMember, oneshot::Sender<Vec<u8>>),
    UploadComplete(ChunkKey, Vec<ChunkMember>),
    DownloadQueryFragment(ChunkKey, Peer, oneshot::Sender<Vec<u8>>),
    RepairInvite(ChunkKey, u32, Vec<ChunkMember>),
    RepairQueryFragment(ChunkKey, ChunkMember, oneshot::Sender<Vec<u8>>),
    Ping(ChunkKey, PingMessage),

    AcceptUploadFragment(ChunkKey, Bytes),
    AcceptFragment(ChunkKey, u32, Bytes),
    RecoverFinish(ChunkKey, Vec<u8>),
    UploadFinish(ChunkKey),
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
pub type ShutdownServer = mpsc::UnboundedSender<()>;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChunkMember {
    peer: Peer,
    index: u32,
    proof: (),
}

#[derive(Debug, Serialize, Deserialize)]
struct PingMessage {
    members: Vec<ChunkMember>,
    index: u32,
    time: SystemTime,
    signature: ed25519_dalek::Signature,
}

#[post("/upload/invite/{key}/{index}")]
async fn upload_invite(
    data: Data<AppState>,
    path: Path<(String, u32)>,
    message: Json<Peer>,
) -> HttpResponse {
    data.send(AppCommand::UploadInvite(parse_key(&path.0), path.1, message.0).into())
        .unwrap();
    HttpResponse::Ok().finish()
}

#[post("/upload/query-fragment/{key}")]
async fn upload_query_fragment(
    data: Data<AppState>,
    path: Path<String>,
    message: Json<ChunkMember>,
) -> HttpResponse {
    let result = oneshot::channel();
    data.send(AppCommand::UploadQueryFragment(parse_key(&path), message.0, result.0).into())
        .unwrap();
    HttpResponse::Ok().body(result.1.await.unwrap())
}

#[post("/upload/complete/{key}")]
async fn upload_complete(
    data: Data<AppState>,
    path: Path<String>,
    message: Json<Vec<ChunkMember>>,
) -> HttpResponse {
    data.send(AppCommand::UploadComplete(parse_key(&path), message.0).into())
        .unwrap();
    HttpResponse::Ok().finish()
}

#[get("/download/query-fragment/{key}")]
async fn download_query_fragment(
    data: Data<AppState>,
    path: Path<String>,
    message: Json<Peer>,
) -> HttpResponse {
    let result = oneshot::channel();
    data.send(AppCommand::DownloadQueryFragment(parse_key(&path), message.0, result.0).into())
        .unwrap();
    HttpResponse::Ok().body(result.1.await.unwrap())
}

#[post("/ping/{key}")]
async fn ping(
    data: Data<AppState>,
    path: Path<String>,
    message: Json<PingMessage>,
) -> HttpResponse {
    data.send(AppCommand::Ping(parse_key(&path), message.0).into())
        .unwrap();
    HttpResponse::Ok().finish()
}

#[post("/repair/invite/{key}/{index}")]
async fn repair_invite(
    data: Data<AppState>,
    path: Path<(String, u32)>,
    message: Json<Vec<ChunkMember>>,
) -> HttpResponse {
    data.send(AppCommand::RepairInvite(parse_key(&path.0), path.1, message.0).into())
        .unwrap();
    HttpResponse::Ok().finish()
}

#[post("/repair/query-fragment/{key}")]
async fn repair_query_fragment(
    data: Data<AppState>,
    path: Path<String>,
    message: Json<ChunkMember>,
) -> HttpResponse {
    let result = oneshot::channel();
    data.send(AppCommand::RepairQueryFragment(parse_key(&path), message.0, result.0).into())
        .unwrap();
    HttpResponse::Ok().body(result.1.await.unwrap())
}

#[post("/benchmark/put")]
async fn benchmark_put(data: Data<AppState>) -> HttpResponse {
    data.send(AppCommand::Put.into()).unwrap();
    HttpResponse::Ok().finish()
}

#[get("/benchmark/put")]
async fn benchmark_put_status(data: Data<AppState>) -> HttpResponse {
    let result = oneshot::channel();
    data.send(AppCommand::PutStatus(result.0).into()).unwrap();
    HttpResponse::Ok().json(result.1.await.unwrap())
}

#[derive(Debug)]
pub struct State {
    local_peer: Peer,
    local_secret: SigningKey,

    peer_store: peer::Store,
    chunk_store: chunk::Store,

    chunk_states: HashMap<ChunkKey, ChunkState>,
    put_state: Option<PutState>,
    put_uploads: HashMap<ChunkKey, UploadChunkState>,

    messages: mpsc::WeakUnboundedSender<StateMessage>,
}

#[derive(Debug)]
struct ChunkState {
    local_index: u32,
    members: Vec<ChunkMember>,
    pinged: HashSet<u32>,
    indexes: Range<u32>,
    fragment_present: bool,
}

#[derive(Debug)]
struct UploadChunkState {
    members: Vec<ChunkMember>,
}

#[derive(Debug, Clone, Serialize)]
struct PutState {
    key: [u8; 32],
    start: SystemTime,
    end: Option<SystemTime>,
}

impl State {
    pub fn spawn(
        local_peer: Peer,
        local_secret: SigningKey,
        peer_store: peer::Store,
        chunk_store: chunk::Store,
    ) -> (JoinHandle<Self>, impl FnOnce(&mut ServiceConfig) + Clone) {
        let messages = mpsc::unbounded_channel();
        let mut state = State {
            local_peer,
            local_secret,
            peer_store,
            chunk_store,
            chunk_states: Default::default(),
            put_state: None,
            put_uploads: Default::default(),
            messages: messages.0.downgrade(),
        };
        let handle = spawn(async move {
            state.run(messages.1).await;
            state
        });
        (handle, |config| Self::config(config, messages.0))
    }

    async fn run(&mut self, mut messages: mpsc::UnboundedReceiver<StateMessage>) {
        while let Some(message) = messages.recv().await {
            let _entered = info_span!(parent: &message.span, "execute command").entered();
            match message.command {
                AppCommand::UploadInvite(key, index, message) => {
                    self.handle_upload_invite(&key, index, message)
                }
                AppCommand::RepairInvite(key, index, message) => {
                    self.handle_repair_invite(&key, index, message)
                }
                AppCommand::RepairQueryFragment(key, message, result) => {
                    self.handle_repair_query_fragment(&key, message, result)
                }
                AppCommand::Ping(key, message) => self.handle_ping(&key, message),
                AppCommand::AcceptUploadFragment(key, fragment) => {
                    self.handle_accept_upload_fragment(&key, fragment)
                }
                AppCommand::AcceptFragment(key, index, fragment) => {
                    self.handle_accept_fragment(&key, index, fragment)
                }
                AppCommand::RecoverFinish(key, fragment) => {
                    self.handle_recover_finish(&key, fragment)
                }
                _ => todo!(),
            }
        }
    }

    fn config(config: &mut ServiceConfig, app_data: AppState) {
        config
            .app_data(Data::new(app_data))
            .service(upload_invite)
            .service(upload_query_fragment)
            .service(download_query_fragment)
            .service(repair_invite)
            .service(repair_query_fragment)
            .service(ping)
            .service(benchmark_put)
            .service(benchmark_put_status);
    }

    fn handle_put(&mut self) {
        //
    }

    fn handle_put_state(&mut self, result: oneshot::Sender<PutState>) {
        let _ = result.send(self.put_state.clone().unwrap());
    }

    fn handle_upload_invite(&mut self, key: &ChunkKey, index: u32, message: Peer) {
        let local_member = if let Some(chunk_state) = self.chunk_states.get(key) {
            if chunk_state.fragment_present {
                return;
            }
            chunk_state
                .members
                .iter()
                .find(|member| member.index == index)
                .unwrap()
                .clone()
        } else {
            let mut chunk_state = ChunkState {
                local_index: index,
                members: Default::default(),
                pinged: Default::default(),
                indexes: 0..1, // TODO
                fragment_present: false,
            };
            // TODO generate proof
            let local_member = ChunkMember {
                peer: self.local_peer.clone(),
                index,
                proof: (),
            };
            chunk_state.members.push(local_member.clone());
            self.chunk_states.insert(*key, chunk_state);
            local_member
        };

        let hex_key = hex_string(key);
        let messages = self.messages.clone();
        let key = *key;
        spawn_local(
            async move {
                let fragment = Client::new()
                    .post(format!(
                        "http://{}/repair/query-fragment/{hex_key}",
                        message.uri
                    ))
                    .trace_request()
                    .send_json(&local_member)
                    .await
                    .ok()?
                    .body()
                    .await
                    .ok()?;
                messages
                    .upgrade()?
                    .send(AppCommand::AcceptUploadFragment(key, fragment).into())
                    .ok()?;
                Some(())
            }
            .instrument(Span::current()),
        );
    }

    fn handle_upload_query_fragment(&mut self, key: &ChunkKey, message: ChunkMember) {
        //
    }

    fn handle_repair_invite(&mut self, key: &ChunkKey, index: u32, message: Vec<ChunkMember>) {
        let local_member = if let Some(chunk_state) = self.chunk_states.get(key) {
            if chunk_state.fragment_present {
                return;
            }
            chunk_state
                .members
                .iter()
                .find(|member| member.index == index)
                .unwrap()
                .clone()
        } else {
            let mut chunk_state = ChunkState {
                local_index: index,
                members: message.clone(),
                pinged: Default::default(),
                indexes: 0..1, // TODO
                fragment_present: false,
            };
            // TODO generate proof
            let local_member = ChunkMember {
                peer: self.local_peer.clone(),
                index,
                proof: (),
            };
            chunk_state.members.push(local_member.clone());
            self.chunk_states.insert(*key, chunk_state);
            local_member
        };

        self.chunk_store.recover_chunk(key);
        let hex_key = hex_string(key);
        for member in message {
            // TODO skip query for already-have fragments
            let local_member = local_member.clone();
            let hex_key = hex_key.clone();
            let messages = self.messages.clone();
            let key = *key;
            spawn_local(
                async move {
                    let fragment = Client::new()
                        .post(format!(
                            "http://{}/repair/query-fragment/{hex_key}",
                            member.peer.uri
                        ))
                        .trace_request()
                        .send_json(&local_member)
                        .await
                        .ok()?
                        .body()
                        .await
                        .ok()?;
                    messages
                        .upgrade()?
                        .send(AppCommand::AcceptFragment(key, member.index, fragment).into())
                        .ok()?;
                    Some(())
                }
                .instrument(Span::current()),
            );
        }
    }

    fn handle_repair_query_fragment(
        &mut self,
        key: &ChunkKey,
        message: ChunkMember,
        result: oneshot::Sender<Vec<u8>>,
    ) {
        // TODO verify proof
        let Some(chunk_state) = self.chunk_states.get(key) else {
            //
            return;
        };
        assert!(chunk_state.fragment_present);
        let fragment = self.chunk_store.get_fragment(key, chunk_state.local_index);
        spawn(
            async move {
                let _ = result.send(fragment.await);
            }
            .instrument(Span::current()),
        );
    }

    fn handle_accept_upload_fragment(&mut self, key: &ChunkKey, fragment: Bytes) {
        let chunk_state = self.chunk_states.get_mut(key).unwrap();
        chunk_state.fragment_present = true;
        spawn(
            self.chunk_store
                .put_fragment(key, chunk_state.local_index, fragment.to_vec())
                .instrument(Span::current()),
        );
    }

    fn handle_accept_fragment(&mut self, key: &ChunkKey, index: u32, fragment: Bytes) {
        let chunk_state = &self.chunk_states[key];
        if chunk_state.fragment_present {
            //
            return;
        }
        assert_ne!(index, chunk_state.local_index);
        let task = self.chunk_store.accept_fragment(
            key,
            index,
            fragment.to_vec(),
            chunk_state.local_index,
        );
        let messages = self.messages.clone();
        let key = *key;
        spawn(
            async move {
                let fragment = task.await?;
                messages
                    .upgrade()?
                    .send(AppCommand::RecoverFinish(key, fragment).into())
                    .ok()?;
                Some(())
            }
            .instrument(Span::current()),
        );
    }

    fn handle_recover_finish(&mut self, key: &ChunkKey, fragment: Vec<u8>) {
        self.chunk_store.finish_recover(key);
        let chunk_state = self.chunk_states.get_mut(key).unwrap();
        chunk_state.fragment_present = true;
        spawn(
            self.chunk_store
                .put_fragment(key, chunk_state.local_index, fragment)
                .instrument(Span::current()),
        );
    }

    fn handle_ping(&mut self, key: &ChunkKey, message: PingMessage) {}
}
