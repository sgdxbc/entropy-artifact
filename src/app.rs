use std::{
    collections::{HashMap, HashSet},
    future::Future,
    ops::Range,
    sync::Arc,
    time::SystemTime,
};

use actix_web::{
    get,
    http::StatusCode,
    post,
    web::{Bytes, Data, Json, Path, ServiceConfig},
    HttpResponse,
};
use actix_web_opentelemetry::ClientExt;
use awc::Client;
use ed25519_dalek::SigningKey;
use opentelemetry::{trace::FutureExt, Context};
use rand::{thread_rng, RngCore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    spawn,
    sync::{mpsc, oneshot},
    task::spawn_local,
};
use tracing::{info, instrument};
use wirehair::WirehairEncoder;

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
    Get,
    GetStatus(oneshot::Sender<()>),

    UploadInvite(ChunkKey, u32, Peer, oneshot::Sender<bool>),
    UploadQueryFragment(ChunkKey, ChunkMember, oneshot::Sender<Option<Vec<u8>>>),
    UploadComplete(ChunkKey, Vec<ChunkMember>),
    DownloadQueryFragment(ChunkKey, Peer, oneshot::Sender<Option<Vec<u8>>>),
    RepairInvite(ChunkKey, u32, Vec<ChunkMember>),
    RepairQueryFragment(ChunkKey, ChunkMember, oneshot::Sender<Option<Vec<u8>>>),
    Ping(ChunkKey, PingMessage),

    AcceptUploadFragment(ChunkKey, Bytes),
    AcceptFragment(ChunkKey, u32, Bytes),
    RecoverFinish(ChunkKey, Vec<u8>),
    UploadChunk(Vec<u8>),
    UploadFinish(ChunkKey),
    UploadExtraInvite(ChunkKey),
}

struct StateMessage {
    command: AppCommand,
    context: Context,
}

impl From<AppCommand> for StateMessage {
    fn from(value: AppCommand) -> Self {
        Self {
            command: value,
            context: Context::current(),
        }
    }
}

type AppState = mpsc::UnboundedSender<StateMessage>;

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
#[instrument(skip(data))]
async fn upload_invite(
    data: Data<AppState>,
    path: Path<(String, u32)>,
    message: Json<Peer>,
) -> HttpResponse {
    let result = oneshot::channel();
    data.send(AppCommand::UploadInvite(parse_key(&path.0), path.1, message.0, result.0).into())
        .unwrap();
    if result.1.await.unwrap() {
        HttpResponse::Ok().finish()
    } else {
        HttpResponse::new(StatusCode::IM_A_TEAPOT)
    }
}

#[post("/upload/query-fragment/{key}")]
#[instrument(skip(data))]
async fn upload_query_fragment(
    data: Data<AppState>,
    path: Path<String>,
    message: Json<ChunkMember>,
) -> HttpResponse {
    let result = oneshot::channel();
    data.send(AppCommand::UploadQueryFragment(parse_key(&path), message.0, result.0).into())
        .unwrap();
    if let Some(fragment) = result.1.await.unwrap() {
        HttpResponse::Ok().body(fragment)
    } else {
        HttpResponse::NotFound().finish()
    }
}

#[post("/upload/complete/{key}")]
#[instrument(skip(data, message))]
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
#[instrument(skip(data))]
async fn download_query_fragment(
    data: Data<AppState>,
    path: Path<String>,
    message: Json<Peer>,
) -> HttpResponse {
    let result = oneshot::channel();
    data.send(AppCommand::DownloadQueryFragment(parse_key(&path), message.0, result.0).into())
        .unwrap();
    if let Some(fragment) = result.1.await.unwrap() {
        HttpResponse::Ok().body(fragment)
    } else {
        HttpResponse::NotFound().finish()
    }
}

#[post("/ping/{key}")]
#[instrument(skip(data, message))]
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
#[instrument(skip(data, message))]
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
#[instrument(skip(data))]
async fn repair_query_fragment(
    data: Data<AppState>,
    path: Path<String>,
    message: Json<ChunkMember>,
) -> HttpResponse {
    let result = oneshot::channel();
    data.send(AppCommand::RepairQueryFragment(parse_key(&path), message.0, result.0).into())
        .unwrap();
    if let Some(fragment) = result.1.await.unwrap() {
        HttpResponse::Ok().body(fragment)
    } else {
        HttpResponse::NotFound().finish()
    }
}

#[post("/benchmark/put")]
#[instrument(skip(data))]
async fn benchmark_put(data: Data<AppState>) -> HttpResponse {
    data.send(AppCommand::Put.into()).unwrap();
    HttpResponse::Ok().finish()
}

#[get("/benchmark/put")]
#[instrument(skip(data))]
async fn benchmark_put_status(data: Data<AppState>) -> HttpResponse {
    let result = oneshot::channel();
    data.send(AppCommand::PutStatus(result.0).into()).unwrap();
    HttpResponse::Ok().json(result.1.await.unwrap())
}

#[derive(Debug)]
pub struct State {
    local_peer: Peer,
    local_secret: SigningKey,

    fragment_size: u32,
    inner_n: u32,
    inner_k: u32,
    outer_n: u32,
    outer_k: u32,

    peer_store: peer::Store,
    chunk_store: chunk::Store,

    chunk_states: HashMap<ChunkKey, ChunkState>,
    put_state: Option<PutState>,
    put_uploads: HashMap<ChunkKey, UploadChunkState>,
    get_downloads: HashMap<ChunkKey, UploadChunkState>,

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

#[derive(Debug, Clone)]
struct UploadChunkState {
    members: Vec<ChunkMember>,
    next_invite: u32,
}

#[derive(Debug, Clone, Serialize)]
struct PutState {
    key: [u8; 32],
    start: SystemTime,
    end: Option<SystemTime>,
    num_upload: usize,
}

impl State {
    pub fn spawn(
        local_peer: Peer,
        local_secret: SigningKey,
        fragment_size: u32,
        inner_n: u32,
        inner_k: u32,
        outer_n: u32,
        outer_k: u32,
        peer_store: peer::Store,
        chunk_store: chunk::Store,
    ) -> (
        impl Future<Output = Self>,
        impl FnOnce(&mut ServiceConfig) + Clone,
    ) {
        let messages = mpsc::unbounded_channel();
        let mut state = State {
            local_peer,
            local_secret,
            fragment_size,
            inner_n,
            inner_k,
            outer_n,
            outer_k,
            peer_store,
            chunk_store,
            chunk_states: Default::default(),
            put_state: None,
            put_uploads: Default::default(),
            get_downloads: Default::default(),
            messages: messages.0.downgrade(),
        };
        let run_state = async move {
            state.run(messages.1).await;
            state
        };
        (run_state, |config| Self::config(config, messages.0))
    }

    async fn run(&mut self, mut messages: mpsc::UnboundedReceiver<StateMessage>) {
        while let Some(message) = messages.recv().await {
            let _attach = message.context.attach();
            match message.command {
                AppCommand::Put => self.handle_put(),
                AppCommand::PutStatus(result) => self.handle_put_state(result),
                AppCommand::UploadInvite(key, index, message, result) => {
                    self.handle_upload_invite(&key, index, message, result)
                }
                AppCommand::UploadQueryFragment(key, message, result) => {
                    self.handle_upload_query_fragment(&key, message, result)
                }
                AppCommand::UploadComplete(key, message) => {
                    self.handle_upload_complete(&key, message)
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
                AppCommand::UploadChunk(chunk) => self.handle_upload_chunk(chunk),
                AppCommand::UploadFinish(key) => self.handle_upload_finish(&key),
                AppCommand::UploadExtraInvite(key) => self.handle_upload_extra_invite(&key),
                _ => todo!(),
            }
        }
    }

    fn config(config: &mut ServiceConfig, app_data: AppState) {
        config
            .app_data(Data::new(app_data))
            .service(upload_invite)
            .service(upload_query_fragment)
            .service(upload_complete)
            .service(download_query_fragment)
            .service(repair_invite)
            .service(repair_query_fragment)
            .service(ping)
            .service(benchmark_put)
            .service(benchmark_put_status);
    }

    #[instrument(skip(self))]
    fn handle_put(&mut self) {
        if let Some(put_state) = &self.put_state {
            if put_state.end.is_none() {
                tracing::warn!("previous PUT operation not end");
            }
        }

        let mut object = vec![0; (self.fragment_size * self.inner_k * self.outer_k) as _];
        thread_rng().fill_bytes(&mut object);
        self.put_state = Some(PutState {
            key: Sha256::digest(&object).into(),
            start: SystemTime::now(),
            end: None,
            num_upload: 0,
        });
        self.put_uploads.clear();
        let encoder = Arc::new(WirehairEncoder::new(
            object,
            self.fragment_size * self.inner_k,
        ));
        for outer_index in 0..self.outer_n {
            let encoder = encoder.clone();
            let fragment_size = self.fragment_size;
            let inner_k = self.inner_k;
            let messages = self.messages.clone();
            spawn(
                async move {
                    let mut chunk = vec![0; (fragment_size * inner_k) as _];
                    encoder.encode(outer_index, &mut chunk).unwrap();
                    if let Some(messages) = messages.upgrade() {
                        let _ = messages.send(AppCommand::UploadChunk(chunk).into());
                    }
                }
                .with_current_context(),
            );
        }
    }

    #[instrument(skip(self))]
    fn handle_upload_chunk(&mut self, chunk: Vec<u8>) {
        let key = self.chunk_store.upload_chunk(chunk);
        self.put_uploads.insert(
            key,
            UploadChunkState {
                members: Default::default(),
                next_invite: self.inner_n,
            },
        );
        for inner_index in 0..self.inner_n {
            self.upload_invite(&key, inner_index);
        }
    }

    fn upload_invite(&self, key: &ChunkKey, index: u32) {
        let peer = self.peer_store.closest_peers(&fragment_id(key, index), 1)[0];
        let hex_key = hex_string(key);
        let peer_uri = peer.uri.clone();
        let local_peer = self.local_peer.clone();
        let task = async move {
            let response = Client::new()
                .post(format!("{}/upload/invite/{hex_key}/{index}", peer_uri))
                .trace_request()
                .send_json(&local_peer)
                .await
                .ok()?;
            if response.status() == StatusCode::OK {
                Some(())
            } else {
                None
            }
        };
        let messages = self.messages.clone();
        let key = *key;
        spawn_local(
            async move {
                if task.with_current_context().await.is_none() {
                    if let Some(messages) = messages.upgrade() {
                        let _ = messages.send(AppCommand::UploadExtraInvite(key).into());
                    }
                }
            }
            .with_current_context(),
        );
    }

    #[instrument(skip(self))]
    fn handle_upload_extra_invite(&mut self, key: &ChunkKey) {
        let put_upload = self.put_uploads.get_mut(key).unwrap();
        let index = put_upload.next_invite;
        put_upload.next_invite += 1;
        self.upload_invite(key, index);
    }

    #[instrument(skip(self, result))]
    fn handle_put_state(&mut self, result: oneshot::Sender<PutState>) {
        let _ = result.send(self.put_state.clone().unwrap());
    }

    #[instrument(skip(self))]
    fn handle_upload_invite(
        &mut self,
        key: &ChunkKey,
        index: u32,
        message: Peer,
        result: oneshot::Sender<bool>,
    ) {
        let local_member = if let Some(chunk_state) = self.chunk_states.get(key) {
            if chunk_state.fragment_present {
                let _ = result.send(true);
                return;
            }
            chunk_state
                .members
                .iter()
                .find(|member| member.peer.id == self.local_peer.id)
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
        if local_member.index != index {
            info!(
                local_index = local_member.index,
                index, "reject duplicated invite"
            );
            let _ = result.send(false);
            return; //
        }

        let hex_key = hex_string(key);
        let messages = self.messages.clone();
        let key = *key;
        spawn_local(
            async move {
                let mut response = Client::new()
                    .post(format!("{}/upload/query-fragment/{hex_key}", message.uri))
                    .trace_request()
                    .send_json(&local_member)
                    .await
                    .ok()?;
                if response.status() == StatusCode::NOT_FOUND {
                    return None;
                }
                let fragment = response.body().await.ok()?;
                messages
                    .upgrade()?
                    .send(AppCommand::AcceptUploadFragment(key, fragment).into())
                    .ok()?;
                Some(())
            }
            .with_current_context(),
        );
        let _ = result.send(true);
    }

    #[instrument(skip(self, result))]
    fn handle_upload_query_fragment(
        &mut self,
        key: &ChunkKey,
        message: ChunkMember,
        result: oneshot::Sender<Option<Vec<u8>>>,
    ) {
        let Some(upload) = self.put_uploads.get_mut(key) else {
            let _ = result.send(None);
            return;
        };
        // TODO verify proof
        // TODO deduplicate
        let task = self.chunk_store.generate_fragment(key, message.index);
        spawn(async move {
            let _ = result.send(Some(task.await));
        });
        upload.members.push(message);
        if upload.members.len() == self.inner_n as usize {
            let upload = self.put_uploads.remove(key).unwrap();
            self.get_downloads.insert(*key, upload.clone());
            let mut tasks = Vec::new();
            let hex_key = hex_string(key);
            for member in upload.members.clone() {
                let hex_key = hex_key.clone();
                let members = upload.members.clone();
                tasks.push(spawn_local(
                    async move {
                        let _ = Client::new()
                            .post(format!("{}/upload/complete/{hex_key}", member.peer.uri))
                            .trace_request()
                            .send_json(&members)
                            .await;
                    }
                    .with_current_context(),
                ));
            }
            let messages = self.messages.clone();
            let key = *key;
            spawn(
                async move {
                    for task in tasks {
                        let _ = task.await;
                    }
                    if let Some(messages) = messages.upgrade() {
                        let _ = messages.send(AppCommand::UploadFinish(key).into());
                    }
                }
                .with_current_context(),
            );
        }
    }

    #[instrument(skip(self, fragment))]
    fn handle_accept_upload_fragment(&mut self, key: &ChunkKey, fragment: Bytes) {
        let chunk_state = self.chunk_states.get_mut(key).unwrap();
        chunk_state.fragment_present = true;
        spawn(
            self.chunk_store
                .put_fragment(key, chunk_state.local_index, fragment.to_vec())
                .with_current_context(),
        );
    }

    #[instrument(skip(self))]
    fn handle_upload_complete(&mut self, key: &ChunkKey, message: Vec<ChunkMember>) {
        let chunk_state = self.chunk_states.get_mut(key).unwrap();
        chunk_state.members = message;
        // TODO index
    }

    #[instrument(skip(self))]
    fn handle_upload_finish(&mut self, key: &ChunkKey) {
        let put_state = self.put_state.as_mut().unwrap();
        assert!(self.get_downloads.contains_key(key));
        self.chunk_store.finish_upload(key);
        put_state.num_upload += 1;
        if put_state.num_upload == self.outer_n as usize {
            put_state.end = Some(SystemTime::now());
        }
    }

    #[instrument(skip(self, message))]
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
                            "{}/repair/query-fragment/{hex_key}",
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
                .with_current_context(),
            );
        }
    }

    #[instrument(skip(self, result))]
    fn handle_repair_query_fragment(
        &mut self,
        key: &ChunkKey,
        message: ChunkMember,
        result: oneshot::Sender<Option<Vec<u8>>>,
    ) {
        // TODO verify proof
        let Some(chunk_state) = self.chunk_states.get(key) else {
            let _ = result.send(None);
            return;
        };
        assert!(chunk_state.fragment_present);
        let fragment = self.chunk_store.get_fragment(key, chunk_state.local_index);
        spawn(
            async move {
                let _ = result.send(Some(fragment.await));
            }
            .with_current_context(),
        );
    }

    #[instrument(skip(self, message))]
    fn handle_ping(&mut self, key: &ChunkKey, message: PingMessage) {
        //
    }

    #[instrument(skip(self, fragment))]
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
            .with_current_context(),
        );
    }

    #[instrument(skip(self, fragment))]
    fn handle_recover_finish(&mut self, key: &ChunkKey, fragment: Vec<u8>) {
        self.chunk_store.finish_recover(key);
        let chunk_state = self.chunk_states.get_mut(key).unwrap();
        chunk_state.fragment_present = true;
        spawn(
            self.chunk_store
                .put_fragment(key, chunk_state.local_index, fragment)
                .with_current_context(),
        );
    }
}
