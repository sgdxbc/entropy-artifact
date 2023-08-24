use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, SystemTime},
};

use actix_web::{
    get, post,
    web::{Data, Json, Path, ServiceConfig},
    HttpResponse,
};
use anyhow::anyhow;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::{
    spawn,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::common::HandlerResult;

pub struct State {
    participants: HashMap<u32, Value>,
    participant_id: u32,
    activities: BTreeMap<SystemTime, Activity>,
    ready_number: usize,
    assemble_time: Option<SystemTime>,
    shared: Value,
}

enum Activity {
    Join(Value),
    Leave(Value),
}

type AppState = mpsc::UnboundedSender<AppCommand>;

enum AppCommand {
    Join(Value, oneshot::Sender<u32>),
    Leave(u32),
    // TODO implement this with `tokio::sync::watch`
    RunStatus(oneshot::Sender<Value>),
    // interval activities
}

#[post("/join")]
// #[instrument(skip(data, participant))]
async fn join(data: Data<AppState>, participant: Json<Value>) -> HandlerResult<HttpResponse> {
    let participant_id = oneshot::channel();
    data.send(AppCommand::Join(participant.0, participant_id.0))?;
    Ok(HttpResponse::Ok().json(json!({ "id": participant_id.1.await? })))
}

#[post("/leave/{id}")]
async fn leave(data: Data<AppState>, id: Path<u32>) -> HandlerResult<HttpResponse> {
    data.send(AppCommand::Leave(id.into_inner()))?;
    Ok(HttpResponse::Ok().finish())
}

#[derive(Debug, Deserialize)]
pub struct Run<P, S> {
    pub ready: bool,
    pub retry_interval: Option<Duration>,
    pub participants: Option<Vec<P>>,
    pub assemble_time: Option<SystemTime>,
    pub shared: Option<S>,
}

#[get("/run")]
async fn run_status(data: Data<AppState>) -> HandlerResult<HttpResponse> {
    let run = oneshot::channel();
    data.send(AppCommand::RunStatus(run.0))?;
    Ok(HttpResponse::Ok().json(run.1.await?))
}

impl State {
    fn new(expect_number: usize, shared: Value) -> Self {
        Self {
            participants: Default::default(),
            participant_id: Default::default(),
            activities: Default::default(),
            ready_number: expect_number,
            assemble_time: Default::default(),
            shared,
        }
    }

    pub fn spawn(
        expect_number: usize,
        shared: Value,
    ) -> (
        JoinHandle<anyhow::Result<Self>>,
        impl FnOnce(&mut ServiceConfig) + Clone,
    ) {
        let mut state = Self::new(expect_number, shared);
        let command = mpsc::unbounded_channel();
        let handle = spawn(async move {
            state.run(command.1).await?;
            Ok(state)
        });
        (handle, |config| Self::config(config, command.0))
    }

    fn config(config: &mut ServiceConfig, app_data: AppState) {
        config
            .app_data(Data::new(app_data))
            .service(join)
            .service(leave)
            .service(run_status);
    }

    async fn run(
        &mut self,
        mut command: mpsc::UnboundedReceiver<AppCommand>,
    ) -> anyhow::Result<()> {
        while let Some(command) = command.recv().await {
            match command {
                AppCommand::Join(participant, result) => {
                    self.participant_id += 1;
                    assert!(self.participants.len() < self.ready_number);
                    self.participants
                        .insert(self.participant_id, participant.clone());
                    self.activities
                        .insert(SystemTime::now(), Activity::Join(participant));
                    result
                        .send(self.participant_id)
                        .map_err(|_| anyhow!("reciver dropped"))?
                }
                AppCommand::Leave(participant_id) => {
                    let participant = self.participants.remove(&participant_id).unwrap();
                    self.activities
                        .insert(SystemTime::now(), Activity::Leave(participant));
                }
                AppCommand::RunStatus(result) => {
                    let response = if self.participants.len() < self.ready_number {
                        json!({
                            "ready": false,
                            "retry_interval": Duration::from_millis(self.ready_number as u64 / 100 / 1000).max(Duration::from_secs(1))
                        })
                    } else {
                        let assemble_time = *self.assemble_time.get_or_insert_with(SystemTime::now);
                        json!({
                            "ready": true,
                            "participants": Vec::from_iter(self.participants.values()),
                            "assemble_time": assemble_time,
                            "shared": self.shared
                        })
                    };
                    result
                        .send(response)
                        .map_err(|_| anyhow!("reciver dropped"))?
                }
            }
        }
        Ok(())
    }
}
