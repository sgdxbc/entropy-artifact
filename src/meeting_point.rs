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
use serde::{Deserialize, Serialize};
use serde_json::{json, to_value, Value};
use tokio::{
    spawn,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::common::HandlerResult;

pub struct State<S> {
    participants: HashMap<u32, Value>,
    participant_id: u32,
    activities: BTreeMap<SystemTime, Activity>,
    ready_number: usize,
    assemble_time: Option<SystemTime>,
    shared: S,
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

#[derive(Debug, Serialize, Deserialize)]
pub enum Run<P, S> {
    Retry(Duration),
    Ready {
        participants: Vec<P>,
        assemble_time: SystemTime,
        shared: S,
    },
}

#[get("/run")]
async fn run_status(data: Data<AppState>) -> HandlerResult<HttpResponse> {
    let run = oneshot::channel();
    data.send(AppCommand::RunStatus(run.0))?;
    Ok(HttpResponse::Ok().json(run.1.await?))
}

impl<S> State<S> {
    fn new(expect_number: usize, shared: S) -> Self {
        Self {
            participants: Default::default(),
            participant_id: Default::default(),
            activities: Default::default(),
            ready_number: expect_number,
            assemble_time: Default::default(),
            shared,
        }
    }

    pub fn spawn<P>(
        expect_number: usize,
        shared: S,
    ) -> (
        JoinHandle<anyhow::Result<Self>>,
        impl FnOnce(&mut ServiceConfig) + Clone,
    )
    where
        S: Send + Serialize + Clone + 'static,
        P: Serialize,
    {
        let mut state = Self::new(expect_number, shared);
        let command = mpsc::unbounded_channel();
        let handle = spawn(async move {
            state.run::<P>(command.1).await?;
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

    async fn run<P>(
        &mut self,
        mut command: mpsc::UnboundedReceiver<AppCommand>,
    ) -> anyhow::Result<()>
    where
        S: Serialize + Clone,
        P: Serialize,
    {
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
                        to_value(Run::<P, S>::Retry(
                            Duration::from_millis(self.ready_number as u64 / 100 / 1000)
                                .max(Duration::from_secs(1)),
                        ))
                        .unwrap()
                    } else {
                        let assemble_time = *self.assemble_time.get_or_insert_with(SystemTime::now);
                        to_value(Run::Ready {
                            participants: Vec::from_iter(self.participants.values()),
                            assemble_time,
                            shared: self.shared.clone(),
                        })
                        .unwrap()
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
