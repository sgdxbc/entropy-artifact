use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, SystemTime},
};

use actix_web::{
    get, post,
    web::{Data, Json, Path, ServiceConfig},
    HttpResponse,
};

use serde::{Deserialize, Serialize};
use serde_json::{json, to_value, Value};
use tokio::{
    spawn,
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{info_span, Span};

pub struct State<S> {
    participants: HashMap<u32, Value>,
    participant_id: u32,
    activities: BTreeMap<SystemTime, Activity>,
    ready_number: usize,
    assemble_time: Option<SystemTime>,
    shutdown: bool,
    shared: S,
}

enum Activity {
    Join(Value),
    Leave(Value),
}

type AppState = mpsc::UnboundedSender<StateMessage>;

enum AppCommand {
    Join(Value, oneshot::Sender<u32>),
    Leave(u32),
    Shutdown,
    // TODO implement this with `tokio::sync::watch`
    RunStatus(oneshot::Sender<Value>),
    News(oneshot::Sender<News>),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct News {
    pub shutdown: bool,
    // TODO activities
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

#[post("/join")]
#[tracing::instrument(skip_all)]
async fn join(data: Data<AppState>, participant: Json<Value>) -> HttpResponse {
    let participant_id = oneshot::channel();
    data.send(AppCommand::Join(participant.0, participant_id.0).into())
        .unwrap();
    HttpResponse::Ok().json(json!({ "id": participant_id.1.await.unwrap() }))
}

#[post("/leave/{id}")]
#[tracing::instrument(skip(data))]
async fn leave(data: Data<AppState>, id: Path<u32>) -> HttpResponse {
    data.send(AppCommand::Leave(id.into_inner()).into())
        .unwrap();
    HttpResponse::Ok().finish()
}

#[post("/shutdown")]
#[tracing::instrument(skip(data))]
async fn shutdown(data: Data<AppState>) -> HttpResponse {
    data.send(AppCommand::Shutdown.into()).unwrap();
    HttpResponse::Ok().finish()
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
#[tracing::instrument(skip(data))]
async fn run_spec(data: Data<AppState>) -> HttpResponse {
    let run = oneshot::channel();
    data.send(AppCommand::RunStatus(run.0).into()).unwrap();
    HttpResponse::Ok().json(run.1.await.unwrap())
}

#[get("/news")]
#[tracing::instrument(skip(data))]
async fn interval_news(data: Data<AppState>) -> HttpResponse {
    let news = oneshot::channel();
    data.send(AppCommand::News(news.0).into()).unwrap();
    HttpResponse::Ok().json(news.1.await.unwrap())
}

impl<S> State<S> {
    fn new(expect_number: usize, shared: S) -> Self {
        Self {
            participants: Default::default(),
            participant_id: Default::default(),
            activities: Default::default(),
            ready_number: expect_number,
            assemble_time: Default::default(),
            shutdown: false,
            shared,
        }
    }

    pub fn spawn<P>(
        expect_number: usize,
        shared: S,
    ) -> (JoinHandle<Self>, impl FnOnce(&mut ServiceConfig) + Clone)
    where
        S: Send + Serialize + Clone + 'static,
        P: Serialize,
    {
        let mut state = Self::new(expect_number, shared);
        let messages = mpsc::unbounded_channel();
        let handle = spawn(async move {
            state.run::<P>(messages.1).await;
            state
        });
        (handle, |config| Self::config(config, messages.0))
    }

    fn config(config: &mut ServiceConfig, app_data: AppState) {
        config
            .app_data(Data::new(app_data))
            .service(join)
            .service(leave)
            .service(shutdown)
            .service(run_spec)
            .service(interval_news);
    }

    async fn run<P>(&mut self, mut messages: mpsc::UnboundedReceiver<StateMessage>)
    where
        S: Serialize + Clone,
        P: Serialize,
    {
        while let Some(message) = messages.recv().await {
            // tokio::time::sleep(Duration::from_millis(100)).await;
            let _entered = info_span!(parent: &message.span, "execute command").entered();
            let closed = match message.command {
                AppCommand::Join(participant, result) => {
                    self.participant_id += 1;
                    assert!(self.participants.len() < self.ready_number);
                    self.participants
                        .insert(self.participant_id, participant.clone());
                    self.activities
                        .insert(SystemTime::now(), Activity::Join(participant));
                    result.send(self.participant_id).is_err()
                }
                AppCommand::Leave(participant_id) => {
                    let participant = self.participants.remove(&participant_id).unwrap();
                    self.activities
                        .insert(SystemTime::now(), Activity::Leave(participant));
                    false
                }
                AppCommand::Shutdown => {
                    self.shutdown = true;
                    false
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
                    result.send(response).is_err()
                }
                AppCommand::News(result) => {
                    let news = News {
                        shutdown: self.shutdown,
                    };
                    result.send(news).is_err()
                }
            };
            if closed {
                break;
            }
        }
    }
}
