use std::{
    sync::Mutex,
    time::{Duration, SystemTime},
};

use actix_web::{
    get, post,
    web::{Data, Json, ServiceConfig},
    HttpResponse,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::instrument;

struct StateInner {
    participants: Mutex<Vec<(SystemTime, Value)>>,
    expect_number: usize,
    assemble_time: Mutex<Option<SystemTime>>,
    shared: Value,
}

#[derive(Clone)]
pub struct State(Data<StateInner>);

#[post("/join")]
#[instrument(skip(data, participant))]
async fn join(data: Data<StateInner>, participant: Json<Value>) -> HttpResponse {
    let mut participants = data.participants.lock().unwrap();
    let participant_id = participants.len();
    assert!(participant_id < data.expect_number);
    participants.push((SystemTime::now(), participant.0));
    HttpResponse::Ok().json(json!({ "id": participant_id }))
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
async fn run_status(data: Data<StateInner>) -> HttpResponse {
    let participants = data.participants.lock().unwrap();
    let response = if participants.len() < data.expect_number {
        json!({
            "ready": false,
            "retry_interval": Duration::from_millis(data.expect_number as u64 / 100 / 1000).max(Duration::from_secs(1))
        })
    } else {
        let assemble_time = *data
            .assemble_time
            .lock()
            .unwrap()
            .get_or_insert_with(SystemTime::now);
        json!({
            "ready": true,
            "participants": Vec::from_iter(participants.iter().map(|(_, value)| value.clone())),
            "assemble_time": assemble_time,
            "shared": data.shared
        })
    };
    HttpResponse::Ok().json(response)
}

impl State {
    pub fn new(expect_number: usize, shared: Value) -> Self {
        Self(Data::new(StateInner {
            participants: Default::default(),
            expect_number,
            assemble_time: Default::default(),
            shared,
        }))
    }

    pub fn config(&self, config: &mut ServiceConfig) {
        config
            .app_data(self.0.clone())
            .service(join)
            .service(run_status);
    }
}
