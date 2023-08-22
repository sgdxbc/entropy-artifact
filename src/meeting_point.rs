use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicU32, Ordering::SeqCst},
        Mutex,
    },
    time::{Duration, SystemTime},
};

use actix_web::{
    get, post,
    web::{Data, Json, Path, ServiceConfig},
    HttpResponse,
};
use serde::Deserialize;
use serde_json::{json, Value};

struct StateInner {
    participants: Mutex<HashMap<u32, Value>>,
    participant_id: AtomicU32,
    activities: Mutex<BTreeMap<SystemTime, Activity>>,
    ready_number: usize,
    assemble_time: Mutex<Option<SystemTime>>,
    shared: Value,
}

enum Activity {
    Join(Value),
    Leave(Value),
}

#[derive(Clone)]
pub struct State(Data<StateInner>);

#[post("/join")]
// #[instrument(skip(data, participant))]
async fn join(data: Data<StateInner>, participant: Json<Value>) -> HttpResponse {
    let participant_id = data.participant_id.fetch_add(1, SeqCst);
    let mut participants = data.participants.lock().unwrap();
    assert!(participants.len() < data.ready_number);
    participants.insert(participant_id, participant.0.clone());
    data.activities
        .lock()
        .unwrap()
        .insert(SystemTime::now(), Activity::Join(participant.0));
    HttpResponse::Ok().json(json!({ "id": participant_id }))
}

#[post("/leave/{id}")]
async fn leave(data: Data<StateInner>, id: Path<u32>) -> HttpResponse {
    let participant = data
        .participants
        .lock()
        .unwrap()
        .remove(&id.into_inner())
        .unwrap();
    data.activities
        .lock()
        .unwrap()
        .insert(SystemTime::now(), Activity::Leave(participant));
    HttpResponse::Ok().finish()
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
    let response = if participants.len() < data.ready_number {
        json!({
            "ready": false,
            "retry_interval": Duration::from_millis(data.ready_number as u64 / 100 / 1000).max(Duration::from_secs(1))
        })
    } else {
        let assemble_time = *data
            .assemble_time
            .lock()
            .unwrap()
            .get_or_insert_with(SystemTime::now);
        json!({
            "ready": true,
            "participants": Vec::from_iter(participants.values()),
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
            participant_id: Default::default(),
            activities: Default::default(),
            ready_number: expect_number,
            assemble_time: Default::default(),
            shared,
        }))
    }

    pub fn config(&self, config: &mut ServiceConfig) {
        config
            .app_data(self.0.clone())
            .service(join)
            .service(leave)
            .service(run_status);
    }
}
