use std::{sync::Mutex, time::SystemTime};

use actix_web::{
    get, post,
    web::{Data, Json, ServiceConfig},
    HttpResponse,
};
use serde_json::{json, Value};

struct StateInner {
    participants: Mutex<Vec<Value>>,
    expect_number: usize,
    assemble_time: Mutex<Option<SystemTime>>,
    shared: Value,
}

#[derive(Clone)]
pub struct State(Data<StateInner>);

#[post("/join")]
async fn join(data: Data<StateInner>, participant: Json<Value>) -> HttpResponse {
    let mut participants = data.participants.lock().unwrap();
    participants.push(participant.0);
    assert!(participants.len() <= data.expect_number);
    HttpResponse::Ok().finish()
}

#[get("/run")]
async fn run_status(data: Data<StateInner>) -> HttpResponse {
    let participants = data.participants.lock().unwrap();
    let response = if participants.len() < data.expect_number {
        json!({"ready": false})
    } else {
        let assemble_time = *data
            .assemble_time
            .lock()
            .unwrap()
            .get_or_insert_with(SystemTime::now);
        json!({
            "ready": true,
            "participants": &*participants,
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
