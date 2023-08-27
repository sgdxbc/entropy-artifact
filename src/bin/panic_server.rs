use actix_web::{get, post, App, HttpResponse, HttpServer};

#[get("/status")]
async fn status() -> String {
    "ok".into()
}

#[post("/crash")]
async fn crash() -> HttpResponse {
    panic!("crashed")
}

#[actix_web::main]
async fn main() {
    HttpServer::new(move || {
        App::new()
            .wrap(actix_web_opentelemetry::RequestTracing::new())
            .service(status)
            .service(crash)
    })
    .workers(1)
    .bind(("127.0.0.1", 8080))
    .unwrap()
    .run()
    .await
    .unwrap();
}
