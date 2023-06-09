use crate::*;
use actix_web::{get, put, web, HttpResponse, Responder};
use std::net::SocketAddr;

#[get("/kv/{key}")]
async fn get(path: web::Path<String>, kv: web::Data<Store>) -> impl Responder {
    let key = path.into_inner();
    println!("GET {}", key);
    match kv.get(key.clone()) {
        Some(value) => HttpResponse::Ok().body(value),
        None => HttpResponse::NotFound().body("Not Found"),
    }
}

#[put("/kv/{key}")]
async fn put(
    path: web::Path<String>,
    kv: web::Data<Store>,
    wal: web::Data<Log>,
    async_replicas: web::Data<AsyncReplicas>,
    sync_replicas: web::Data<SyncReplicas>,
    bytes: web::Bytes,
) -> impl Responder {
    let key = path.into_inner();
    let value = match String::from_utf8(bytes.to_vec()) {
        Ok(value) => value,
        Err(_) => return HttpResponse::BadRequest().body("Invalid UTF-8"),
    };

    println!("PUT {} {}", key, value);

    println!("Writing to WAL");
    let op = wal.put(key.clone(), value.clone());
    println!("Writing to {} sync replicas", sync_replicas.len());
    if let Err(e) = sync_replicas.replicate(op).await {
        return HttpResponse::InternalServerError().body(e.to_string());
    }
    println!("Writing to {} async replicas", async_replicas.len());
    async_replicas.put(key.clone(), value.clone());

    kv.put(key, value);
    HttpResponse::Ok().body("OK")
}

#[put("/replica")]
async fn replica(
    async_replicas: web::Data<AsyncReplicas>,
    sync_replicas: web::Data<SyncReplicas>,
    replica: web::Json<Replica>,
) -> impl Responder {
    let addr = replica.addr.parse::<SocketAddr>();
    match addr {
        Ok(addr) => {
            if replica.asynchronous {
                async_replicas.add(addr);
                println!("Aynchronous replica added: {}", addr);
            } else {
                sync_replicas.add(addr);
                println!("Synchronous replica added: {}", addr);
            }
            HttpResponse::Ok().body("OK")
        }
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}
