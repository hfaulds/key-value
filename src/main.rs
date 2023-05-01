use actix_web::{get, put, web, App, HttpResponse, HttpServer, Responder};
use clap::Parser;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Mutex, RwLock};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    port: u16,
    #[arg(short, long, default_value = "")]
    follow: String,
}

struct KVStore {
    inner: RwLock<HashMap<String, String>>,
}

impl KVStore {
    fn get(&self, key: String) -> Option<String> {
        let inner = self.inner.read().unwrap();
        let val = inner.get(&key)?.clone();
        Some(val)
    }

    fn put(&self, key: String, value: String) {
        let mut inner = self.inner.write().unwrap();
        inner.insert(key, value);
    }
}

#[get("/kv/{key}")]
async fn get(path: web::Path<String>, kv: web::Data<KVStore>) -> impl Responder {
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
    kv: web::Data<KVStore>,
    replicas: web::Data<Replicas>,
    bytes: web::Bytes,
) -> impl Responder {
    let key = path.into_inner();
    let value = match String::from_utf8(bytes.to_vec()) {
        Ok(value) => value,
        Err(_) => return HttpResponse::BadRequest().body("Invalid UTF-8"),
    };

    println!("PUT {} {}", key, value);
    if let Err(e) = replicas.put(key.clone(), value.clone()).await {
        return HttpResponse::InternalServerError().body(e.to_string());
    }
    kv.put(key, value);
    HttpResponse::Ok().body("OK")
}

struct Replicas {
    inner: Mutex<Vec<SocketAddr>>,
}

impl Replicas {
    fn add(&self, addr: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        inner.push(addr);
    }

    async fn put(&self, key: String, value: String) -> Result<(), std::io::Error> {
        let inner = self.inner.lock().unwrap();
        let requests = inner.iter().map(|addr| {
            reqwest::Client::new()
                .put(format!("http://{}/kv/{}", addr, key))
                .body(value.clone())
                .send()
        });

        for resp in join_all(requests).await {
            match resp {
                Ok(_resp) => (),
                Err(e) => {
                    return std::io::Result::Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Failed to replicate to replica: {}", e),
                    ))
                }
            }
        }
        return Ok(());
    }
}

#[derive(Serialize, Deserialize)]
struct Replica {
    addr: String,
}

#[put("/replica")]
async fn replica(replicas: web::Data<Replicas>, replica: web::Json<Replica>) -> impl Responder {
    let addr = replica.addr.parse::<SocketAddr>();
    match addr {
        Ok(addr) => {
            replicas.add(addr);
            println!("Replica added: {}", addr);
            HttpResponse::Ok().body("OK")
        }
        Err(e) => HttpResponse::BadRequest().body(e.to_string()),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let kv_store = web::Data::new(KVStore {
        inner: RwLock::new(HashMap::new()),
    });
    let replicas = web::Data::new(Replicas {
        inner: Mutex::new(Vec::new()),
    });

    let http_server = HttpServer::new(move || {
        App::new()
            .app_data(kv_store.clone())
            .app_data(replicas.clone())
            .service(get)
            .service(put)
            .service(replica)
    });

    println!("Listening on port {}", args.port);
    let server = http_server.bind(("127.0.0.1", args.port)).unwrap().run();

    if args.follow != "" {
        let resp = reqwest::Client::new()
            .put(format!("{}/replica", args.follow))
            .json(&Replica {
                addr: format!("127.0.0.1:{}", args.port),
            })
            .send()
            .await;
        match resp {
            Ok(resp) => println!("Registered with primary: {}", resp.status()),
            Err(e) => {
                return std::io::Result::Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Failed to register with primary: {}", e),
                ))
            }
        }
    }

    server.await
}
