use actix_web::{get, put, web, App, HttpResponse, HttpServer, Responder};
use std::collections::HashMap;
use std::env;
use std::sync::RwLock;

struct KVStore {
    inner: RwLock<HashMap<String, String>>,
}

impl KVStore {
    fn list(&self) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        inner.keys().cloned().collect::<Vec<String>>()
    }

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

#[get("/{key}")]
async fn get(path: web::Path<String>, kv: web::Data<KVStore>) -> impl Responder {
    let key = path.into_inner();
    println!("GET {}", key);
    match kv.get(key.clone()) {
        Some(value) => HttpResponse::Ok().body(value),
        None => HttpResponse::NotFound().body("Not Found"),
    }
}

#[put("/{key}")]
async fn put(path: web::Path<String>, kv: web::Data<KVStore>, bytes: web::Bytes) -> impl Responder {
    let key = path.into_inner();
    let value = match String::from_utf8(bytes.to_vec()) {
        Ok(value) => value,
        Err(_) => return HttpResponse::BadRequest(),
    };
    println!("PUT {} {}", key, value);
    kv.put(key, value);
    HttpResponse::Ok()
}

#[get("/")]
async fn index(kv: web::Data<KVStore>) -> impl Responder {
    kv.list().join("\n")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();

    let port = if args.len() > 1 {
        args[1].parse::<u16>().unwrap()
    } else {
        8080
    };
    let kv_store = web::Data::new(KVStore {
        inner: RwLock::new(HashMap::new()),
    });

    let http_server = HttpServer::new(move || {
        App::new()
            .app_data(kv_store.clone())
            .service(index)
            .service(get)
            .service(put)
    });

    println!("Listening on port {}", port);
    http_server.bind(("127.0.0.1", port)).unwrap().run().await
}
