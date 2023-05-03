use crate::Replicas;
use actix_web::{get, put, web, HttpResponse, Responder};
use std::collections::HashMap;
use std::sync::RwLock;

pub struct Store {
    inner: RwLock<HashMap<String, String>>,
}

impl Store {
    pub fn new() -> Store {
        Store {
            inner: RwLock::new(HashMap::new()),
        }
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
