use actix_web::{put, web, HttpResponse, Responder};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Mutex;

pub struct Replicas {
    inner: Mutex<Vec<SocketAddr>>,
}

impl Replicas {
    pub fn new() -> Replicas {
        Replicas {
            inner: Mutex::new(Vec::new()),
        }
    }

    fn add(&self, addr: SocketAddr) {
        let mut inner = self.inner.lock().unwrap();
        inner.push(addr);
    }

    pub async fn put(&self, key: String, value: String) -> Result<(), std::io::Error> {
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
pub struct Replica {
    addr: String,
}

impl Replica {
    pub fn new(port: u16) -> Replica {
        Replica {
            addr: format!("127.0.0.1:{}", port),
        }
    }
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
