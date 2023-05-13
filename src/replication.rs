use futures::future::join_all;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

pub struct AsyncReplicas {
    tx: watch::Sender<(String, String)>,
    replicas: Arc<Mutex<Vec<SocketAddr>>>,
}

impl AsyncReplicas {
    pub async fn new() -> AsyncReplicas {
        let (tx, mut rx) = watch::channel(("".to_string(), "".to_string()));
        let replicas = Arc::new(Mutex::new(Vec::new()));
        {
            let replicas = replicas.clone();
            tokio::spawn(async move {
                while rx.changed().await.is_ok() {
                    let (key, value) = rx.borrow().clone();
                    let replicas = replicas.lock().unwrap().clone();
                    for addr in replicas.iter() {
                        let resp = reqwest::Client::new()
                            .put(format!("http://{}/kv/{}", addr, key))
                            .body(value.clone())
                            .send()
                            .await;

                        match resp {
                            Ok(_resp) => (),
                            Err(e) => println!("Failed to replicate to replica: {}", e),
                        }
                    }
                }
            });
        }

        AsyncReplicas {
            tx,
            replicas: replicas.clone(),
        }
    }

    pub fn add(&self, addr: SocketAddr) {
        let mut replicas = self.replicas.lock().unwrap();
        replicas.push(addr);
    }

    pub fn put(&self, key: String, value: String) {
        self.tx.send((key, value)).unwrap();
    }
}

pub struct SyncReplicas {
    replicas: Mutex<Vec<SocketAddr>>,
}

impl SyncReplicas {
    pub fn new() -> SyncReplicas {
        SyncReplicas {
            replicas: Mutex::new(Vec::new()),
        }
    }

    pub fn add(&self, addr: SocketAddr) {
        let mut replicas = self.replicas.lock().unwrap();
        replicas.push(addr);
    }

    pub async fn put(&self, key: String, value: String) -> Result<(), std::io::Error> {
        let replicas = self.replicas.lock().unwrap();
        let requests = replicas.iter().map(|addr| {
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
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct Replica {
    pub addr: String,
    pub asynchronous: bool,
}

impl Replica {
    pub fn new(port: u16, asynchronous: bool) -> Replica {
        Replica {
            addr: format!("127.0.0.1:{}", port),
            asynchronous,
        }
    }
}
