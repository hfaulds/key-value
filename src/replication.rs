use crate::*;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

pub struct AsyncReplicas {
    ops_sender: watch::Sender<(String, String)>,
    replicas: Arc<Mutex<Vec<SocketAddr>>>,
}

impl AsyncReplicas {
    pub async fn new() -> AsyncReplicas {
        let (ops_sender, mut ops_reader) = watch::channel(("".to_string(), "".to_string()));
        let replicas = Arc::new(Mutex::new(Vec::new()));
        {
            let replicas = replicas.clone();
            tokio::spawn(async move {
                while ops_reader.changed().await.is_ok() {
                    let (key, value) = ops_reader.borrow().clone();
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
            ops_sender,
            replicas: replicas.clone(),
        }
    }

    pub fn add(&self, addr: SocketAddr) {
        let mut replicas = self.replicas.lock().unwrap();
        replicas.push(addr);
    }

    pub fn put(&self, key: String, value: String) {
        self.ops_sender.send((key, value)).unwrap();
    }

    pub fn len(&self) -> usize {
        self.replicas.lock().unwrap().len()
    }
}

#[derive(Clone, Debug)]
pub struct ReplicationError {}

impl fmt::Display for ReplicationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ReplicationError")
    }
}

#[derive(Clone, Debug)]
struct ReplicationResult {
    id: u64,
    res: Result<(), ReplicationError>,
}

impl Default for ReplicationResult {
    fn default() -> Self {
        Self { id: 0, res: Ok(()) }
    }
}

struct SyncReplica {
    ops_sender: watch::Sender<Op>,
    results_reader: watch::Receiver<ReplicationResult>,
}

impl SyncReplica {
    pub fn spawn(addr: SocketAddr) -> Self {
        let (ops_sender, mut ops_reader) = watch::channel(Op::default());
        let (results_sender, results_reader) = watch::channel(ReplicationResult::default());

        tokio::spawn(async move {
            while ops_reader.changed().await.is_ok() {
                let op = ops_reader.borrow().clone();
                match op.kind {
                    OpKind::Set { key, value } => {
                        let resp = reqwest::Client::new()
                            .put(format!("http://{}/kv/{}", addr, key))
                            .body(value.clone())
                            .send()
                            .await;

                        let res = match resp {
                            Ok(_resp) => Ok(()),
                            Err(_e) => Err(ReplicationError {}),
                        };
                        let res = ReplicationResult { id: op.id, res };
                        println!("Replication result: {:?}", res);

                        match results_sender.send(res) {
                            Ok(_) => (),
                            Err(e) => println!("Failed to send result of replication: {}", e),
                        }
                    }
                }
            }
        });
        Self {
            ops_sender,
            results_reader,
        }
    }

    pub async fn replicate(&mut self, op: Op) -> Result<(), ReplicationError> {
        self.ops_sender.send(op.clone()).unwrap();
        let res = self
            .results_reader
            .wait_for(|res| res.id >= op.id)
            .await
            .unwrap()
            .clone();
        println!("Replication result: {:?}", res);
        res.res
    }
}

pub struct SyncReplicas {
    replicas: Arc<Mutex<Vec<SyncReplica>>>,
}

impl SyncReplicas {
    pub fn new() -> SyncReplicas {
        SyncReplicas {
            replicas: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn add(&self, addr: SocketAddr) {
        let r = SyncReplica::spawn(addr);
        let mut replicas = self.replicas.lock().unwrap();
        replicas.push(r);
    }

    pub fn len(&self) -> usize {
        let replicas = self.replicas.lock().unwrap();
        replicas.len()
    }

    pub async fn replicate(&self, op: Op) -> Result<(), ReplicationError> {
        let mut replicas = self.replicas.lock().unwrap();
        let res =
            futures::future::join_all(replicas.iter_mut().map(|r| r.replicate(op.clone()))).await;
        for r in res {
            println!("Replication res: {:?}", r);
            if r.is_err() {
                return r;
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
