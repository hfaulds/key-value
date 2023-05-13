use std::sync::RwLock;

struct LogState {
    id: u64,
    ops: Vec<Op>,
}

pub struct Log {
    inner: RwLock<LogState>,
}

enum Op {
    Set { key: String, value: String },
}

impl Log {
    pub fn new() -> Log {
        Log {
            inner: RwLock::new(LogState {
                id: 0,
                ops: Vec::new(),
            }),
        }
    }

    pub async fn put(&self, key: String, value: String) {
        let mut inner = self.inner.write().unwrap();
        inner.id += 1;
        inner.ops.push(Op::Set { key, value });
    }
}
