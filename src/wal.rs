use std::sync::Mutex;

struct LogState {
    id: u64,
    ops: Vec<Op>,
}

pub struct Log {
    inner: Mutex<LogState>,
}

#[derive(Clone, Debug)]
pub struct Op {
    pub id: u64,
    pub kind: OpKind,
}

impl Default for Op {
    fn default() -> Op {
        Op {
            id: 0,
            kind: OpKind::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum OpKind {
    Set { key: String, value: String },
}

impl Default for OpKind {
    fn default() -> OpKind {
        OpKind::Set {
            key: String::new(),
            value: String::new(),
        }
    }
}

impl Log {
    pub fn new() -> Log {
        Log {
            inner: Mutex::new(LogState {
                id: 0,
                ops: Vec::new(),
            }),
        }
    }

    pub fn put(&self, key: String, value: String) -> Op {
        let mut inner = self.inner.lock().unwrap();
        inner.id += 1;
        let op = Op {
            id: inner.id,
            kind: OpKind::Set { key, value },
        };
        inner.ops.push(op.clone());
        op
    }
}
