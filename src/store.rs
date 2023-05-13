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

    pub fn get(&self, key: String) -> Option<String> {
        let inner = self.inner.read().unwrap();
        let val = inner.get(&key)?.clone();
        Some(val)
    }

    pub fn put(&self, key: String, value: String) {
        let mut inner = self.inner.write().unwrap();
        inner.insert(key, value);
    }
}
