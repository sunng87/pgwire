use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

pub trait SessionStore<V>: Send + Sync {
    fn get(&self, name: &str) -> Option<Arc<V>>;

    fn put(&mut self, name: &str, value: Arc<V>);

    fn del(&mut self, name: &str);
}

#[derive(Debug, Default)]
pub struct MemSessionStore<V>(RwLock<BTreeMap<String, Arc<V>>>);

impl<V: Send + Sync> SessionStore<V> for MemSessionStore<V> {
    fn get(&self, name: &str) -> Option<Arc<V>> {
        let guard = self.0.read().unwrap();
        guard.get(name).cloned()
    }

    fn put(&mut self, name: &str, value: Arc<V>) {
        let mut guard = self.0.write().unwrap();
        guard.insert(name.to_owned(), value);
    }

    fn del(&mut self, name: &str) {
        let mut guard = self.0.write().unwrap();
        guard.remove(name);
    }
}
