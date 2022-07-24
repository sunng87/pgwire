use std::collections::BTreeMap;

pub trait SessionStore<V> {
    fn get(&self, name: &str) -> Option<&V>;

    fn put(&mut self, name: &str, value: V);

    fn del(&mut self, name: &str);
}

#[derive(Debug, Default)]
pub struct MemSessionStore<V>(BTreeMap<String, V>);

impl<V> SessionStore<V> for MemSessionStore<V> {
    fn get(&self, name: &str) -> Option<&V> {
        self.0.get(name)
    }

    fn put(&mut self, name: &str, value: V) {
        self.0.insert(name.to_owned(), value);
    }

    fn del(&mut self, name: &str) {
        self.0.remove(name);
    }
}
