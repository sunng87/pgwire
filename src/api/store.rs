use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use super::portal::Portal;
use super::results::QueryResponse;
use super::stmt::StoredStatement;

pub trait PortalStore: Send + Sync {
    type Statement;

    fn put_statement(&self, statement: Arc<StoredStatement<Self::Statement>>);

    fn rm_statement(&self, name: &str);

    fn get_statement(&self, name: &str) -> Option<Arc<StoredStatement<Self::Statement>>>;

    fn put_portal(&self, portal: Arc<Portal<Self::Statement>>);

    fn rm_portal(&self, name: &str);

    fn get_portal(&self, name: &str) -> Option<Arc<Portal<Self::Statement>>>;
}

#[derive(Debug, Default, new)]
pub struct MemPortalStore<S> {
    #[new(default)]
    statements: RwLock<BTreeMap<String, Arc<StoredStatement<S>>>>,
    #[new(default)]
    portals: RwLock<BTreeMap<String, Arc<Portal<S>>>>,
}

impl<S: Clone + Send + Sync> PortalStore for MemPortalStore<S> {
    type Statement = S;

    fn put_statement(&self, statement: Arc<StoredStatement<Self::Statement>>) {
        let mut guard = self.statements.write().unwrap();
        guard.insert(statement.id.to_owned(), statement);
    }

    fn rm_statement(&self, name: &str) {
        let mut guard = self.statements.write().unwrap();
        guard.remove(name);
    }

    fn get_statement(&self, name: &str) -> Option<Arc<StoredStatement<Self::Statement>>> {
        let guard = self.statements.read().unwrap();
        guard.get(name).cloned()
    }

    fn put_portal(&self, portal: Arc<Portal<Self::Statement>>) {
        let mut guard = self.portals.write().unwrap();
        guard.insert(portal.name.to_owned(), portal);
    }

    fn rm_portal(&self, name: &str) {
        let mut guard = self.portals.write().unwrap();
        guard.remove(name);
    }

    fn get_portal(&self, name: &str) -> Option<Arc<Portal<Self::Statement>>> {
        let guard = self.portals.read().unwrap();
        guard.get(name).cloned()
    }
}

pub trait PortalSuspendedResult {
    fn put_result(&self, name: &str, result: Arc<QueryResponse>);

    fn rm_result(&self, name: &str);

    fn get_result(&self, name: &str) -> Option<Arc<QueryResponse>>;

    fn take_result(&self, name: &str) -> Option<Arc<QueryResponse>>;
}

#[derive(Default, new)]
pub struct MemPortalSuspendedResult {
    #[new(default)]
    results: RwLock<BTreeMap<String, Arc<QueryResponse>>>,
}

impl std::fmt::Debug for MemPortalSuspendedResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemPortalSuspendedResult")
    }
}

impl PortalSuspendedResult for MemPortalSuspendedResult {
    fn put_result(&self, name: &str, result: Arc<QueryResponse>) {
        let mut guard = self.results.write().unwrap();
        guard.insert(name.to_owned(), result);
    }

    fn rm_result(&self, name: &str) {
        let mut guard = self.results.write().unwrap();
        guard.remove(name);
    }

    fn get_result(&self, name: &str) -> Option<Arc<QueryResponse>> {
        let guard = self.results.read().unwrap();
        guard.get(name).cloned()
    }

    fn take_result(&self, name: &str) -> Option<Arc<QueryResponse>> {
        let mut guard = self.results.write().unwrap();
        guard.remove(name)
    }
}
