use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use super::portal::Portal;
use super::stmt::StoredStatement;

pub trait PortalStore: Send + Sync {
    type Statement;
    type State;

    fn put_statement(&self, statement: Arc<StoredStatement<Self::Statement>>);

    fn rm_statement(&self, name: &str);

    fn get_statement(&self, name: &str) -> Option<Arc<StoredStatement<Self::Statement>>>;

    fn put_portal(&self, portal: Arc<Portal<Self::Statement, Self::State>>);

    fn rm_portal(&self, name: &str);

    fn get_portal(&self, name: &str) -> Option<Arc<Portal<Self::Statement, Self::State>>>;
}

#[derive(Clone, Default, Debug)]
pub struct EmptyState;

#[derive(Debug, Default, new)]
pub struct MemPortalStore<S> {
    #[new(default)]
    statements: RwLock<BTreeMap<String, Arc<StoredStatement<S>>>>,
    #[new(default)]
    portals: RwLock<BTreeMap<String, Arc<Portal<S, EmptyState>>>>,
}

impl<S: Clone + Send + Sync> PortalStore for MemPortalStore<S> {
    type Statement = S;
    type State = EmptyState;

    fn put_statement(&self, statement: Arc<StoredStatement<Self::Statement>>) {
        let mut guard = self.statements.write().unwrap();
        guard.insert(statement.id().to_owned(), statement);
    }

    fn rm_statement(&self, name: &str) {
        let mut guard = self.statements.write().unwrap();
        guard.remove(name);
    }

    fn get_statement(&self, name: &str) -> Option<Arc<StoredStatement<Self::Statement>>> {
        let guard = self.statements.read().unwrap();
        guard.get(name).cloned()
    }

    fn put_portal(&self, portal: Arc<Portal<Self::Statement, Self::State>>) {
        let mut guard = self.portals.write().unwrap();
        guard.insert(portal.name().to_owned(), portal);
    }

    fn rm_portal(&self, name: &str) {
        let mut guard = self.portals.write().unwrap();
        guard.remove(name);
    }

    fn get_portal(&self, name: &str) -> Option<Arc<Portal<Self::Statement, Self::State>>> {
        let guard = self.portals.read().unwrap();
        guard.get(name).cloned()
    }
}
