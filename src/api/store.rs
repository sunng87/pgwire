use std::any::Any;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use super::portal::Portal;
use super::stmt::StoredStatement;

/// Storage trait for prepared statements and portals.
pub trait PortalStore: Any + Send + Sync + 'static {
    type Statement;

    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Store a prepared statement by name.
    fn put_statement(&self, statement: Arc<StoredStatement<Self::Statement>>);

    /// Remove a prepared statement by name.
    fn rm_statement(&self, name: &str);

    /// Retrieve a prepared statement by name.
    fn get_statement(&self, name: &str) -> Option<Arc<StoredStatement<Self::Statement>>>;

    /// Store a portal by name.
    fn put_portal(&self, portal: Arc<Portal<Self::Statement>>);

    /// Remove a portal by name.
    fn rm_portal(&self, name: &str);

    /// Remove all portals.
    fn clear_portals(&self);

    /// Retrieve a portal by name.
    fn get_portal(&self, name: &str) -> Option<Arc<Portal<Self::Statement>>>;
}

/// In-memory implementation of `PortalStore` backed by `BTreeMap`.
#[derive(Debug, Default, new)]
pub struct MemPortalStore<S> {
    #[new(default)]
    statements: RwLock<BTreeMap<String, Arc<StoredStatement<S>>>>,
    #[new(default)]
    portals: RwLock<BTreeMap<String, Arc<Portal<S>>>>,
}

impl<S: Clone + Send + Sync + 'static> PortalStore for MemPortalStore<S> {
    type Statement = S;

    fn as_any(&self) -> &dyn Any {
        self
    }

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

    fn clear_portals(&self) {
        let mut guard = self.portals.write().unwrap();
        guard.clear();
    }

    fn get_portal(&self, name: &str) -> Option<Arc<Portal<Self::Statement>>> {
        let guard = self.portals.read().unwrap();
        guard.get(name).cloned()
    }
}
