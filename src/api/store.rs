use std::any::Any;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use super::portal::Portal;
use super::stmt::StoredStatement;

/// The stored state of a prepared statement name.
///
/// [`StatementEntry::Empty`] represents a statement that was `Parse`d from an
/// empty query (a query string without any statement, such as `""` or
/// `";;"`). Following PostgreSQL's extended-query protocol, an empty
/// statement has no parsed representation: it binds with zero parameters,
/// describes as `ParameterDescription` (no parameters) + `NoData`, and
/// executes to `EmptyQueryResponse` without reaching the query handler.
#[derive(Debug, Clone)]
pub enum StatementEntry<S> {
    /// The name holds a statement parsed from an empty query.
    Empty,
    /// The name holds a parsed statement.
    Statement(Arc<StoredStatement<S>>),
}

impl<S> StatementEntry<S> {
    /// Get the stored statement, if this entry is not empty.
    pub fn as_statement(&self) -> Option<&Arc<StoredStatement<S>>> {
        match self {
            StatementEntry::Empty => None,
            StatementEntry::Statement(stmt) => Some(stmt),
        }
    }

    /// Test whether this entry is an empty statement.
    pub fn is_empty(&self) -> bool {
        matches!(self, StatementEntry::Empty)
    }
}

/// The stored state of a portal name.
///
/// [`PortalEntry::Empty`] represents a portal bound from an empty statement:
/// it describes as `NoData` and executes to `EmptyQueryResponse`.
#[derive(Debug, Clone)]
pub enum PortalEntry<S> {
    /// The name holds a portal bound from an empty statement.
    Empty,
    /// The name holds a bound portal.
    Portal(Arc<Portal<S>>),
}

impl<S> PortalEntry<S> {
    /// Get the bound portal, if this entry is not empty.
    pub fn as_portal(&self) -> Option<&Arc<Portal<S>>> {
        match self {
            PortalEntry::Empty => None,
            PortalEntry::Portal(portal) => Some(portal),
        }
    }

    /// Test whether this entry is an empty portal.
    pub fn is_empty(&self) -> bool {
        matches!(self, PortalEntry::Empty)
    }
}

/// Storage trait for prepared statements and portals.
///
/// Both statements and portals can also be *empty*: a `Parse` of an empty
/// query or a `Bind` on an empty statement stores an empty marker under the
/// target name (replacing whatever was stored under that name before), like
/// PostgreSQL. Empty entries are returned by `get_statement`/`get_portal` as
/// [`StatementEntry::Empty`]/[`PortalEntry::Empty`]; removing or clearing
/// removes them along with regular entries.
pub trait PortalStore: Any + Send + Sync + 'static {
    type Statement;

    /// Downcast to concrete type.
    fn as_any(&self) -> &dyn Any;

    /// Store a prepared statement by name.
    fn put_statement(&self, statement: Arc<StoredStatement<Self::Statement>>);

    /// Store an empty prepared statement by name, replacing any statement
    /// previously stored under the same name.
    fn put_empty_statement(&self, name: &str);

    /// Remove a prepared statement by name.
    fn rm_statement(&self, name: &str);

    /// Retrieve a prepared statement by name.
    fn get_statement(&self, name: &str) -> Option<StatementEntry<Self::Statement>>;

    /// Store a portal by name.
    fn put_portal(&self, portal: Arc<Portal<Self::Statement>>);

    /// Store an empty portal by name, replacing any portal previously stored
    /// under the same name.
    fn put_empty_portal(&self, name: &str);

    /// Remove a portal by name.
    fn rm_portal(&self, name: &str);

    /// Remove all portals, including empty ones.
    fn clear_portals(&self);

    /// Retrieve a portal by name.
    fn get_portal(&self, name: &str) -> Option<PortalEntry<Self::Statement>>;
}

/// In-memory implementation of `PortalStore` backed by `BTreeMap`.
#[derive(Debug, Default, new)]
pub struct MemPortalStore<S> {
    #[new(default)]
    statements: RwLock<BTreeMap<String, StatementEntry<S>>>,
    #[new(default)]
    portals: RwLock<BTreeMap<String, PortalEntry<S>>>,
}

impl<S: Clone + Send + Sync + 'static> PortalStore for MemPortalStore<S> {
    type Statement = S;

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn put_statement(&self, statement: Arc<StoredStatement<Self::Statement>>) {
        let name = statement.id.to_owned();
        let mut guard = self.statements.write().unwrap();
        guard.insert(name, StatementEntry::Statement(statement));
    }

    fn put_empty_statement(&self, name: &str) {
        let mut guard = self.statements.write().unwrap();
        guard.insert(name.to_owned(), StatementEntry::Empty);
    }

    fn rm_statement(&self, name: &str) {
        let mut guard = self.statements.write().unwrap();
        guard.remove(name);
    }

    fn get_statement(&self, name: &str) -> Option<StatementEntry<Self::Statement>> {
        let guard = self.statements.read().unwrap();
        guard.get(name).cloned()
    }

    fn put_portal(&self, portal: Arc<Portal<Self::Statement>>) {
        let mut guard = self.portals.write().unwrap();
        guard.insert(portal.name.to_owned(), PortalEntry::Portal(portal));
    }

    fn put_empty_portal(&self, name: &str) {
        let mut guard = self.portals.write().unwrap();
        guard.insert(name.to_owned(), PortalEntry::Empty);
    }

    fn rm_portal(&self, name: &str) {
        let mut guard = self.portals.write().unwrap();
        guard.remove(name);
    }

    fn clear_portals(&self) {
        let mut guard = self.portals.write().unwrap();
        guard.clear();
    }

    fn get_portal(&self, name: &str) -> Option<PortalEntry<Self::Statement>> {
        let guard = self.portals.read().unwrap();
        guard.get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_entries_replace_each_other() {
        let store: MemPortalStore<String> = MemPortalStore::new();
        assert!(store.get_statement("s").is_none());

        store.put_empty_statement("s");
        assert!(store.get_statement("s").unwrap().is_empty());

        // a real statement replaces the empty marker
        store.put_statement(Arc::new(StoredStatement::new(
            "s".to_owned(),
            "select 1".to_owned(),
            vec![],
        )));
        assert_eq!(
            store
                .get_statement("s")
                .and_then(|e| e.as_statement().map(|s| s.statement.clone())),
            Some("select 1".to_owned())
        );

        // and an empty marker replaces the real statement
        store.put_empty_statement("s");
        assert!(store.get_statement("s").unwrap().is_empty());

        store.rm_statement("s");
        assert!(store.get_statement("s").is_none());
    }

    #[test]
    fn portal_entries_replace_each_other_and_clear() {
        let store: MemPortalStore<String> = MemPortalStore::new();
        let statement = Arc::new(StoredStatement::new(
            "s".to_owned(),
            "select 1".to_owned(),
            vec![],
        ));
        let portal = Portal::new_cursor("p".to_owned(), statement);

        store.put_portal(Arc::new(portal));
        assert!(store.get_portal("p").unwrap().as_portal().is_some());

        store.put_empty_portal("p");
        assert!(store.get_portal("p").unwrap().is_empty());

        store.put_empty_portal("p2");
        store.clear_portals();
        assert!(store.get_portal("p").is_none());
        assert!(store.get_portal("p2").is_none());
    }
}
