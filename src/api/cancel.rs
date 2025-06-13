use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::channel::oneshot::Sender;
use futures::lock::Mutex;

use crate::messages::cancel::CancelRequest;

/// Handler for Cancel Request
#[async_trait]
pub trait CancelHandler: Send + Sync {
    async fn on_cancel_request(&self, cancel_request: CancelRequest);
}

#[derive(Debug, new)]
pub struct DefaultCancelHandler {
    cancel_manager: Arc<Mutex<DefaultQueryCancelManager>>,
}

#[async_trait]
impl CancelHandler for DefaultCancelHandler {
    async fn on_cancel_request(&self, cancel_request: CancelRequest) {
        let keypair = (cancel_request.pid, cancel_request.secret_key);
        if let Some(handle) = self.cancel_manager.lock().await.remove(&keypair) {
            let _ = handle.send(());
        }
    }
}

#[async_trait]
impl CancelHandler for super::NoopHandler {
    async fn on_cancel_request(&self, _cancel_request: CancelRequest) {}
}

pub trait QueryCancelManager {
    /// Add query handle to the manager
    fn add(&mut self, keypair: (i32, i32), handle: Sender<()>);

    /// Remove query handle from the manager, return the handle if keypair matches
    fn remove(&mut self, keypair: &(i32, i32)) -> Option<Sender<()>>;
}

#[derive(Debug)]
pub struct DefaultQueryCancelManager {
    inner: HashMap<(i32, i32), Sender<()>>,
}

impl QueryCancelManager for DefaultQueryCancelManager {
    fn add(&mut self, keypair: (i32, i32), handle: Sender<()>) {
        self.inner.insert(keypair, handle);
    }

    fn remove(&mut self, keypair: &(i32, i32)) -> Option<Sender<()>> {
        self.inner.remove(keypair)
    }
}

/// This implementation of QueryCancelManager is designed
#[derive(Debug)]
pub struct NoopQueryCancelManager;

impl QueryCancelManager for NoopQueryCancelManager {
    fn add(&mut self, _keypair: (i32, i32), _handle: Sender<()>) {}

    fn remove(&mut self, _keypair: &(i32, i32)) -> Option<Sender<()>> {
        None
    }
}
