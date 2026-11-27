use std::sync::Arc;

use async_trait::async_trait;

use super::ConnectionManager;
use crate::messages::cancel::CancelRequest;

/// Handler for Cancel Request
#[async_trait]
pub trait CancelHandler: Send + Sync {
    /// Called for a cancel request or when a TCP client disconnects during
    /// message processing after startup.
    async fn on_cancel_request(&self, cancel_request: CancelRequest);
}

/// Default cancel handler that delegates to the connection manager.
#[derive(Debug, derive_new::new)]
pub struct DefaultCancelHandler {
    manager: Arc<ConnectionManager>,
}

#[async_trait]
impl CancelHandler for DefaultCancelHandler {
    async fn on_cancel_request(&self, cancel_request: CancelRequest) {
        self.manager
            .cancel(cancel_request.pid, &cancel_request.secret_key)
            .await;
    }
}

#[async_trait]
impl CancelHandler for super::NoopHandler {
    async fn on_cancel_request(&self, _cancel_request: CancelRequest) {}
}
