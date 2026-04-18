use std::sync::Arc;

use async_trait::async_trait;

use super::ConnectionManager;
use crate::messages::cancel::CancelRequest;

#[async_trait]
pub trait CancelHandler: Send + Sync {
    async fn on_cancel_request(&self, cancel_request: CancelRequest);
}

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
