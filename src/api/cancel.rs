use async_trait::async_trait;

use crate::messages::cancel::CancelRequest;

/// Handler for Cancel Request
#[async_trait]
pub trait CancelHandler: Send + Sync {
    async fn on_cancel_request(&self, cancel_request: CancelRequest);
}

pub struct DefaultCancelHandler;

#[async_trait]
impl CancelHandler for DefaultCancelHandler {
    async fn on_cancel_request(&self, cancel_request: CancelRequest) {
        println!("cancelling: {}", cancel_request.pid);
    }
}
