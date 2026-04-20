use crate::messages::response::TransactionStatus;

impl TransactionStatus {
    /// Transition to idle (no active transaction) state.
    pub fn to_idle_state(self) -> TransactionStatus {
        TransactionStatus::Idle
    }

    /// Transition to error state, preserving idle state.
    pub fn to_error_state(self) -> TransactionStatus {
        match self {
            TransactionStatus::Idle => TransactionStatus::Idle,
            _ => TransactionStatus::Error,
        }
    }

    /// Transition to in-transaction state.
    pub fn to_in_transaction_state(self) -> TransactionStatus {
        match self {
            TransactionStatus::Idle => TransactionStatus::Transaction,
            TransactionStatus::Transaction => TransactionStatus::Transaction,
            TransactionStatus::Error => TransactionStatus::Error,
        }
    }
}
