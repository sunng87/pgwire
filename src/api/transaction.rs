use crate::messages::response::TransactionStatus;

impl TransactionStatus {
    pub fn to_idle_state(self) -> TransactionStatus {
        TransactionStatus::Idle
    }

    pub fn to_error_state(self) -> TransactionStatus {
        match self {
            TransactionStatus::Idle => TransactionStatus::Idle,
            _ => TransactionStatus::Error,
        }
    }

    pub fn to_in_transaction_state(self) -> TransactionStatus {
        match self {
            TransactionStatus::Idle => TransactionStatus::Transaction,
            TransactionStatus::Transaction => TransactionStatus::Transaction,
            TransactionStatus::Error => TransactionStatus::Error,
        }
    }
}
