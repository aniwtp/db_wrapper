//! Database-layer errors.

/// Errors originating from the database / redb layer.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// Underlying redb storage error.
    #[error("storage error: {0}")]
    Redb(#[from] shodh_redb::error::Error),

    /// Filesystem I/O during database operations.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The requested table does not exist or wasn't registered.
    #[error("table `{0}` not found or not registered")]
    TableNotFound(String),
}
