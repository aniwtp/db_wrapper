//! `team-db` — embedded redb database layer with atomic counters and unified write buffers.
//!
//! # Architecture
//!
//! - **Counters**: lock-free `AtomicU64` array, persisted as a single blob.
//! - **Buffers**: two unified buffers (regular + TTL) for all tables, no per-table pools.
//! - **Read-through**: `get_buffered()` checks buffers before falling back to DB.
//! - **No async runtime**: flush is triggered manually or by size threshold.

pub mod counter;
pub mod buffer;
pub mod wrapper;
pub mod error;

pub use wrapper::DBWrapper;
pub use error::DbError;
pub use buffer::serialize_value;

// ---------------------------------------------------------------------------
// Internal helper macro — wraps redb calls into DbError::Redb
// ---------------------------------------------------------------------------

#[macro_export]
macro_rules! db {
    ($e:expr) => {
        $e.map_err(|e| $crate::DbError::Redb(e.into()))
    };
}
