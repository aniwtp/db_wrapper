//! Commit policy — how buffered flushes reach the disk.
//!
//! Three strategies, selected at open time via [`CommitPolicy`]:
//!
//! * **`Durable`** (default) — each flush batch is its own write transaction
//!   committed with `Durability::Immediate` (fsync per batch).  Maximum
//!   durability, one fsync per flush.
//!
//! * **`Grouped`** — flush batches are submitted through redb's group-commit
//!   pipeline (`Database::submit_write_batch`).  Concurrent batches from
//!   regular + TTL buffers (or multiple threads) are merged into a single
//!   write transaction with a single fsync.  Durability is unchanged;
//!   only the fsync count drops.
//!
//! * **`GroupedNonDurable`** — same grouping, but the transaction commits
//!   with `Durability::None`: no fsync on the hot path.  Data becomes
//!   durable when a later durable commit (checkpoint) lands.  Fastest,
//!   weakest guarantees — a power loss may lose recent writes (the DB file
//!   itself stays consistent; redb recovers to the last durable commit).
//!
//! The policy is fixed per `DBWrapper` instance and intentionally not
//! switchable at runtime: mixing durability semantics mid-flight makes
//! reasoning about crash guarantees impossible.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use shodh_redb::WriteBatch;

use crate::DbError;
use crate::db;

/// Which commit strategy buffered flushes use.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum CommitPolicy {
    /// One durable (fsync) commit per flush batch.
    #[default]
    Durable,
    /// Batches go through redb group-commit: concurrent batches share one
    /// durable commit.  Same durability as `Durable`, fewer fsyncs.
    Grouped,
    /// Group-commit with `Durability::None`: no fsync on the hot path.
    /// Recent writes become durable at the next durable checkpoint.
    GroupedNonDurable,
}

impl CommitPolicy {
    pub(crate) const fn id(self) -> u8 {
        match self {
            Self::Durable => 0,
            Self::Grouped => 1,
            Self::GroupedNonDurable => 2,
        }
    }

    pub(crate) const fn from_id(id: u8) -> Self {
        match id {
            1 => Self::Grouped,
            2 => Self::GroupedNonDurable,
            _ => Self::Durable,
        }
    }
}

/// Runtime holder for the selected policy (interior mutability not needed:
/// the policy is fixed after open, the atomic only makes `&self` sharing easy).
#[derive(Debug, Clone)]
pub(crate) struct CommitPolicyCell(Arc<AtomicU8>);

impl CommitPolicyCell {
    pub(crate) fn new(policy: CommitPolicy) -> Self {
        Self(Arc::new(AtomicU8::new(policy.id())))
    }

    pub(crate) fn get(&self) -> CommitPolicy {
        CommitPolicy::from_id(self.0.load(Ordering::Relaxed))
    }
}

/// Apply a set of table operations through the configured commit path.
///
/// `op` receives a write transaction and performs inserts/removes for one
/// flush batch.  With `Durable` the closure runs in a fresh transaction that
/// is committed (or aborted on error) here.  With the grouped policies the
/// closure is packaged into a `WriteBatch` and submitted to redb's
/// group-commit pipeline, where it merges with whatever else is in flight.
pub(crate) fn commit_ops<F>(
    db: &shodh_redb::Database,
    policy: CommitPolicy,
    op: F,
) -> Result<(), DbError>
where
    F: FnOnce(&shodh_redb::WriteTransaction) -> Result<(), DbError> + Send + 'static,
{
    match policy {
        // redb group-commit pipeline: our batch merges with whatever other
        // batches are in flight; one durable commit (fsync) per group.
        CommitPolicy::Grouped => {
            // The WriteBatch closure must fail with shodh_redb::Error.  Our op
            // fails with DbError (e.g. TableNotFound); wrap it as a Corrupted
            // storage error — the message (with the original error) is preserved
            // and reported back to this caller via GroupCommitError::BatchFailed.
            let batch = WriteBatch::new(move |tx| {
                op(tx).map_err(|e| {
                    shodh_redb::Error::from(shodh_redb::error::StorageError::Corrupted(
                        e.to_string(),
                    ))
                })
            });
            db.submit_write_batch(batch).map_err(DbError::from)
        }
        // No grouped-non-durable API in shodh-redb: emulate with a dedicated
        // transaction committing at Durability::None (no fsync on this path).
        CommitPolicy::GroupedNonDurable => {
            let mut tx = db!(db.begin_write())?;
            db!(tx.set_durability(shodh_redb::Durability::None))?;
            match op(&tx) {
                Ok(()) => db!(tx.commit()),
                Err(e) => {
                    drop(tx);
                    Err(e)
                }
            }
        }
        // Classic path: one durable transaction per flush batch.
        CommitPolicy::Durable => {
            let tx = db!(db.begin_write())?;
            match op(&tx) {
                Ok(()) => db!(tx.commit()),
                Err(e) => {
                    drop(tx);
                    Err(e)
                }
            }
        }
    }
}
