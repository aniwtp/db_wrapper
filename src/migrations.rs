//! Schema versioning and sequential migrations.
//!
//! The schema version is stored inside the main database file in the
//! internal `_schema` table (`u8 key → u64 version`).  A fresh database
//! starts at version 0 (no entry); migrations move it forward one step at
//! a time.  Each migration runs in its own write transaction and must be
//! idempotent: if the process dies mid-run, the version was not bumped, so
//! the same migration is simply applied again on the next open.
//!
//! # Example
//!
//! ```ignore
//! let db = DBWrapper::new("app.redb")?;
//! db.register_migration_boxed(MigrateV1ToV2);
//! db.register_migration_boxed(MigrateV2ToV3);
//! // Explicit call — never implicit on open.
//! let applied = db.run_migrations()?;
//! ```

use std::sync::Arc;

use shodh_redb::{Database, ReadableDatabase, TableDefinition};

use crate::db;
use crate::error::DbError;
use crate::wrapper::DBWrapper;

/// Internal table holding the schema version: key `0` → current version.
pub(crate) const SCHEMA_TABLE: TableDefinition<u8, u64> = TableDefinition::new("_schema");

/// Fixed key for the single version entry.
const SCHEMA_KEY: u8 = 0;

/// One sequential schema migration step.
///
/// Implementations must be idempotent: `run_migrations` only executes a step
/// when the stored version equals [`Self::from_version`], but a crash after
/// the data was rewritten and before the version bump replays the whole step
/// on the next open.
#[allow(clippy::wrong_self_convention)]
pub trait Migration: Send + Sync {
    /// Schema version this migration upgrades from (must be >= 1).
    fn from_version(&self) -> u64;

    /// Schema version this migration upgrades to (must be `from_version + 1`).
    fn to_version(&self) -> u64;

    /// Apply the migration.
    fn migrate(&self, db: &DBWrapper) -> Result<(), DbError>;
}

impl DBWrapper {
    /// Register a migration step.  Registration order does not matter;
    /// [`Self::run_migrations`] applies steps strictly sequentially by
    /// version.
    pub fn register_migration(&self, m: Arc<dyn Migration>) {
        self.migrations.lock().push(m);
    }

    /// Convenience wrapper: register an owned migration.
    pub fn register_migration_boxed(&self, m: impl Migration + 'static) {
        self.register_migration(Arc::new(m));
    }

    /// Read the current schema version (0 = fresh database).
    pub fn schema_version(&self) -> Result<u64, DbError> {
        read_schema_version(&self.db)
    }

    /// Apply all registered migrations whose `from_version` matches the
    /// stored schema version, one per write transaction, persisting the new
    /// version after each.  Idempotent: running it on an up-to-date database
    /// is a no-op.
    ///
    /// Returns the number of applied migrations.
    ///
    /// Fails with [`DbError::SchemaVersionTooNew`] if the file's version is
    /// higher than anything the registered chain can produce (downgrade
    /// guard): opening an older binary against a newer database is refused
    /// instead of silently corrupting it.
    pub fn run_migrations(&self) -> Result<usize, DbError> {
        // One runner at a time (per DBWrapper instance tree): two racing
        // callers would otherwise both pick the same pending step.
        let _run_guard = self.migrations_run_lock.lock();
        let mut applied = 0usize;
        loop {
            let current = self.schema_version()?;

            // Find exactly one pending step for the current version.
            let next = self
                .migrations
                .lock()
                .iter()
                .find(|m| m.from_version() == current)
                .cloned();

            let Some(migration) = next else {
                let max_known = self
                    .migrations
                    .lock()
                    .iter()
                    .map(|m| m.to_version())
                    .max()
                    .unwrap_or(0);
                if current > max_known && !self.migrations.lock().is_empty() {
                    return Err(DbError::SchemaVersionTooNew {
                        file_version: current,
                        supported_version: max_known,
                    });
                }
                // Up to date (or no chain registered at all).
                return Ok(applied);
            };

            log::info!(
                "running migration v{} → v{}",
                migration.from_version(),
                migration.to_version()
            );
            // Data first, version last: a crash between the two replays the
            // migration on the next open (hence the idempotency requirement).
            migration.migrate(self)?;
            write_schema_version(&self.db, migration.to_version())?;
            applied += 1;
        }
    }
}

/// Create the internal `_schema` table if missing.  Called from
/// `DBWrapper::new`; a no-op (no write transaction) on an existing database.
pub(crate) fn ensure_schema_table(db: &Database) -> Result<(), DbError> {
    crate::wrapper::ensure_table(db, SCHEMA_TABLE)
}

fn read_schema_version(db: &Database) -> Result<u64, DbError> {
    let tx = db!(db.begin_read())?;
    let table = db!(tx.open_table(SCHEMA_TABLE))?;
    match db!(table.get(SCHEMA_KEY))? {
        Some(v) => Ok(v.value()),
        None => Ok(0),
    }
}

fn write_schema_version(db: &Database, version: u64) -> Result<(), DbError> {
    let tx = db!(db.begin_write())?;
    {
        let mut table = db!(tx.open_table(SCHEMA_TABLE))?;
        db!(table.insert(SCHEMA_KEY, version))?;
    }
    db!(tx.commit())?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use shodh_redb::{ReadableTable, TableDefinition};
    use std::fs;
    use std::sync::atomic::{AtomicBool, AtomicU32};

    static TEST_ID: AtomicU32 = AtomicU32::new(0);
    fn unique_dir(label: &str) -> std::path::PathBuf {
        let id = TEST_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!("team_test_mig_{label}_{id}"))
    }

    const DATA_TABLE: TableDefinition<u64, String> = TableDefinition::new("mig_data");

    fn open_fresh(label: &str) -> (DBWrapper, String) {
        let dir = unique_dir(label);
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.redb");
        let db = DBWrapper::new(path.to_str().unwrap()).expect("new");
        db.register_table(DATA_TABLE);
        (db, dir.to_str().unwrap().to_owned())
    }

    fn reopen(dir: &str) -> DBWrapper {
        let path = format!("{dir}/test.redb");
        let db = DBWrapper::new(&path).expect("reopen");
        db.register_table(DATA_TABLE);
        db
    }

    fn seed_v1_values(db: &DBWrapper) {
        for i in 0..5u64 {
            db.write(DATA_TABLE, i, format!("old:{i}")).expect("seed");
        }
        db.flush_buffers().expect("flush");
    }

    /// v1→v2: rewrites every `"old:<n>"` value to `"new:<n>"`.
    struct RenameValues;

    impl Migration for RenameValues {
        fn from_version(&self) -> u64 {
            1
        }
        fn to_version(&self) -> u64 {
            2
        }
        fn migrate(&self, db: &DBWrapper) -> Result<(), DbError> {
            let tx = db!(db.db.begin_write())?;
            {
                let mut table = db!(tx.open_table(DATA_TABLE))?;
                let keys: Vec<u64> = table
                    .iter()
                    .map_err(|e| DbError::Redb(e.into()))?
                    .filter_map(|r| r.ok())
                    .map(|(k, _)| k.value())
                    .collect();
                for k in keys {
                    let cur = table
                        .get(k)
                        .map_err(|e| DbError::Redb(e.into()))?
                        .map(|g| g.value());
                    if let Some(v) = cur.filter(|v| v.starts_with("old:")) {
                        let n = v.trim_start_matches("old:");
                        db!(table.insert(k, format!("new:{n}")))?;
                    }
                }
            }
            db!(tx.commit())?;
            Ok(())
        }
    }

    /// v2→v3: appends a marker suffix; counts invocations to verify that a
    /// simulated crash replay does not double-apply on healthy runs.
    struct AddSuffix {
        calls: AtomicBool,
    }

    impl Migration for AddSuffix {
        fn from_version(&self) -> u64 {
            2
        }
        fn to_version(&self) -> u64 {
            3
        }
        fn migrate(&self, db: &DBWrapper) -> Result<(), DbError> {
            self.calls.store(true, std::sync::atomic::Ordering::Relaxed);
            let tx = db!(db.db.begin_write())?;
            {
                let mut table = db!(tx.open_table(DATA_TABLE))?;
                let keys: Vec<u64> = table
                    .iter()
                    .map_err(|e| DbError::Redb(e.into()))?
                    .filter_map(|r| r.ok())
                    .map(|(k, _)| k.value())
                    .collect();
                for k in keys {
                    let cur = table
                        .get(k)
                        .map_err(|e| DbError::Redb(e.into()))?
                        .map(|g| g.value());
                    if let Some(v) = cur.filter(|v| !v.ends_with("#v3")) {
                        db!(table.insert(k, format!("{v}#v3")))?;
                    }
                }
            }
            db!(tx.commit())?;
            Ok(())
        }
    }

    #[test]
    fn fresh_db_is_version_zero_and_needs_no_migrations() {
        let (db, dir) = open_fresh("fresh");
        assert_eq!(db.schema_version().expect("version"), 0);
        assert_eq!(db.run_migrations().expect("run"), 0);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn migration_applies_and_bumps_version() {
        let (db, dir) = open_fresh("apply");
        seed_v1_values(&db);
        write_schema_version(&db.db, 1).expect("set v1");

        db.register_migration_boxed(RenameValues);
        let applied = db.run_migrations().expect("run");
        assert_eq!(applied, 1);
        assert_eq!(db.schema_version().expect("version"), 2);

        let tx = db.db.begin_read().expect("read");
        let table = tx.open_table(DATA_TABLE).expect("open");
        assert_eq!(table.get(3u64).unwrap().unwrap().value(), "new:3");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn chain_applies_in_order_across_reopens() {
        let (db, dir) = open_fresh("chain");
        seed_v1_values(&db);
        write_schema_version(&db.db, 1).expect("set v1");
        drop(db);

        // First process: registers both steps, applies them sequentially.
        let db = reopen(&dir);
        db.register_migration_boxed(RenameValues);
        db.register_migration_boxed(AddSuffix {
            calls: AtomicBool::new(false),
        });

        let applied = db.run_migrations().expect("run");
        assert_eq!(applied, 2);
        assert_eq!(db.schema_version().expect("version"), 3);

        let tx = db.db.begin_read().expect("read");
        let table = tx.open_table(DATA_TABLE).expect("open");
        assert_eq!(table.get(4u64).unwrap().unwrap().value(), "new:4#v3");
        drop(db);

        // Second process (restart): everything is up to date, nothing runs.
        let db2 = reopen(&dir);
        db2.register_migration_boxed(RenameValues);
        db2.register_migration_boxed(AddSuffix {
            calls: AtomicBool::new(false),
        });
        assert_eq!(db2.run_migrations().expect("run"), 0);
        assert_eq!(db2.schema_version().expect("version"), 3);

        // Data untouched by the second run.
        let tx = db2.db.begin_read().expect("read");
        let table = tx.open_table(DATA_TABLE).expect("open");
        assert_eq!(table.get(0u64).unwrap().unwrap().value(), "new:0#v3");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn registration_order_does_not_matter() {
        let (db, dir) = open_fresh("order");
        write_schema_version(&db.db, 1).expect("set v1");

        // Register out of order.
        db.register_migration_boxed(AddSuffix {
            calls: AtomicBool::new(false),
        });
        db.register_migration_boxed(RenameValues);

        db.run_migrations().expect("run");
        assert_eq!(db.schema_version().expect("version"), 3);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn downgrade_is_refused_with_error() {
        let (db, dir) = open_fresh("downgrade");
        write_schema_version(&db.db, 5).expect("set v5");
        db.register_migration_boxed(RenameValues); // chain tops out at v2

        let err = db.run_migrations().expect_err("must refuse");
        assert!(
            matches!(
                err,
                DbError::SchemaVersionTooNew {
                    file_version: 5,
                    supported_version: 2
                }
            ),
            "unexpected error: {err:?}"
        );
        // Version is untouched by the failed attempt.
        assert_eq!(db.schema_version().expect("version"), 5);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn gap_in_chain_reports_no_pending_step() {
        let (db, dir) = open_fresh("gap");
        // File at v1, but only a v2→v3 step registered.
        write_schema_version(&db.db, 1).expect("set v1");
        db.register_migration_boxed(AddSuffix {
            calls: AtomicBool::new(false),
        });

        // No step matches v1 and the file is not *ahead* of the chain, so
        // this is a no-op success, not an error.
        assert_eq!(db.run_migrations().expect("run"), 0);
        assert_eq!(db.schema_version().expect("version"), 1);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn migration_failure_leaves_version_untouched() {
        struct FailingMigration;

        impl Migration for FailingMigration {
            fn from_version(&self) -> u64 {
                1
            }
            fn to_version(&self) -> u64 {
                2
            }
            fn migrate(&self, _db: &DBWrapper) -> Result<(), DbError> {
                Err(DbError::TableNotFound("boom".to_owned()))
            }
        }

        let (db, dir) = open_fresh("fail");
        write_schema_version(&db.db, 1).expect("set v1");
        db.register_migration_boxed(FailingMigration);

        assert!(db.run_migrations().is_err());
        // The version was NOT bumped — the migration will be retried on the
        // next open, exactly the crash-recovery contract.
        assert_eq!(db.schema_version().expect("version"), 1);
        drop(db);

        // "Next open": a fresh instance without the failing step succeeds.
        let db = reopen(&dir);
        db.register_migration_boxed(RenameValues);
        assert_eq!(db.run_migrations().expect("run"), 1);
        assert_eq!(db.schema_version().expect("version"), 2);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn concurrent_run_migrations_applies_each_step_once() {
        use std::sync::Arc as StdArc;

        let (db, dir) = open_fresh("concurrent");
        seed_v1_values(&db);
        write_schema_version(&db.db, 1).expect("set v1");

        let db = StdArc::new(db);
        let mut handles = Vec::new();
        for _ in 0..4 {
            let d = db.clone();
            handles.push(std::thread::spawn(move || {
                d.register_migration_boxed(RenameValues);
                d.register_migration_boxed(AddSuffix {
                    calls: AtomicBool::new(false),
                });
                d.run_migrations().expect("run")
            }));
        }
        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

        // Every step is applied exactly once across all racing callers.
        assert_eq!(total, 2, "steps were double-applied under concurrency");
        assert_eq!(db.schema_version().expect("version"), 3);

        let tx = db.db.begin_read().expect("read");
        let table = tx.open_table(DATA_TABLE).expect("open");
        assert_eq!(table.get(1u64).unwrap().unwrap().value(), "new:1#v3");
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn migrations_mutex_shared_across_clones() {
        let (db, dir) = open_fresh("clone");
        let clone = db.clone();
        clone.register_migration_boxed(RenameValues);
        // Registered through the clone must be visible via the original.
        assert_eq!(db.migrations.lock().len(), 1);
        fs::remove_dir_all(dir).ok();
    }
}
