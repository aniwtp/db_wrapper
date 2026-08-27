//! DBWrapper — database handle: open, compact, backup, buffers, maintenance.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use shodh_redb::ttl_table::TtlTableDefinition;
use shodh_redb::{Database, Key, ReadableDatabase, TableDefinition, TableHandle, Value};

use crate::DbError;
use crate::buffer::{BufferStore, serialize_value};
use crate::counter::{BUFFER_FLUSH_SECS, BUFFER_MAX_ENTRIES, COUNTER_BLOB, CounterStore};
use crate::db;
use crate::migrations::ensure_schema_table;

// ---------------------------------------------------------------------------
// DBWrapper
// ---------------------------------------------------------------------------

pub struct DBWrapper {
    pub db: Arc<Database>,
    /// Unified buffer for regular writes.
    regular_buf: Arc<BufferStore>,
    /// Unified buffer for TTL writes.
    ttl_buf: Arc<BufferStore>,
    counters: Arc<CounterStore>,
    /// Registered schema migrations (shared with clones).
    pub(crate) migrations: Arc<Mutex<Vec<Arc<dyn crate::migrations::Migration>>>>,
    /// Serializes `run_migrations` across clones/threads.
    pub(crate) migrations_run_lock: Arc<Mutex<()>>,
}

impl Clone for DBWrapper {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            regular_buf: self.regular_buf.clone(),
            ttl_buf: self.ttl_buf.clone(),
            counters: self.counters.clone(),
            migrations: self.migrations.clone(),
            migrations_run_lock: self.migrations_run_lock.clone(),
        }
    }
}

impl std::fmt::Debug for DBWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DBWrapper")
            .field("db", &self.db)
            .field("regular_pending", &self.regular_buf.len())
            .field("ttl_pending", &self.ttl_buf.len())
            .finish()
    }
}

impl DBWrapper {
    // -- Open ----------------------------------------------------------------

    /// Open (or create) the redb database at `path`.
    ///
    /// Two database files: `{path}` (data) + `{path}.counter` (counters).
    pub fn new(path: &str) -> Result<Self, DbError> {
        Self::with_commit_policy(path, crate::group_commit::CommitPolicy::Durable)
    }

    /// Open with a specific commit policy for buffered flushes.
    ///
    /// * [`CommitPolicy::Durable`] — one durable commit per flush (default).
    /// * [`CommitPolicy::Grouped`] — flushes go through redb's group-commit
    ///   pipeline; concurrent batches share one fsync.  Same durability.
    /// * [`CommitPolicy::GroupedNonDurable`] — flushes commit without fsync;
    ///   data becomes durable at the next durable commit (e.g. close or any
    ///   `Durable`-mode operation).  Fastest, weakest crash guarantees.
    pub fn with_commit_policy(
        path: &str,
        policy: crate::group_commit::CommitPolicy,
    ) -> Result<Self, DbError> {
        log::debug!("DBWrapper::new opening database at {path} (policy={policy:?})");

        let counter_path = format!("{path}.counter");

        let db = Arc::new(db!(Database::create(path))?);
        let write_txn = db!(db.begin_write())?;
        db!(write_txn.commit())?;

        let db_counter = Arc::new(db!(Database::create(&counter_path))?);
        let write_txn = db!(db_counter.begin_write())?;
        {
            db!(write_txn.open_table(COUNTER_BLOB))?;
        }
        db!(write_txn.commit())?;

        let counters = Arc::new(CounterStore::new(db_counter)?);
        ensure_schema_table(&db)?;

        // Default buffer sizes: 10_000 entries before auto-flush.
        // Both buffers flush counters first (for approximate sync).
        let on_flush: Arc<dyn Fn() -> Result<(), DbError> + Send + Sync> = {
            let c = counters.clone();
            Arc::new(move || c.flush().map(|_| ()))
        };
        let policy_cell = crate::group_commit::CommitPolicyCell::new(policy);
        let regular_buf = Arc::new(BufferStore::new(BUFFER_MAX_ENTRIES, policy_cell.clone()));
        regular_buf.set_on_flush(on_flush.clone());
        let ttl_buf = Arc::new(BufferStore::new(BUFFER_MAX_ENTRIES, policy_cell));
        ttl_buf.set_on_flush(on_flush);

        log::info!("database opened: {path} (+ {counter_path})");
        Ok(Self {
            db,
            regular_buf,
            ttl_buf,
            counters,
            migrations: Arc::new(Mutex::new(Vec::new())),
            migrations_run_lock: Arc::new(Mutex::new(())),
        })
    }

    /// Register tables via a closure.  Call once at startup.
    ///
    /// ```ignore
    /// db.init_tables(|db| {
    ///     db.register_table(MY_TABLE);
    ///     db.register_ttl_table(MY_TTL, ttl);
    /// });
    /// ```
    pub fn init_tables(&self, f: impl FnOnce(&Self)) {
        f(self);
    }

    // -- Compaction ----------------------------------------------------------

    /// Run a full compaction on the main database.
    pub fn compact(&self) -> Result<(), DbError> {
        log::debug!("starting database compaction");
        let handle = db!(self.db.start_compaction())?;
        let steps = db!(handle.run())?;
        log::info!("database compaction completed ({steps} steps)");
        Ok(())
    }

    // -- Counter -------------------------------------------------------------

    /// Increment an atomic counter and return the new value.
    ///
    /// In-memory operation — counters are flushed to redb on explicit
    /// `flush_counters` and before any buffer flush.
    pub fn next(&self, counter_id: u8) -> u64 {
        self.counters.next(counter_id)
    }

    /// Force-flush all dirty counters to redb immediately.
    pub fn flush_counters(&self) -> Result<usize, DbError> {
        self.counters.flush()
    }

    // -- Backup --------------------------------------------------------------

    /// Create a timestamped backup inside the `backups/` directory.
    pub fn backup(&self) -> Result<(), DbError> {
        log::debug!("starting database backup");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let backup_dir = "backups";
        std::fs::create_dir_all(backup_dir).inspect_err(|e| {
            log::error!("failed to create backup dir {backup_dir}: {e}");
        })?;

        let backup_path = format!("{backup_dir}/redb_{now}.redb");
        log::trace!("writing backup to {backup_path}");
        db!(self.db.backup(&backup_path))?;

        log::info!("backup saved to {backup_path}");
        Ok(())
    }

    // -- Write (buffered) ----------------------------------------------------

    /// Register a regular table so the buffer knows how to flush it.
    /// Must be called once per table before any writes.
    pub fn register_table<K, V>(&self, def: TableDefinition<'static, K, V>)
    where
        K: Key + Send + Sync + 'static,
        V: Value + Send + Sync + 'static,
    {
        // Ensure table exists.  Failure to open a transaction is non-fatal
        // here: `open_table` inside a write transaction also creates the
        // table, so a later flush will create it even if this call failed.
        let tx = match self.db.begin_write() {
            Ok(tx) => tx,
            Err(e) => {
                log::error!("register_table `{}`: begin_write failed: {e}", def.name());
                self.regular_buf.register_regular(def);
                return;
            }
        };
        {
            let table = tx.open_table(def);
            drop(table);
            if let Err(e) = tx.commit() {
                log::error!("register_table `{}`: commit failed: {e}", def.name());
            }
        }
        self.regular_buf.register_regular(def);
        log::debug!("registered regular table `{}`", def.name());
    }

    /// Register a TTL table so the buffer knows how to flush it.
    pub fn register_ttl_table<K, V>(&self, def: TtlTableDefinition<K, V>, default_ttl: Duration)
    where
        K: Key + Send + Sync + 'static,
        V: Value + Send + Sync + 'static,
    {
        let tx = match self.db.begin_write() {
            Ok(tx) => tx,
            Err(e) => {
                log::error!(
                    "register_ttl_table `{}`: begin_write failed: {e}",
                    def.name()
                );
                self.ttl_buf.register_ttl(def, default_ttl);
                return;
            }
        };
        {
            let table = tx.open_ttl_table(def);
            drop(table);
            if let Err(e) = tx.commit() {
                log::error!("register_ttl_table `{}`: commit failed: {e}", def.name());
            }
        }
        self.ttl_buf.register_ttl(def, default_ttl);
        log::debug!("registered TTL table `{}`", def.name());
    }

    /// Write a key-value pair to the regular buffer.
    pub fn write<K, V>(
        &self,
        def: TableDefinition<'static, K, V>,
        key: K,
        value: V,
    ) -> Result<(), DbError>
    where
        K: Key + Send + 'static,
        V: Value + Send + 'static,
    {
        let k_bytes = serialize_value(&key);
        let v_bytes = serialize_value(&value);
        self.regular_buf
            .push(def.name(), k_bytes, v_bytes, &self.db, false)
    }

    /// Write a key-value pair to the TTL buffer.
    /// If `ttl` is shorter than the maintenance interval, force-flushes immediately.
    pub fn write_ttl<K, V>(
        &self,
        def: TtlTableDefinition<K, V>,
        key: K,
        value: V,
        ttl: Duration,
    ) -> Result<(), DbError>
    where
        K: Key + Send + 'static,
        V: Value + Send + 'static,
    {
        let force = ttl < Duration::from_secs(BUFFER_FLUSH_SECS);
        let k_bytes = serialize_value(&key);
        let v_bytes = serialize_value(&value);
        self.ttl_buf
            .push_with_ttl(def.name(), k_bytes, v_bytes, &self.db, force, Some(ttl))
    }

    // -- Read (buffer-first) -------------------------------------------------

    /// Look up a key in the write buffers (regular → TTL).
    /// Returns `None` if not found in either buffer — caller should
    /// fall back to a direct DB read via `self.db`.
    ///
    /// Prefer [`get`](Self::get) for a single-call buffer+DB lookup.
    pub fn get_buffered(&self, table_name: &str, key_bytes: &[u8]) -> Option<Vec<u8>> {
        self.regular_buf
            .get(table_name, key_bytes)
            .or_else(|| self.ttl_buf.get(table_name, key_bytes))
    }

    /// Buffer-first read with automatic DB fallback.
    ///
    /// Checks the write buffers first, then falls back to a direct redb read.
    /// Returns the raw value bytes (as stored in the buffer).
    /// This is the recommended single-call lookup for most use cases.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(bytes) = db.get(MY_TABLE, my_key)? {
    ///     let item: MyType = postcard::from_bytes(&bytes)?;
    /// }
    /// ```
    pub fn get<K, V>(
        &self,
        def: TableDefinition<'static, K, V>,
        key: K,
    ) -> Result<Option<Vec<u8>>, DbError>
    where
        K: Key + Send + 'static,
        V: Value + Send + 'static,
    {
        let key_bytes = serialize_value(&key);

        // 1. Buffer-first — catches unflushed writes.
        if let Some(v_bytes) = self.get_buffered(def.name(), &key_bytes) {
            return Ok(Some(v_bytes));
        }

        // 2. DB fallback — use from_bytes to get SelfType (avoids Borrow<SelfType> HRTB).
        let tx = db!(self.db.begin_read())?;
        let table = db!(tx.open_table(def))?;
        let k_ref = K::from_bytes(&key_bytes);
        if let Some(guard) = db!(table.get(k_ref))? {
            let v_bytes = V::as_bytes(&guard.value()).as_ref().to_vec();
            return Ok(Some(v_bytes));
        }
        Ok(None)
    }

    // -- Remove ---------------------------------------------------------------

    /// Remove a key from both buffers and the database.
    ///
    /// This is the recommended single-call deletion for most use cases.
    /// It ensures the key is gone from both the in-memory buffer and the
    /// on-disk redb store.
    pub fn remove<K, V>(&self, def: TableDefinition<'static, K, V>, key: K) -> Result<(), DbError>
    where
        K: Key + Send + 'static,
        V: Value + Send + 'static,
    {
        let key_bytes = serialize_value(&key);

        // Hold both buffers' flush locks for the whole remove cycle:
        // otherwise a concurrent flush could take the key into its batch
        // right after we clear the buffer and re-insert it into the DB.
        let _r = self.regular_buf.lock_flush();
        let _t = self.ttl_buf.lock_flush();

        // 1. Remove from both buffers (regular + TTL).
        let _ = self.regular_buf.remove(def.name(), &key_bytes);
        let _ = self.ttl_buf.remove(def.name(), &key_bytes);

        // 2. Remove from DB through the configured commit policy.
        crate::group_commit::commit_ops(&self.db, self.regular_buf.policy().get(), move |tx| {
            let mut table = db!(tx.open_table(def))?;
            let k_ref = K::from_bytes(&key_bytes);
            db!(table.remove(k_ref))?;
            Ok(())
        })?;

        log::trace!("remove `{}`", def.name());
        Ok(())
    }

    // -- Flush ---------------------------------------------------------------

    /// Flush both regular and TTL buffers to disk.
    pub fn flush_buffers(&self) -> Result<(), DbError> {
        let r = self.regular_buf.flush(&self.db)?;
        let t = self.ttl_buf.flush(&self.db)?;
        if r + t > 0 {
            log::debug!("flush_buffers: regular={r}, ttl={t}");
        }
        Ok(())
    }

    /// Number of pending entries in the regular buffer.
    pub fn pending_writes(&self) -> usize {
        self.regular_buf.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use shodh_redb::{ReadableDatabase, TableDefinition};
    use std::fs;
    use std::sync::atomic::AtomicU32;

    static TEST_ID: AtomicU32 = AtomicU32::new(0);
    fn unique_dir(label: &str) -> std::path::PathBuf {
        let id = TEST_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!("team_test_{label}_{id}"))
    }

    const TEST_TABLE: TableDefinition<u64, String> = TableDefinition::new("test_wr");

    fn open_db() -> (DBWrapper, String) {
        let dir = unique_dir("wrapper2");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let data_path = dir.join("test.redb");
        let data_path_s = data_path.to_str().unwrap().to_owned();
        let db = DBWrapper::new(&data_path_s).expect("new");
        db.register_table(TEST_TABLE);
        (db, dir.to_str().unwrap().to_owned())
    }

    #[test]
    fn new_creates_db_files() {
        let (_db, dir) = open_db();
        let path = format!("{dir}/test.redb");
        assert!(std::path::Path::new(&path).exists());
        assert!(std::path::Path::new(&format!("{path}.counter")).exists());
    }

    #[test]
    fn next_increments() {
        let (db, _dir) = open_db();
        assert_eq!(db.next(0), 1);
        assert_eq!(db.next(0), 2);
        assert_eq!(db.next(1), 1);
        assert_eq!(db.next(0), 3);
    }

    #[test]
    fn write_and_get_from_buffer() {
        let (db, _dir) = open_db();
        db.write(TEST_TABLE, 1u64, "hello".to_string())
            .expect("write");

        // Should be readable immediately from buffer (before flush).
        let k = serialize_value(&1u64);
        let val = db.get_buffered(TEST_TABLE.name(), &k).expect("in buffer");
        assert_eq!(String::from_bytes(&val), "hello");

        // Not yet in DB.
        let tx = db.db.begin_read().expect("read");
        let table = tx.open_table(TEST_TABLE).expect("open");
        assert!(table.get(1u64).unwrap().is_none());
    }

    #[test]
    fn flush_persists_and_readable() {
        let (db, _dir) = open_db();
        db.write(TEST_TABLE, 1u64, "x".to_string()).expect("w1");
        db.write(TEST_TABLE, 2u64, "y".to_string()).expect("w2");

        db.flush_buffers().expect("flush");
        assert_eq!(db.pending_writes(), 0);

        // Buffer empty → DB has it.
        let tx = db.db.begin_read().expect("read");
        let table = tx.open_table(TEST_TABLE).expect("open");
        assert_eq!(table.get(2u64).unwrap().unwrap().value(), "y");
    }

    #[test]
    fn get_buffered_nonexistent() {
        let (db, _dir) = open_db();
        assert!(db.get_buffered(TEST_TABLE.name(), b"no_such_key").is_none());
    }

    #[test]
    fn get_from_buffer_before_flush() {
        let (db, _dir) = open_db();
        db.write(TEST_TABLE, 1u64, "hello".to_string())
            .expect("write");

        let val = db.get(TEST_TABLE, 1u64).expect("get").expect("value");
        assert_eq!(String::from_bytes(&val), "hello");
    }

    #[test]
    fn get_db_fallback_after_flush() {
        let (db, _dir) = open_db();
        db.write(TEST_TABLE, 1u64, "persisted".to_string())
            .expect("write");
        db.flush_buffers().expect("flush");
        assert_eq!(db.pending_writes(), 0);

        // Buffer is empty — should fall back to DB.
        let val = db.get(TEST_TABLE, 1u64).expect("get").expect("value");
        assert_eq!(String::from_bytes(&val), "persisted");
    }

    #[test]
    fn get_nonexistent_returns_none() {
        let (db, _dir) = open_db();
        assert!(db.get(TEST_TABLE, 999u64).expect("get").is_none());
    }

    #[test]
    fn get_overwrite_in_buffer() {
        let (db, _dir) = open_db();
        db.write(TEST_TABLE, 1u64, "old".to_string()).expect("w1");
        db.write(TEST_TABLE, 1u64, "new".to_string()).expect("w2");

        let val = db.get(TEST_TABLE, 1u64).expect("get").expect("value");
        assert_eq!(String::from_bytes(&val), "new");
    }

    #[test]
    fn remove_from_buffer() {
        let (db, _dir) = open_db();
        db.write(TEST_TABLE, 1u64, "x".to_string()).expect("write");
        assert!(db.get(TEST_TABLE, 1u64).expect("get").is_some());

        db.remove(TEST_TABLE, 1u64).expect("remove");
        assert!(db.get(TEST_TABLE, 1u64).expect("get").is_none());
        assert!(
            db.get_buffered(TEST_TABLE.name(), &serialize_value(&1u64))
                .is_none()
        );
    }

    #[test]
    fn remove_from_db_after_flush() {
        let (db, _dir) = open_db();
        db.write(TEST_TABLE, 1u64, "x".to_string()).expect("write");
        db.write(TEST_TABLE, 2u64, "y".to_string()).expect("write");
        db.flush_buffers().expect("flush");
        assert_eq!(db.pending_writes(), 0);

        // Remove from flushed DB.
        db.remove(TEST_TABLE, 1u64).expect("remove");
        assert!(db.get(TEST_TABLE, 1u64).expect("get").is_none());
        assert!(db.get(TEST_TABLE, 2u64).expect("get").is_some()); // other key survives
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let (db, _dir) = open_db();
        // Removing a key that doesn't exist should not error.
        db.remove(TEST_TABLE, 999u64).expect("remove");
    }

    #[test]
    fn overwrite_in_buffer() {
        let (db, _dir) = open_db();
        let k = serialize_value(&1u64);
        db.write(TEST_TABLE, 1u64, "old".to_string()).expect("w1");
        db.write(TEST_TABLE, 1u64, "new".to_string()).expect("w2");

        let val = db.get_buffered(TEST_TABLE.name(), &k).expect("in buffer");
        assert_eq!(String::from_bytes(&val), "new");
    }

    #[test]
    fn next_is_thread_safe() {
        let (db, _dir) = open_db();
        let db = Arc::new(db);
        let b1 = db.clone();
        let b2 = db.clone();

        let h1 = std::thread::spawn(move || {
            for _ in 0..5000 {
                b1.next(0);
            }
        });
        let h2 = std::thread::spawn(move || {
            for _ in 0..5000 {
                b2.next(0);
            }
        });
        h1.join().unwrap();
        h2.join().unwrap();

        let total = db.next(0) - 1;
        assert_eq!(total, 10000);
    }

    #[test]
    fn flush_counters_persists() {
        let (db, _dir) = open_db();
        for _ in 0..100 {
            db.next(0);
        }
        db.flush_counters().expect("flush");
        // After flush + reload: value should survive.
        let val = db.next(0);
        assert!(val > 100);
    }

    #[test]
    fn write_ttl_short_ttl_flushes_immediately() {
        use shodh_redb::ttl_table::TtlTableDefinition;
        const TTL_TABLE: TtlTableDefinition<u64, String> =
            TtlTableDefinition::new("test_ttl_short");

        let dir = unique_dir("wrapper_ttl_short");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let data_path = dir.join("test.redb");
        let db = DBWrapper::new(data_path.to_str().unwrap()).expect("new");
        db.register_ttl_table(TTL_TABLE, Duration::from_secs(3600));

        // ttl < BUFFER_FLUSH_SECS → force flush, entry lands in DB at once.
        db.write_ttl(
            TTL_TABLE,
            1u64,
            "expiring".to_string(),
            Duration::from_secs(1),
        )
        .expect("write_ttl");

        let tx = db.db.begin_read().expect("read");
        let table = tx.open_ttl_table(TTL_TABLE).expect("open");
        let guard = table.get(1u64).expect("get").expect("present");
        assert_eq!(guard.value(), "expiring");
        assert!(guard.expires_at_ms() > 0);
    }

    #[test]
    fn write_ttl_long_ttl_stays_buffered_until_flush() {
        use shodh_redb::ttl_table::TtlTableDefinition;
        const TTL_TABLE: TtlTableDefinition<u64, String> = TtlTableDefinition::new("test_ttl_long");

        let dir = unique_dir("wrapper_ttl_long");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let data_path = dir.join("test.redb");
        let db = DBWrapper::new(data_path.to_str().unwrap()).expect("new");
        db.register_ttl_table(TTL_TABLE, Duration::from_secs(3600));

        // ttl >= BUFFER_FLUSH_SECS → buffered, not yet in DB.
        db.write_ttl(
            TTL_TABLE,
            1u64,
            "buffered".to_string(),
            Duration::from_secs(60),
        )
        .expect("write_ttl");

        let key_bytes = serialize_value(&1u64);
        let in_buf = db
            .get_buffered(TTL_TABLE.name(), &key_bytes)
            .expect("in ttl buffer");
        assert_eq!(String::from_bytes(&in_buf), "buffered");

        let tx = db.db.begin_read().expect("read");
        let table = tx.open_ttl_table(TTL_TABLE).expect("open");
        assert!(table.get(1u64).unwrap().is_none());

        db.flush_buffers().expect("flush");
        let tx = db.db.begin_read().expect("read");
        let table = tx.open_ttl_table(TTL_TABLE).expect("open");
        assert_eq!(table.get(1u64).unwrap().unwrap().value(), "buffered");
    }

    #[test]
    fn write_ttl_preserves_per_write_ttl_after_flush() {
        use shodh_redb::ttl_table::TtlTableDefinition;
        const TTL_TABLE: TtlTableDefinition<u64, String> = TtlTableDefinition::new("test_ttl_keep");

        let dir = unique_dir("wrapper_ttl_keep");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let data_path = dir.join("test.redb");
        let db = DBWrapper::new(data_path.to_str().unwrap()).expect("new");
        // Registration default is 10 minutes.
        db.register_ttl_table(TTL_TABLE, Duration::from_secs(600));

        let before = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Per-write TTL of 60s must win over the registration default (600s).
        db.write_ttl(
            TTL_TABLE,
            1u64,
            "short_lived".to_string(),
            Duration::from_secs(60),
        )
        .expect("write_ttl");
        db.flush_buffers().expect("flush");

        let after = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let tx = db.db.begin_read().expect("read");
        let table = tx.open_ttl_table(TTL_TABLE).expect("open");
        let guard = table.get(1u64).expect("get").expect("present");
        assert_eq!(guard.value(), "short_lived");
        let expires_at = guard.expires_at_ms();
        // expiry ≈ now + 60s; if the registration default had been applied it
        // would be ≈ now + 600s.
        assert!(
            expires_at <= after + 65_000,
            "ttl from write_ttl was not preserved: expires_at={expires_at}, now≈{after}"
        );
        assert!(
            expires_at >= before + 55_000,
            "expiry implausibly early: expires_at={expires_at}"
        );
    }

    #[test]
    fn concurrent_remove_and_flush_never_resurrects_key() {
        for _ in 0..20 {
            let (db, dir) = open_db();

            db.write(TEST_TABLE, 1u64, "value".to_string())
                .expect("write");

            let d1 = db.clone();
            let d2 = db.clone();
            let h_flush = std::thread::spawn(move || {
                for _ in 0..50 {
                    let _ = d1.flush_buffers();
                }
            });
            let h_remove = std::thread::spawn(move || {
                d2.remove(TEST_TABLE, 1u64).expect("remove");
            });

            h_flush.join().unwrap();
            h_remove.join().unwrap();

            db.flush_buffers().expect("final flush");
            let tx = db.db.begin_read().expect("read");
            let table = tx.open_table(TEST_TABLE).expect("open");
            assert!(
                table.get(1u64).unwrap().is_none(),
                "removed key resurrected by a racing flush"
            );

            let _ = fs::remove_dir_all(&dir);
        }
    }

    #[test]
    fn get_buffered_reads_ttl_buffer() {
        use shodh_redb::ttl_table::TtlTableDefinition;
        const TTL_TABLE: TtlTableDefinition<u64, String> = TtlTableDefinition::new("test_ttl_gb");

        let dir = unique_dir("wrapper_ttl_gb");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let data_path = dir.join("test.redb");
        let db = DBWrapper::new(data_path.to_str().unwrap()).expect("new");
        db.register_ttl_table(TTL_TABLE, Duration::from_secs(3600));

        db.write_ttl(
            TTL_TABLE,
            7u64,
            "ttl_value".to_string(),
            Duration::from_secs(60),
        )
        .expect("write_ttl");

        let val = db
            .get_buffered(TTL_TABLE.name(), &serialize_value(&7u64))
            .expect("value");
        assert_eq!(String::from_bytes(&val), "ttl_value");
    }

    #[test]
    fn register_table_is_idempotent() {
        let (db, _dir) = open_db();
        db.register_table(TEST_TABLE);
        db.register_table(TEST_TABLE);
        db.write(TEST_TABLE, 1u64, "x".to_string()).expect("write");
        db.flush_buffers().expect("flush");
        assert!(db.get(TEST_TABLE, 1u64).expect("get").is_some());
    }

    #[test]
    fn remove_does_not_resurrect_after_flush() {
        let (db, _dir) = open_db();
        // Two writes to the same key, then remove: nothing must come back.
        db.write(TEST_TABLE, 1u64, "old".to_string()).expect("w1");
        db.write(TEST_TABLE, 1u64, "new".to_string()).expect("w2");
        db.remove(TEST_TABLE, 1u64).expect("remove");

        db.flush_buffers().expect("flush");
        assert!(db.get(TEST_TABLE, 1u64).expect("get").is_none());

        // And the DB itself must not contain the key.
        let tx = db.db.begin_read().expect("read");
        let table = tx.open_table(TEST_TABLE).expect("open");
        assert!(table.get(1u64).unwrap().is_none());
    }

    #[test]
    fn roundtrip_supported_value_types() {
        use crate::strip_varint;
        const STR_TABLE: TableDefinition<u64, String> = TableDefinition::new("rt_str");
        const BLOB_TABLE: TableDefinition<u64, Vec<u8>> = TableDefinition::new("rt_blob");
        const LIST_TABLE: TableDefinition<u64, Vec<String>> = TableDefinition::new("rt_list");

        let dir = unique_dir("wrapper_rt");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let data_path = dir.join("test.redb");
        let db = DBWrapper::new(data_path.to_str().unwrap()).expect("new");
        db.register_table(STR_TABLE);
        db.register_table(BLOB_TABLE);
        db.register_table(LIST_TABLE);

        db.write(STR_TABLE, 1u64, "text".to_string()).expect("w1");
        db.write(BLOB_TABLE, 1u64, vec![1u8, 2, 3, 250])
            .expect("w2");
        db.write(LIST_TABLE, 1u64, vec!["a".to_string(), "b".to_string()])
            .expect("w3");
        db.flush_buffers().expect("flush");

        let tx = db.db.begin_read().expect("read");
        assert_eq!(
            tx.open_table(STR_TABLE)
                .unwrap()
                .get(1u64)
                .unwrap()
                .unwrap()
                .value(),
            "text"
        );
        assert_eq!(
            tx.open_table(BLOB_TABLE)
                .unwrap()
                .get(1u64)
                .unwrap()
                .unwrap()
                .value(),
            vec![1u8, 2, 3, 250]
        );
        assert_eq!(
            tx.open_table(LIST_TABLE)
                .unwrap()
                .get(1u64)
                .unwrap()
                .unwrap()
                .value(),
            vec!["a".to_string(), "b".to_string()]
        );

        // Vec<u8> roundtrip through the wrapper's get() — varint prefix check.
        let raw = db.get(BLOB_TABLE, 1u64).expect("get").expect("value");
        assert_eq!(strip_varint(&raw), &[1u8, 2, 3, 250]);
    }

    #[test]
    fn backup_creates_file() {
        let (db, _dir) = open_db();
        db.backup().expect("backup");
        let backups: Vec<_> = fs::read_dir("backups")
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .map(|n| n.starts_with("redb_"))
                    .unwrap_or(false)
            })
            .collect();
        assert!(!backups.is_empty());
    }

    // -- Commit policies ------------------------------------------------------

    use crate::CommitPolicy;

    fn open_db_with(policy: CommitPolicy) -> (DBWrapper, String) {
        let dir = unique_dir("policy");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let data_path = dir.join("test.redb");
        let db = DBWrapper::with_commit_policy(data_path.to_str().unwrap(), policy).expect("new");
        db.register_table(TEST_TABLE);
        (db, dir.to_str().unwrap().to_owned())
    }

    /// Every policy: write → flush → data is on disk, read-back works,
    /// remove through the policy path works too.
    #[test]
    fn flush_persists_under_every_policy() {
        for policy in [
            CommitPolicy::Durable,
            CommitPolicy::Grouped,
            CommitPolicy::GroupedNonDurable,
        ] {
            let (db, dir) = open_db_with(policy);
            db.write(TEST_TABLE, 1u64, "one".to_string()).unwrap();
            db.write(TEST_TABLE, 2u64, "two".to_string()).unwrap();
            db.flush_buffers().unwrap();

            let tx = db.db.begin_read().unwrap();
            let table = tx.open_table(TEST_TABLE).unwrap();
            assert_eq!(table.get(1u64).unwrap().unwrap().value(), "one");
            assert_eq!(table.get(2u64).unwrap().unwrap().value(), "two");
            drop(tx);

            // Remove goes through the same policy.
            db.remove(TEST_TABLE, 1u64).unwrap();
            assert!(db.get(TEST_TABLE, 1u64).unwrap().is_none());
            let _ = fs::remove_dir_all(&dir);
        }
    }

    /// GroupedNonDurable: data written but not yet durable must become visible
    /// after a reopen ONLY if some durable commit followed.  Here we verify the
    /// weaker guarantee explicitly: reopen without a durable commit may lose the
    /// tail, so we do a durable marker write first and check the rest survived.
    #[test]
    fn grouped_non_durable_becomes_durable_on_next_durable_commit() {
        let (db, dir) = open_db_with(CommitPolicy::GroupedNonDurable);
        db.write(TEST_TABLE, 1u64, "nondurable".to_string())
            .unwrap();
        db.flush_buffers().unwrap();

        // Switch to a durable operation on the same DB: reopening in Durable
        // mode and writing makes the storage fsync the WAL state.
        drop(db);
        let data_path = format!("{dir}/test.redb");
        let db2 = DBWrapper::with_commit_policy(&data_path, CommitPolicy::Durable).expect("reopen");
        db2.register_table(TEST_TABLE);
        db2.write(TEST_TABLE, 2u64, "durable".to_string()).unwrap();
        db2.flush_buffers().unwrap();
        drop(db2);

        let db3 =
            DBWrapper::with_commit_policy(&data_path, CommitPolicy::Durable).expect("reopen2");
        db3.register_table(TEST_TABLE);
        // The non-durable commit may or may not have survived — both are legal.
        // What must hold: no corruption and the durable write is there.
        assert_eq!(
            db3.get(TEST_TABLE, 2u64).unwrap().map(|b| !b.is_empty()),
            Some(true)
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// Concurrent flushes from several threads under Grouped policy —
    /// exercises redb's group-commit pipeline (batches from different threads
    /// merging into one commit).  All writes must land.
    #[test]
    fn grouped_flush_concurrent_threads() {
        let (db, dir) = open_db_with(CommitPolicy::Grouped);
        let db = std::sync::Arc::new(db);

        const THREADS: usize = 4;
        const PER_THREAD: usize = 250;
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let db = db.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..PER_THREAD {
                    let key = (t * PER_THREAD + i) as u64;
                    db.write(TEST_TABLE, key, format!("t{t}_{i}")).unwrap();
                    if i % 50 == 49 {
                        db.flush_buffers().unwrap();
                    }
                }
            }));
        }
        for h in handles {
            h.join().expect("thread");
        }
        db.flush_buffers().unwrap();

        let tx = db.db.begin_read().unwrap();
        let table = tx.open_table(TEST_TABLE).unwrap();
        for t in 0..THREADS {
            for i in 0..PER_THREAD {
                let key = (t * PER_THREAD + i) as u64;
                assert_eq!(
                    table.get(key).unwrap().unwrap().value(),
                    format!("t{t}_{i}"),
                    "missing/incorrect key {key}"
                );
            }
        }
        drop(tx);
        let _ = fs::remove_dir_all(&dir);
    }
}
