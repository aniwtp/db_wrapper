//! DBWrapper — database handle: open, compact, backup, buffers, maintenance.

use std::sync::Arc;
use std::time::Duration;

use shodh_redb::ttl_table::TtlTableDefinition;
use shodh_redb::{Database, Key, ReadableDatabase, TableDefinition, TableHandle, Value};

use crate::buffer::{BufferStore, serialize_value};
use crate::counter::{
    CounterStore, COUNTER_BLOB, BUFFER_FLUSH_SECS, BUFFER_MAX_ENTRIES,
};
use crate::DbError;
use crate::db;

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
}

impl Clone for DBWrapper {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            regular_buf: self.regular_buf.clone(),
            ttl_buf: self.ttl_buf.clone(),
            counters: self.counters.clone(),
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
        log::debug!("DBWrapper::new opening database at {path}");

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

        // Default buffer sizes: 10_000 entries before auto-flush.
        // Both buffers flush counters first (for approximate sync).
        let on_flush: Arc<dyn Fn() + Send + Sync> = {
            let c = counters.clone();
            Arc::new(move || { let _ = c.flush(); })
        };
        let regular_buf = Arc::new(BufferStore::new(BUFFER_MAX_ENTRIES));
        regular_buf.set_on_flush(on_flush.clone());
        let ttl_buf = Arc::new(BufferStore::new(BUFFER_MAX_ENTRIES));
        ttl_buf.set_on_flush(on_flush);

        log::info!("database opened: {path} (+ {counter_path})");
        Ok(Self { db, regular_buf, ttl_buf, counters })
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
    /// In-memory operation — counters are flushed to redb periodically
    /// by the maintenance loop (every 5 min by default).
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
        // Ensure table exists.
        if let Ok(tx) = self.db.begin_write() {
            let _ = tx.open_table(def);
            let _ = tx.commit();
        }
        self.regular_buf.register_regular(def);
        log::debug!("registered regular table `{}`", def.name());
    }

    /// Register a TTL table so the buffer knows how to flush it.
    pub fn register_ttl_table<K, V>(
        &self,
        def: TtlTableDefinition<K, V>,
        default_ttl: Duration,
    ) where
        K: Key + Send + Sync + 'static,
        V: Value + Send + Sync + 'static,
    {
        if let Ok(tx) = self.db.begin_write() {
            let _ = tx.open_ttl_table(def);
            let _ = tx.commit();
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
        self.regular_buf.push(def.name(), k_bytes, v_bytes, &self.db, false)
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
        self.ttl_buf.push(def.name(), k_bytes, v_bytes, &self.db, force)
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
        db.write(TEST_TABLE, 1u64, "hello".to_string()).expect("write");

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
        db.write(TEST_TABLE, 1u64, "hello".to_string()).expect("write");

        let val = db.get(TEST_TABLE, 1u64).expect("get").expect("value");
        assert_eq!(String::from_bytes(&val), "hello");
    }

    #[test]
    fn get_db_fallback_after_flush() {
        let (db, _dir) = open_db();
        db.write(TEST_TABLE, 1u64, "persisted".to_string()).expect("write");
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
            for _ in 0..5000 { b1.next(0); }
        });
        let h2 = std::thread::spawn(move || {
            for _ in 0..5000 { b2.next(0); }
        });
        h1.join().unwrap();
        h2.join().unwrap();

        let total = db.next(0) - 1;
        assert_eq!(total, 10000);
    }

    #[test]
    fn flush_counters_persists() {
        let (db, _dir) = open_db();
        for _ in 0..100 { db.next(0); }
        db.flush_counters().expect("flush");
        // After flush + reload: value should survive.
        let val = db.next(0);
        assert!(val > 100);
    }

    #[test]
    fn backup_creates_file() {
        let (db, _dir) = open_db();
        db.backup().expect("backup");
        let backups: Vec<_> = fs::read_dir("backups").unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_str().map(|n| n.starts_with("redb_")).unwrap_or(false))
            .collect();
        assert!(!backups.is_empty());
    }
}
