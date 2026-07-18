//! Unified write buffers — single regular + single TTL, no per-table pools.
//!
//! All writes across all tables land in one of two buffers:
//! - **regular** — normal inserts, flushed together.
//! - **ttl**    — TTL inserts, force-flushed if expiry is sooner than next flush.
//!
//! A single maintenance loop (in `wrapper.rs`) handles all flushing —
//! no per-table background tasks.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use shodh_redb::ttl_table::TtlTableDefinition;
use shodh_redb::{Database, Key,  TableDefinition, TableHandle, Value};

use crate::DbError;
use crate::db;

// ---------------------------------------------------------------------------
// FlushTarget — type-erased flush handler for a single table
// ---------------------------------------------------------------------------

/// Knows how to flush a batch of raw (key_bytes, value_bytes) pairs
/// into a specific redb table.  One instance per registered table.
trait FlushTarget: Send + Sync {
    // fn name(&self) -> &str;
    fn flush(
        &self,
        db: &Database,
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), DbError>;
}

/// Concrete impl for a regular table.
struct RegularTarget<K: Key + 'static + Send + Sync, V: Value + 'static + Send + Sync> {
    def: TableDefinition<'static, K, V>,
}

impl<K: Key + 'static + Send + Sync, V: Value + 'static + Send + Sync> FlushTarget for RegularTarget<K, V> {
    // fn name(&self) -> &str { self.def.name() }
    fn flush(&self, db: &Database, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(), DbError> {
        let tx = db!(db.begin_write())?;
        {
            let mut table = db!(tx.open_table(self.def))?;
            for (k_bytes, v_bytes) in entries {
                let k = K::from_bytes(k_bytes);
                let v = V::from_bytes(v_bytes);
                db!(table.insert(k, v))?;
            }
        }
        db!(tx.commit())?;
        Ok(())
    }
}

/// Concrete impl for a TTL table.
struct TtlTarget<K: Key + 'static + Send + Sync, V: Value + 'static + Send + Sync> {
    def: TtlTableDefinition<K, V>,
    default_ttl: Duration,
}

impl<K: Key + 'static + Send + Sync, V: Value + 'static + Send + Sync> FlushTarget for TtlTarget<K, V> {
    // fn name(&self) -> &str { self.def.name() }
    fn flush(&self, db: &Database, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(), DbError> {
        let tx = db!(db.begin_write())?;
        {
            let mut table = db!(tx.open_ttl_table(self.def))?;
            for (k_bytes, v_bytes) in entries {
                let k = K::from_bytes(k_bytes);
                let v = V::from_bytes(v_bytes);
                db!(table.insert_with_ttl(k, v, self.default_ttl))?;
            }
        }
        db!(tx.commit())?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Serialisation helpers
// ---------------------------------------------------------------------------

/// Reinterprets a `&T` as `&T::SelfType<'_>`.
unsafe fn as_self_type_ref<T: Value>(value: &T) -> &T::SelfType<'_> {
    unsafe { &*(value as *const T as *const T::SelfType<'_>) }
}

/// Serialise a `Value` to bytes.
pub fn serialize_value<T: Value>(value: &T) -> Vec<u8> {
    T::as_bytes(unsafe { as_self_type_ref(value) }).as_ref().to_vec()
}

/// Strip a single varint length prefix from the beginning of a byte slice.
///
/// Redb's [`Value`] impl for `Vec<T>` prepends a varint-encoded element count.
/// Use this to recover the raw inner bytes when storing blob data as `Vec<u8>`.
///
/// Returns the bytes after the varint prefix.  Returns the original slice
/// unchanged if the data is too short to contain a valid varint.
///
/// # Example
///
/// ```ignore
/// // TableDefinition<u64, Vec<u8>> stores with a varint prefix.
/// let raw = db.get(MY_TABLE, key)?.map(|b| strip_varint(&b));
/// let item: MyType = postcard::from_bytes(raw.unwrap())?;
/// ```
pub fn strip_varint(data: &[u8]) -> &[u8] {
    if data.is_empty() {
        return data;
    }
    let consumed = match data[0] {
        0..=253 => 1,
        254 => {
            if data.len() < 3 { return data; }
            3
        }
        255 => {
            if data.len() < 5 { return data; }
            5
        }
    };
    &data[consumed..]
}

// ---------------------------------------------------------------------------
// BufferStore — one buffer for regular, one for TTL
// ---------------------------------------------------------------------------

/// A single buffer holding pending writes across all tables of one kind
/// (regular or TTL).  Flushed by the maintenance loop.
pub(crate) struct BufferStore {
    /// Pending entries: (table_name, key_bytes, value_bytes).
    entries: Mutex<Vec<BufferEntry>>,
    /// How to flush each table.  Populated lazily on first write.
    targets: Mutex<HashMap<String, Arc<dyn FlushTarget>>>,
    /// Max pending entries before auto-flush (0 = no limit).
    max_size: usize,
    /// Called right before any flush (manual or auto).
    /// Used to sync counters to disk alongside buffered data.
    on_flush: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

struct BufferEntry {
    table_name: String,
    key_bytes: Vec<u8>,
    value_bytes: Vec<u8>,
}

impl BufferStore {
    pub(crate) fn new(max_size: usize) -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
            targets: Mutex::new(HashMap::new()),
            max_size,
            on_flush: Mutex::new(None),
        }
    }

    /// Set a callback invoked before every flush (e.g., to sync counters).
    pub(crate) fn set_on_flush(&self, cb: Arc<dyn Fn() + Send + Sync>) {
        *self.on_flush.lock().unwrap() = Some(cb);
    }

    /// Register a regular table for flushing.
    pub(crate) fn register_regular<K: Key + Send + Sync + 'static, V: Value + Send + Sync + 'static>(
        &self,
        def: TableDefinition<'static, K, V>,
    ) {
        let name = def.name().to_owned();
        let target: Arc<dyn FlushTarget> = Arc::new(RegularTarget { def });
        self.targets.lock().unwrap().insert(name, target);
    }

    /// Register a TTL table for flushing.
    pub(crate) fn register_ttl<K: Key + Send + Sync + 'static, V: Value + Send + Sync + 'static>(
        &self,
        def: TtlTableDefinition<K, V>,
        default_ttl: Duration,
    ) {
        let name = def.name().to_owned();
        let target: Arc<dyn FlushTarget> = Arc::new(TtlTarget { def, default_ttl });
        self.targets.lock().unwrap().insert(name, target);
    }

    /// Push a serialised key-value pair.  Auto-flushes if max_size is reached.
    pub(crate) fn push(
        &self,
        table_name: &str,
        key_bytes: Vec<u8>,
        value_bytes: Vec<u8>,
        db: &Database,
        force_flush: bool,
    ) -> Result<(), DbError> {
        let mut entries = self.entries.lock().unwrap();
        entries.push(BufferEntry {
            table_name: table_name.to_owned(),
            key_bytes,
            value_bytes,
        });
        let len = entries.len();
        let should_flush = force_flush || (self.max_size > 0 && len >= self.max_size);
        drop(entries);

        log::trace!("BufferStore::push → `{table_name}` (pending: {len})");

        if should_flush {
            log::debug!("BufferStore auto-flush (len={len}, max={})", self.max_size);
            self.flush(db)?;
        }
        Ok(())
    }

    /// Look up the most recent value for a key in the buffer.
    /// Returns `None` if not found (caller should fall back to DB).
    pub(crate) fn get(&self, table_name: &str, key_bytes: &[u8]) -> Option<Vec<u8>> {
        let entries = self.entries.lock().unwrap();
        entries
            .iter()
            .rev() // newest first
            .find(|e| e.table_name == table_name && e.key_bytes == key_bytes)
            .map(|e| e.value_bytes.clone())
    }

    /// Remove an entry from the buffer by table name and key bytes.
    /// Returns `true` if an entry was removed.
    pub(crate) fn remove(&self, table_name: &str, key_bytes: &[u8]) -> bool {
        let mut entries = self.entries.lock().unwrap();
        if let Some(pos) = entries
            .iter()
            .rev()
            .position(|e| e.table_name == table_name && e.key_bytes == key_bytes)
        {
            let idx = entries.len() - 1 - pos;
            entries.remove(idx);
            true
        } else {
            false
        }
    }

    /// Number of pending entries.
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    /// Flush all pending entries to redb, grouped by table.
    /// Calls `on_flush` callback first (if set) to sync related state.
    pub(crate) fn flush(&self, db: &Database) -> Result<usize, DbError> {
        // Sync related state (e.g., counters) before writing buffered data.
        if let Some(cb) = self.on_flush.lock().unwrap().as_ref() {
            cb();
        }
        let mut entries = self.entries.lock().unwrap();
        if entries.is_empty() {
            return Ok(0);
        }
        let batch: Vec<BufferEntry> = std::mem::take(&mut *entries);
        drop(entries);

        let count = batch.len();

        // Group by table name.
        let mut by_table: HashMap<String, Vec<(Vec<u8>, Vec<u8>)>> = HashMap::new();
        for e in batch {
            by_table
                .entry(e.table_name)
                .or_default()
                .push((e.key_bytes, e.value_bytes));
        }

        // Flush each table via its registered target.
        let targets = self.targets.lock().unwrap();
        for (name, pairs) in &by_table {
            let target = targets.get(name).ok_or_else(|| {
                log::error!("BufferStore::flush: no target registered for table `{name}`");
                DbError::TableNotFound(name.clone())
            })?;
            target.flush(db, pairs)?;
        }

        log::debug!("BufferStore::flush: {count} entries across {} tables", by_table.len());
        Ok(count)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use shodh_redb::ReadableDatabase;
    use std::fs;
    use std::sync::atomic::AtomicU32;

    static TEST_ID: AtomicU32 = AtomicU32::new(0);
    fn unique_dir(label: &str) -> std::path::PathBuf {
        let id = TEST_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!("team_test_{label}_{id}"))
    }

    const TEST_TABLE: TableDefinition<u64, String> = TableDefinition::new("test_buf");

    fn setup() -> (Arc<Database>, BufferStore, String) {
        let dir = unique_dir("bufstore");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.redb");
        let path_s = path.to_str().unwrap().to_owned();
        let db = Arc::new(Database::create(&path_s).expect("create"));
        let tx = db.begin_write().expect("write");
        tx.open_table(TEST_TABLE).expect("open");
        tx.commit().expect("commit");

        let store = BufferStore::new(100);
        store.register_regular(TEST_TABLE);
        (db, store, dir.to_str().unwrap().to_owned())
    }

    #[test]
    fn push_and_get_from_buffer() {
        let (db, store, _dir) = setup();
        let k = serialize_value(&42u64);
        store.push(TEST_TABLE.name(), k.clone(), serialize_value(&"hello".to_string()), &db, false).unwrap();

        // Should be visible in buffer immediately.
        let found = store.get(TEST_TABLE.name(), &k).unwrap();
        let val: String = String::from_bytes(&found);
        assert_eq!(val, "hello");
    }

    #[test]
    fn flush_persists_to_db() {
        let (db, store, _dir) = setup();
        store.push(TEST_TABLE.name(), serialize_value(&1u64), serialize_value(&"x".to_string()), &db, false).unwrap();
        store.push(TEST_TABLE.name(), serialize_value(&2u64), serialize_value(&"y".to_string()), &db, false).unwrap();

        let n = store.flush(&db).expect("flush");
        assert_eq!(n, 2);

        // Read from DB.
        let tx = db.begin_read().expect("read");
        let table = tx.open_table(TEST_TABLE).expect("open");
        assert_eq!(table.get(1u64).unwrap().unwrap().value(), "x");
        assert_eq!(table.get(2u64).unwrap().unwrap().value(), "y");
    }

    #[test]
    fn auto_flush_on_max_size() {
        let (db, _store, _dir) = setup();
        // Override max_size.
        let store = BufferStore::new(3);
        store.register_regular(TEST_TABLE);

        store.push(TEST_TABLE.name(), serialize_value(&1u64), serialize_value(&"a".to_string()), &db, false).unwrap();
        store.push(TEST_TABLE.name(), serialize_value(&2u64), serialize_value(&"b".to_string()), &db, false).unwrap();
        assert_eq!(store.len(), 2);

        // Third push → auto-flush.
        store.push(TEST_TABLE.name(), serialize_value(&3u64), serialize_value(&"c".to_string()), &db, false).unwrap();
        assert_eq!(store.len(), 0);

        let tx = db.begin_read().expect("read");
        let table = tx.open_table(TEST_TABLE).expect("open");
        assert_eq!(table.get(3u64).unwrap().unwrap().value(), "c");
    }

    #[test]
    fn get_not_found_returns_none() {
        let (_db, store, _dir) = setup();
        assert!(store.get("no_such_table", b"key").is_none());
    }

    #[test]
    fn get_returns_newest_entry() {
        let (db, store, _dir) = setup();
        let k = serialize_value(&10u64);

        store.push(TEST_TABLE.name(), k.clone(), serialize_value(&"old".to_string()), &db, false).unwrap();
        store.push(TEST_TABLE.name(), k.clone(), serialize_value(&"new".to_string()), &db, false).unwrap();

        let found = store.get(TEST_TABLE.name(), &k).unwrap();
        assert_eq!(String::from_bytes(&found), "new");
    }

    #[test]
    fn remove_from_buffer() {
        let (db, store, _dir) = setup();
        let k = serialize_value(&1u64);
        store.push(TEST_TABLE.name(), k.clone(), serialize_value(&"x".to_string()), &db, false).unwrap();
        assert!(store.get(TEST_TABLE.name(), &k).is_some());

        assert!(store.remove(TEST_TABLE.name(), &k));
        assert!(store.get(TEST_TABLE.name(), &k).is_none());
    }

    #[test]
    fn remove_nonexistent_returns_false() {
        let (_db, store, _dir) = setup();
        assert!(!store.remove("no_table", b"no_key"));
    }
}
