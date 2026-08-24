//! Unified write buffers — single regular + single TTL, no per-table pools.
//!
//! All writes across all tables land in one of two buffers:
//! - **regular** — normal inserts, flushed together.
//! - **ttl**    — TTL inserts, force-flushed if expiry is sooner than next flush.
//!
//! Flushing is triggered manually, by the size threshold, or by a short TTL —
//! there is no background maintenance loop.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use shodh_redb::ttl_table::TtlTableDefinition;
use shodh_redb::{Database, Key, TableDefinition, TableHandle, Value};

use crate::DbError;
use crate::db;

// ---------------------------------------------------------------------------
// FlushTarget — type-erased flush handler for a single table
// ---------------------------------------------------------------------------

/// Knows how to flush a batch of raw (key_bytes, value_bytes) pairs
/// into a specific redb table.  One instance per registered table.
trait FlushTarget: Send + Sync {
    fn flush(
        &self,
        tx: &shodh_redb::WriteTransaction,
        entries: &HashMap<Vec<u8>, PendingValue>,
    ) -> Result<(), DbError>;
}

/// Concrete impl for a regular table.
struct RegularTarget<K: Key + 'static + Send + Sync, V: Value + 'static + Send + Sync> {
    def: TableDefinition<'static, K, V>,
}

impl<K: Key + 'static + Send + Sync, V: Value + 'static + Send + Sync> FlushTarget
    for RegularTarget<K, V>
{
    fn flush(
        &self,
        tx: &shodh_redb::WriteTransaction,
        entries: &HashMap<Vec<u8>, PendingValue>,
    ) -> Result<(), DbError> {
        let mut table = db!(tx.open_table(self.def))?;
        for (k_bytes, pending) in entries {
            let k = K::from_bytes(k_bytes);
            let v = V::from_bytes(&pending.value_bytes);
            db!(table.insert(k, v))?;
        }
        Ok(())
    }
}

/// Concrete impl for a TTL table.
struct TtlTarget<K: Key + 'static + Send + Sync, V: Value + 'static + Send + Sync> {
    def: TtlTableDefinition<K, V>,
    default_ttl: Duration,
}

impl<K: Key + 'static + Send + Sync, V: Value + 'static + Send + Sync> FlushTarget
    for TtlTarget<K, V>
{
    fn flush(
        &self,
        tx: &shodh_redb::WriteTransaction,
        entries: &HashMap<Vec<u8>, PendingValue>,
    ) -> Result<(), DbError> {
        let mut table = db!(tx.open_ttl_table(self.def))?;
        for (k_bytes, pending) in entries {
            let k = K::from_bytes(k_bytes);
            let v = V::from_bytes(&pending.value_bytes);
            let ttl = pending.ttl.unwrap_or(self.default_ttl);
            db!(table.insert_with_ttl(k, v, ttl))?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Serialisation helpers
// ---------------------------------------------------------------------------

/// Reinterprets a `&T` as `&T::SelfType<'_>`.
///
/// # Safety
///
/// `shodh_redb::Value::SelfType<'a>` is, by the crate's design, the same type
/// as `Self` with owned fields kept owned (e.g. `String` → `String`,
/// `Vec<T>` → `Vec<T::SelfType>`, tuples → tuples of `SelfType`s).  All
/// these have the same layout as the source type, so the pointer cast is
/// sound.  `K::from_bytes`/`V::from_bytes` are used on the flush side to
/// recover `SelfType` from the raw bytes.
unsafe fn as_self_type_ref<T: Value>(value: &T) -> &T::SelfType<'_> {
    unsafe { &*(value as *const T as *const T::SelfType<'_>) }
}

/// Serialise a `Value` to bytes.
pub fn serialize_value<T: Value>(value: &T) -> Vec<u8> {
    T::as_bytes(unsafe { as_self_type_ref(value) })
        .as_ref()
        .to_vec()
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
            if data.len() < 3 {
                return data;
            }
            3
        }
        255 => {
            if data.len() < 5 {
                return data;
            }
            5
        }
    };
    &data[consumed..]
}

// ---------------------------------------------------------------------------
// BufferStore — one buffer for regular, one for TTL
// ---------------------------------------------------------------------------

/// Callback run before every flush (e.g., to sync counters to disk).
type OnFlushCallback = Arc<dyn Fn() -> Result<(), DbError> + Send + Sync>;

/// Pending (key bytes → value bytes) pairs for a single table.
type TableEntries = HashMap<Vec<u8>, PendingValue>;

struct PendingValue {
    value_bytes: Vec<u8>,
    ttl: Option<Duration>,
}

/// A single buffer holding pending writes across all tables of one kind
/// (regular or TTL).  Flushed manually, by size threshold, or on short TTL.
///
/// Pending entries are stored as `table → (key → value)`.  Only the most
/// recent value per key is kept: repeated writes to the same key overwrite
/// the previous entry in place, so lookups are O(1) and removing a key
/// drops every occurrence of it.
pub(crate) struct BufferStore {
    /// Pending entries per table: table name → (key bytes → value bytes).
    entries: Mutex<HashMap<String, TableEntries>>,
    /// How to flush each table.  Populated by `register_*`.
    targets: Mutex<HashMap<String, Arc<dyn FlushTarget>>>,
    /// Max pending entries before auto-flush (0 = no limit).
    max_size: usize,
    /// Called right before any flush (manual or auto).
    /// Used to sync counters to disk alongside buffered data.
    on_flush: Mutex<Option<OnFlushCallback>>,
    /// Serializes flushes so older and newer batches cannot commit out of order.
    flush_lock: Mutex<()>,
}

impl BufferStore {
    pub(crate) fn new(max_size: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            targets: Mutex::new(HashMap::new()),
            max_size,
            on_flush: Mutex::new(None),
            flush_lock: Mutex::new(()),
        }
    }

    /// Set a callback invoked before every flush (e.g., to sync counters).
    pub(crate) fn set_on_flush(&self, cb: OnFlushCallback) {
        *self.on_flush.lock().unwrap() = Some(cb);
    }

    /// Acquire the flush serialization lock.  Held across a full
    /// remove-from-buffer-and-DB cycle so an in-flight flush cannot commit
    /// a batch containing a key that is being deleted.
    pub(crate) fn lock_flush(&self) -> std::sync::MutexGuard<'_, ()> {
        self.flush_lock.lock().unwrap()
    }

    /// Register a regular table for flushing.
    pub(crate) fn register_regular<
        K: Key + Send + Sync + 'static,
        V: Value + Send + Sync + 'static,
    >(
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
        self.push_with_ttl(table_name, key_bytes, value_bytes, db, force_flush, None)
    }

    pub(crate) fn push_with_ttl(
        &self,
        table_name: &str,
        key_bytes: Vec<u8>,
        value_bytes: Vec<u8>,
        db: &Database,
        force_flush: bool,
        ttl: Option<Duration>,
    ) -> Result<(), DbError> {
        let mut entries = self.entries.lock().unwrap();
        // Look up the table by `&str` first: no `String` allocation on the
        // hot path; a table name is only allocated the first time it appears.
        let map = match entries.get_mut(table_name) {
            Some(m) => m,
            None => entries.entry(table_name.to_owned()).or_default(),
        };
        map.insert(key_bytes, PendingValue { value_bytes, ttl });
        let len: usize = entries.values().map(HashMap::len).sum();
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
        self.entries
            .lock()
            .unwrap()
            .get(table_name)
            .and_then(|m| m.get(key_bytes))
            .map(|pending| pending.value_bytes.clone())
    }

    /// Remove a key from the buffer.  Drops every pending occurrence of the
    /// key (repeated writes collapse into one entry).  Returns `true` if an
    /// entry was removed.
    pub(crate) fn remove(&self, table_name: &str, key_bytes: &[u8]) -> bool {
        let mut entries = self.entries.lock().unwrap();
        if let Some(map) = entries.get_mut(table_name) {
            let existed = map.remove(key_bytes).is_some();
            if map.is_empty() {
                entries.remove(table_name);
            }
            return existed;
        }
        false
    }

    /// Number of pending entries (unique keys across all tables).
    pub(crate) fn len(&self) -> usize {
        self.entries
            .lock()
            .unwrap()
            .values()
            .map(HashMap::len)
            .sum()
    }

    /// Flush all pending entries to redb, grouped by table.
    /// Calls `on_flush` callback first (if set) to sync related state.
    ///
    /// If flushing fails part-way, all pending entries are put back into the
    /// buffer so no data is lost (entries that were already written are
    /// re-inserted idempotently on the next attempt).
    pub(crate) fn flush(&self, db: &Database) -> Result<usize, DbError> {
        let _flush_guard = self.flush_lock.lock().unwrap();
        // Sync related state (e.g., counters) before writing buffered data.
        // Clone the Arc so the mutex is not held during the callback.
        let on_flush = self.on_flush.lock().unwrap().clone();
        if let Some(cb) = on_flush {
            cb()?;
        }

        let mut entries = self.entries.lock().unwrap();
        if entries.is_empty() {
            return Ok(0);
        }
        let batch = std::mem::take(&mut *entries);
        drop(entries);

        let count: usize = batch.values().map(HashMap::len).sum();

        // Flush all tables in one write transaction.  redb serializes writers,
        // so opening one transaction is both faster and atomic across tables.
        let tx = db!(db.begin_write())?;
        let targets = self.targets.lock().unwrap();
        let mut failure = None;
        for (name, pairs) in &batch {
            let target = match targets.get(name) {
                Some(t) => t,
                None => {
                    log::error!("BufferStore::flush: no target registered for table `{name}`");
                    failure = Some(DbError::TableNotFound(name.clone()));
                    break;
                }
            };
            if let Err(e) = target.flush(&tx, pairs) {
                failure = Some(e);
                break;
            }
        }
        drop(targets);

        if let Some(e) = failure {
            drop(tx);
            let mut entries = self.entries.lock().unwrap();
            for (name, pairs) in batch {
                let map = entries.entry(name).or_default();
                for (k, v) in pairs {
                    map.entry(k).or_insert(v);
                }
            }
            return Err(e);
        }

        if let Err(e) = db!(tx.commit()) {
            let mut entries = self.entries.lock().unwrap();
            for (name, pairs) in batch {
                let map = entries.entry(name).or_default();
                for (k, v) in pairs {
                    map.entry(k).or_insert(v);
                }
            }
            return Err(e);
        }

        log::debug!(
            "BufferStore::flush: {count} entries across {} tables",
            batch.len()
        );
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
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

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
        store
            .push(
                TEST_TABLE.name(),
                k.clone(),
                serialize_value(&"hello".to_string()),
                &db,
                false,
            )
            .unwrap();

        // Should be visible in buffer immediately.
        let found = store.get(TEST_TABLE.name(), &k).unwrap();
        let val: String = String::from_bytes(&found);
        assert_eq!(val, "hello");
    }

    #[test]
    fn flush_persists_to_db() {
        let (db, store, _dir) = setup();
        store
            .push(
                TEST_TABLE.name(),
                serialize_value(&1u64),
                serialize_value(&"x".to_string()),
                &db,
                false,
            )
            .unwrap();
        store
            .push(
                TEST_TABLE.name(),
                serialize_value(&2u64),
                serialize_value(&"y".to_string()),
                &db,
                false,
            )
            .unwrap();

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

        store
            .push(
                TEST_TABLE.name(),
                serialize_value(&1u64),
                serialize_value(&"a".to_string()),
                &db,
                false,
            )
            .unwrap();
        store
            .push(
                TEST_TABLE.name(),
                serialize_value(&2u64),
                serialize_value(&"b".to_string()),
                &db,
                false,
            )
            .unwrap();
        assert_eq!(store.len(), 2);

        // Third push → auto-flush.
        store
            .push(
                TEST_TABLE.name(),
                serialize_value(&3u64),
                serialize_value(&"c".to_string()),
                &db,
                false,
            )
            .unwrap();
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

        store
            .push(
                TEST_TABLE.name(),
                k.clone(),
                serialize_value(&"old".to_string()),
                &db,
                false,
            )
            .unwrap();
        store
            .push(
                TEST_TABLE.name(),
                k.clone(),
                serialize_value(&"new".to_string()),
                &db,
                false,
            )
            .unwrap();

        let found = store.get(TEST_TABLE.name(), &k).unwrap();
        assert_eq!(String::from_bytes(&found), "new");
    }

    #[test]
    fn overwrite_collapses_to_single_entry() {
        let (db, store, _dir) = setup();
        let k = serialize_value(&7u64);
        store
            .push(
                TEST_TABLE.name(),
                k.clone(),
                serialize_value(&"a".to_string()),
                &db,
                false,
            )
            .unwrap();
        store
            .push(
                TEST_TABLE.name(),
                k.clone(),
                serialize_value(&"b".to_string()),
                &db,
                false,
            )
            .unwrap();

        // Overwrite must not grow the buffer (no duplicate entries).
        assert_eq!(store.len(), 1);
        let found = store.get(TEST_TABLE.name(), &k).unwrap();
        assert_eq!(String::from_bytes(&found), "b");
    }

    #[test]
    fn remove_from_buffer() {
        let (db, store, _dir) = setup();
        let k = serialize_value(&1u64);
        store
            .push(
                TEST_TABLE.name(),
                k.clone(),
                serialize_value(&"x".to_string()),
                &db,
                false,
            )
            .unwrap();
        assert!(store.get(TEST_TABLE.name(), &k).is_some());

        assert!(store.remove(TEST_TABLE.name(), &k));
        assert!(store.get(TEST_TABLE.name(), &k).is_none());
    }

    #[test]
    fn remove_drops_all_occurrences() {
        let (db, store, _dir) = setup();
        let k = serialize_value(&11u64);
        store
            .push(
                TEST_TABLE.name(),
                k.clone(),
                serialize_value(&"old".to_string()),
                &db,
                false,
            )
            .unwrap();
        store
            .push(
                TEST_TABLE.name(),
                k.clone(),
                serialize_value(&"new".to_string()),
                &db,
                false,
            )
            .unwrap();

        assert!(store.remove(TEST_TABLE.name(), &k));
        assert!(store.get(TEST_TABLE.name(), &k).is_none());
        assert_eq!(store.len(), 0);

        // A later flush must not resurrect the removed key.
        let n = store.flush(&db).expect("flush");
        assert_eq!(n, 0);
    }

    #[test]
    fn remove_nonexistent_returns_false() {
        let (_db, store, _dir) = setup();
        assert!(!store.remove("no_table", b"no_key"));
    }

    #[test]
    fn flush_empty_returns_zero() {
        let (db, store, _dir) = setup();
        assert_eq!(store.flush(&db).expect("flush"), 0);
    }

    #[test]
    fn flush_error_keeps_entries() {
        let (db, store, _dir) = setup();
        let k = serialize_value(&1u64);
        let v = serialize_value(&"value".to_string());
        store
            .push("unregistered_table", k.clone(), v.clone(), &db, false)
            .unwrap();

        let err = store.flush(&db).unwrap_err();
        assert!(matches!(err, DbError::TableNotFound(_)));

        // The failed batch is still in the buffer.
        assert_eq!(store.len(), 1);
        assert_eq!(store.get("unregistered_table", &k).unwrap(), v);
    }

    #[test]
    fn flush_partial_failure_keeps_all_entries() {
        const GOOD: TableDefinition<u64, String> = TableDefinition::new("good_table");
        let dir = unique_dir("buf_partial");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.redb");
        let path_s = path.to_str().unwrap().to_owned();
        let db = Arc::new(Database::create(&path_s).expect("create"));
        {
            let tx = db.begin_write().expect("write");
            tx.open_table(GOOD).expect("open");
            tx.commit().expect("commit");
        }
        let store = BufferStore::new(100);
        store.register_regular(GOOD);

        let k1 = serialize_value(&1u64);
        let k2 = serialize_value(&2u64);
        let v1 = serialize_value(&"a".to_string());
        let v2 = serialize_value(&"b".to_string());
        store
            .push(GOOD.name(), k1.clone(), v1.clone(), &db, false)
            .unwrap();
        store
            .push("missing_table", k2.clone(), v2.clone(), &db, false)
            .unwrap();

        let err = store.flush(&db).unwrap_err();
        assert!(matches!(err, DbError::TableNotFound(_)));

        // Both the registered and the unregistered table's entries survive.
        assert_eq!(store.len(), 2);
        assert_eq!(store.get(GOOD.name(), &k1).unwrap(), v1);
        assert_eq!(store.get("missing_table", &k2).unwrap(), v2);
    }

    #[test]
    fn on_flush_callback_runs() {
        let (db, store, _dir) = setup();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls2 = calls.clone();
        store.set_on_flush(Arc::new(move || {
            calls2.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }));

        store
            .push(
                TEST_TABLE.name(),
                serialize_value(&1u64),
                serialize_value(&"x".to_string()),
                &db,
                false,
            )
            .unwrap();
        store.flush(&db).expect("flush");
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        // Flushing an empty buffer still runs the callback (counter sync).
        store.flush(&db).expect("flush");
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn concurrent_push_and_flush_no_loss() {
        let (db, store, _dir) = setup();
        let store = Arc::new(store);
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let s = store.clone();
            let d = db.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..500u64 {
                    let key = t * 1000 + i;
                    s.push(
                        TEST_TABLE.name(),
                        serialize_value(&key),
                        serialize_value(&format!("t{t}_{i}")),
                        &d,
                        false,
                    )
                    .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        store.flush(&db).expect("flush");
        assert_eq!(store.len(), 0);

        let tx = db.begin_read().expect("read");
        let table = tx.open_table(TEST_TABLE).expect("open");
        for t in 0..8u64 {
            for i in 0..500u64 {
                let key = t * 1000 + i;
                assert_eq!(
                    table.get(key).unwrap().unwrap().value(),
                    format!("t{t}_{i}"),
                    "missing/incorrect key {key}"
                );
            }
        }
    }

    // -- strip_varint -------------------------------------------------------

    #[test]
    fn strip_varint_empty() {
        let empty: &[u8] = &[];
        assert_eq!(strip_varint(empty), empty);
    }

    #[test]
    fn strip_varint_single_byte_prefix() {
        assert_eq!(strip_varint(&[5, 1, 2, 3]), &[1u8, 2, 3]);
        assert_eq!(strip_varint(&[0]), &[] as &[u8]);
    }

    #[test]
    fn strip_varint_two_byte_prefix() {
        let data = [254u8, 0x34, 0x12, 9, 9];
        assert_eq!(strip_varint(&data), &[9u8, 9]);
    }

    #[test]
    fn strip_varint_four_byte_prefix() {
        let data = [255u8, 1, 0, 0, 0, 7, 8];
        assert_eq!(strip_varint(&data), &[7u8, 8]);
    }

    #[test]
    fn strip_varint_truncated_returns_input() {
        assert_eq!(strip_varint(&[254, 1]), &[254u8, 1]);
        assert_eq!(strip_varint(&[255, 1, 2]), &[255u8, 1, 2]);
        assert_eq!(strip_varint(&[255, 1, 2, 3]), &[255u8, 1, 2, 3]);
    }
}
