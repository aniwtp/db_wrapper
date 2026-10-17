//! Unified write buffers — single regular + single TTL, no per-table pools.
//!
//! All writes across all tables land in one of two buffers:
//! - **regular** — normal inserts, flushed together.
//! - **ttl**    — TTL inserts, force-flushed if expiry is sooner than next flush.
//!
//! Flushing is triggered manually, by the size threshold, or by a short TTL —
//! there is no background maintenance loop.

use std::sync::Arc;

use parking_lot::Mutex;
use std::time::{Duration, Instant};

use hashbrown::HashMap;
use shodh_redb::ttl_table::TtlTableDefinition;
use shodh_redb::{Database, Key, TableDefinition, TableHandle, Value};

use crate::DbError;
use crate::db;
use crate::group_commit::CommitPolicyCell;

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

/// Pending entries of one table in ascending order of the table's key comparator.
fn sorted_by_key<K: Key>(
    entries: &HashMap<Vec<u8>, PendingValue>,
) -> Vec<(&Vec<u8>, &PendingValue)> {
    let mut items: Vec<_> = entries.iter().collect();
    items.sort_unstable_by(|a, b| K::compare(a.0, b.0));
    items
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
        for (k_bytes, pending) in sorted_by_key::<K>(entries) {
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
        let now = Instant::now();
        for (k_bytes, pending) in sorted_by_key::<K>(entries) {
            let k = K::from_bytes(k_bytes);
            let v = V::from_bytes(&pending.value_bytes);
            // The TTL runs from the moment of the write, not from the flush:
            // remaining = deadline - now (zero → already expired, inserted
            // with a zero TTL so it still supersedes any older stored value).
            let ttl = match pending.deadline {
                Some(deadline) => deadline.saturating_duration_since(now),
                None => self.default_ttl,
            };
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
pub(crate) unsafe fn as_self_type_ref<T: Value>(value: &T) -> &T::SelfType<'_> {
    unsafe { &*(value as *const T as *const T::SelfType<'_>) }
}

/// Borrow the serialised form of a `Value` without copying it.
pub(crate) fn serialized_ref<T: Value>(value: &T) -> T::AsBytes<'_> {
    T::as_bytes(unsafe { as_self_type_ref(value) })
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
/// The prefix is only stripped when the declared element count actually matches
/// the remaining byte count.  This matters for `Vec<u8>` blobs: blob payloads
/// themselves frequently start with a varint-looking byte (e.g. postcard LEB128
/// timestamps), and a blind prefix strip would silently corrupt such payloads.
/// If the declared length doesn't match — or the data is too short to contain
/// a valid varint — the original slice is returned unchanged.
///
/// # Example
///
/// ```ignore
/// // TableDefinition<u64, Vec<u8>> stores with a varint prefix.
/// let raw = db.get(MY_TABLE, key)?.map(|b| strip_varint(&b));
/// let item: MyType = postcard::from_bytes(raw.unwrap())?;
/// ```
pub fn strip_varint(data: &[u8]) -> &[u8] {
    fn decode(data: &[u8]) -> Option<(usize, usize)> {
        match data[0] {
            n @ 0..=253 => Some((n as usize, 1)),
            254 if data.len() >= 3 => Some((u16::from_le_bytes([data[1], data[2]]) as usize, 3)),
            255 if data.len() >= 5 => Some((
                u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize,
                5,
            )),
            _ => None,
        }
    }

    if data.is_empty() {
        return data;
    }
    match decode(data) {
        // Exact fit: this really is a redb Vec<T> prefix — strip it.
        Some((len, hdr)) if data.len() - hdr == len => &data[hdr..],
        // Declared length doesn't match the payload: not a prefix (or a corrupt
        // one) — either way stripping would corrupt the data.
        _ => data,
    }
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
    /// Moment the entry's TTL runs out (`write time + ttl`); `None` = the
    /// table's default TTL applies at flush time.
    deadline: Option<Instant>,
}

/// Pending entries of all tables, keyed by table name.
type TableMap<V> = HashMap<String, V>;

/// Everything guarded by the buffer mutex: per-table entries plus the total
/// entry count, maintained incrementally so `push` stays O(1) regardless of
/// how many tables are live.
#[derive(Default)]
struct Inner {
    tables: TableMap<TableEntries>,
    /// Sum of `tables[*].len()`.
    len: usize,
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
    entries: Mutex<Inner>,
    /// How to flush each table.  Populated by `register_*`.
    targets: Arc<Mutex<HashMap<String, Arc<dyn FlushTarget>>>>,
    /// Max pending entries before auto-flush (0 = no limit).
    max_size: usize,
    /// Called right before any flush (manual or auto).
    /// Used to sync counters to disk alongside buffered data.
    on_flush: Mutex<Option<OnFlushCallback>>,
    /// Serializes flushes so older and newer batches cannot commit out of order.
    flush_lock: Mutex<()>,
    /// Commit strategy for flush batches (Durable / Grouped / GroupedNonDurable).
    policy: CommitPolicyCell,
}

impl BufferStore {
    pub(crate) fn new(max_size: usize, policy: CommitPolicyCell) -> Self {
        Self {
            entries: Mutex::new(Inner::default()),
            targets: Arc::new(Mutex::new(HashMap::new())),
            max_size,
            on_flush: Mutex::new(None),
            flush_lock: Mutex::new(()),
            policy,
        }
    }

    /// Expose the buffer's commit policy for callers that run their own
    /// immediate operations (e.g. `remove`) through the same path.
    pub(crate) fn policy(&self) -> CommitPolicyCell {
        self.policy.clone()
    }

    /// Set a callback invoked before every flush (e.g., to sync counters).
    pub(crate) fn set_on_flush(&self, cb: OnFlushCallback) {
        *self.on_flush.lock() = Some(cb);
    }

    /// Acquire the flush serialization lock.  Held across a full
    /// remove-from-buffer-and-DB cycle so an in-flight flush cannot commit
    /// a batch containing a key that is being deleted.
    pub(crate) fn lock_flush(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.flush_lock.lock()
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
        self.targets.lock().insert(name, target);
    }

    /// Register a TTL table for flushing.
    pub(crate) fn register_ttl<K: Key + Send + Sync + 'static, V: Value + Send + Sync + 'static>(
        &self,
        def: TtlTableDefinition<K, V>,
        default_ttl: Duration,
    ) {
        let name = def.name().to_owned();
        let target: Arc<dyn FlushTarget> = Arc::new(TtlTarget { def, default_ttl });
        self.targets.lock().insert(name, target);
    }

    /// Push a serialised key-value pair.  Auto-flushes if max_size is reached.
    pub(crate) fn push(
        &self,
        table_name: &str,
        key_bytes: impl AsRef<[u8]>,
        value_bytes: impl AsRef<[u8]>,
        db: &Database,
        force_flush: bool,
    ) -> Result<(), DbError> {
        self.push_with_ttl(table_name, key_bytes, value_bytes, db, force_flush, None)
    }

    /// Like [`push`](Self::push) with an explicit per-entry TTL.
    ///
    /// Key and value are taken as slices: when the key is already pending its
    /// existing allocations are reused (overwrite in place), so a repeated
    /// write performs no allocation at all.
    pub(crate) fn push_with_ttl(
        &self,
        table_name: &str,
        key_bytes: impl AsRef<[u8]>,
        value_bytes: impl AsRef<[u8]>,
        db: &Database,
        force_flush: bool,
        ttl: Option<Duration>,
    ) -> Result<(), DbError> {
        let key_bytes = key_bytes.as_ref();
        let value_bytes = value_bytes.as_ref();
        // Clamp so `Instant + Duration` cannot overflow on absurd TTLs
        // (u32::MAX seconds ≈ 136 years).
        let deadline = ttl.map(|t| Instant::now() + t.min(Duration::from_secs(u32::MAX as u64)));
        let mut guard = self.entries.lock();
        let inner = &mut *guard;
        // `entry_ref` hashes once and only allocates an owned `String` / `Vec`
        // when the table / key is not yet present.
        let map = inner.tables.entry_ref(table_name).or_default();
        match map.entry_ref(key_bytes) {
            hashbrown::hash_map::EntryRef::Occupied(mut e) => {
                let pending = e.get_mut();
                pending.value_bytes.clear();
                pending.value_bytes.extend_from_slice(value_bytes);
                pending.deadline = deadline;
            }
            hashbrown::hash_map::EntryRef::Vacant(e) => {
                e.insert(PendingValue {
                    value_bytes: value_bytes.to_vec(),
                    deadline,
                });
                inner.len += 1;
            }
        }
        let len = inner.len;
        let should_flush = force_flush || (self.max_size > 0 && len >= self.max_size);
        drop(guard);

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
            .tables
            .get(table_name)
            .and_then(|m| m.get(key_bytes))
            .map(|pending| pending.value_bytes.clone())
    }

    /// Remove a key from the buffer.  Drops every pending occurrence of the
    /// key (repeated writes collapse into one entry).  Returns `true` if an
    /// entry was removed.
    pub(crate) fn remove(&self, table_name: &str, key_bytes: &[u8]) -> bool {
        let mut guard = self.entries.lock();
        let inner = &mut *guard;
        let Some(map) = inner.tables.get_mut(table_name) else {
            return false;
        };
        let existed = map.remove(key_bytes).is_some();
        if map.is_empty() {
            inner.tables.remove(table_name);
        }
        if existed {
            inner.len -= 1;
        }
        existed
    }

    /// Number of pending entries (unique keys across all tables).
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().len
    }

    /// Flush all pending entries to redb, grouped by table.
    /// Calls `on_flush` callback first (if set) to sync related state.
    ///
    /// If flushing fails part-way, all pending entries are put back into the
    /// buffer so no data is lost (entries that were already written are
    /// re-inserted idempotently on the next attempt).
    pub(crate) fn flush(&self, db: &Database) -> Result<usize, DbError> {
        let _flush_guard = self.flush_lock.lock();
        // Sync related state (e.g., counters) before writing buffered data.
        // Clone the Arc so the mutex is not held during the callback.
        let on_flush = self.on_flush.lock().clone();
        if let Some(cb) = on_flush {
            cb()?;
        }

        let mut guard = self.entries.lock();
        if guard.len == 0 {
            return Ok(0);
        }
        let count = std::mem::take(&mut guard.len);
        let batch = std::mem::take(&mut guard.tables);
        drop(guard);

        let table_count = batch.len();

        // Commit through the configured policy: Durable opens a dedicated
        // transaction, Grouped submits into redb's group-commit pipeline
        // (merging with any concurrent batch, e.g. from the TTL buffer),
        // GroupedNonDurable commits without fsync.  All tables of the batch
        // share one transaction/commit in every mode.
        //
        // The batch lives in a shared slot so that on failure whatever was not
        // consumed by the closure can be put back into the buffer (data-loss
        // prevention).  `commit_ops` invokes the closure at most once.
        let targets = self.targets.clone();
        let slot: Arc<Mutex<Option<TableMap<TableEntries>>>> = Arc::new(Mutex::new(Some(batch)));
        let slot_for_closure = slot.clone();
        let result = crate::group_commit::commit_ops(db, &self.policy, move |tx| {
            let taken = slot_for_closure
                .lock()
                .take()
                .expect("commit_ops invokes the closure at most once");
            let targets = targets.lock();
            match (|| {
                for (name, pairs) in &taken {
                    let target = targets.get(name).ok_or_else(|| {
                        log::error!("BufferStore::flush: no target registered for table `{name}`");
                        DbError::TableNotFound(name.clone())
                    })?;
                    target.flush(tx, pairs)?;
                }
                Ok(())
            })() {
                Ok(()) => Ok(()),
                // Transaction is not committed (or, in grouped mode, the whole
                // group rolls back) — hand the full batch back for a retry.
                Err(e) => {
                    *slot_for_closure.lock() = Some(taken);
                    Err(e)
                }
            }
        });

        match result {
            Ok(()) => {}
            Err(e) => {
                // Put back whatever the closure left behind (full batch on
                // failure — the aborted transaction persisted none of it).
                let leftover = slot.lock().take();
                if let Some(batch) = leftover {
                    let mut guard = self.entries.lock();
                    let inner = &mut *guard;
                    for (name, pairs) in batch {
                        let map = inner.tables.entry(name).or_default();
                        let before = map.len();
                        for (k, v) in pairs {
                            map.entry(k).or_insert(v);
                        }
                        let added = map.len() - before;
                        inner.len += added;
                    }
                }
                return Err(e);
            }
        }

        log::debug!("BufferStore::flush: {count} entries across {table_count} tables");
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

        let store = BufferStore::new(
            100,
            crate::group_commit::CommitPolicyCell::new(crate::group_commit::CommitPolicy::Durable),
        );
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
        let store = BufferStore::new(
            3,
            crate::group_commit::CommitPolicyCell::new(crate::group_commit::CommitPolicy::Durable),
        );
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
        let store = BufferStore::new(
            100,
            crate::group_commit::CommitPolicyCell::new(crate::group_commit::CommitPolicy::Durable),
        );
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
    fn strip_varint_strips_exact_prefix() {
        // 1-byte prefix: claims 3 elements, 3 remain.
        assert_eq!(strip_varint(&[3, 1, 2, 3]), &[1u8, 2, 3]);
        // 0 elements, empty payload.
        assert_eq!(strip_varint(&[0]), &[] as &[u8]);
    }

    #[test]
    fn strip_varint_two_byte_prefix() {
        // 254 marker + u16 LE len: claims 2, exactly 2 remain.
        let data = [254u8, 0x02, 0x00, 9, 9];
        assert_eq!(strip_varint(&data), &[9u8, 9]);
    }

    #[test]
    fn strip_varint_four_byte_prefix() {
        // 255 marker + u32 LE len: claims 2, exactly 2 remain.
        let data = [255u8, 2, 0, 0, 0, 7, 8];
        assert_eq!(strip_varint(&data), &[7u8, 8]);
    }

    #[test]
    fn strip_varint_truncated_returns_input() {
        assert_eq!(strip_varint(&[254, 1]), &[254u8, 1]);
        assert_eq!(strip_varint(&[255, 1, 2]), &[255u8, 1, 2]);
        assert_eq!(strip_varint(&[255, 1, 2, 3]), &[255u8, 1, 2, 3]);
        assert_eq!(strip_varint(&[255]), &[255u8]);
    }

    /// Regression: a blob whose payload itself starts with a varint-looking
    /// byte (postcard LEB128 of a unix timestamp starts with 0xFF) must be
    /// returned unchanged — the declared length never matches the payload.
    #[test]
    fn strip_varint_rejects_payload_that_mimics_prefix() {
        // postcard TitleInfo starts with date_create = 1787837567 (LEB128:
        // ff f8 c0 d4 06). Blind stripping ate 5 bytes and corrupted the row.
        let payload: Vec<u8> = [0xFFu8, 0xF8, 0xC0, 0xD4, 0x06, 0x01]
            .iter()
            .copied()
            .chain([0u8; 20])
            .collect();
        assert_eq!(
            strip_varint(&payload),
            &payload[..],
            "mimicking payload must not be stripped"
        );

        // The 0xFE marker variant of the same hazard.
        let payload_fe: Vec<u8> = [0xFEu8, 0x34, 0x12]
            .iter()
            .copied()
            .chain([0u8; 7])
            .collect();
        assert_eq!(strip_varint(&payload_fe), &payload_fe[..]);
    }

    /// Prefix length mismatch in the other direction: real prefix claims fewer
    /// bytes than remain (corrupt or not a prefix) — leave the data alone.
    #[test]
    fn strip_varint_length_mismatch_returns_input() {
        let data = [7u8, 1, 2, 3, 4]; // claims 7, only 4 remain
        assert_eq!(strip_varint(&data), &data[..]);
    }

    /// A buffered TTL write must expire `ttl` after the *write*, not `ttl`
    /// after the flush that happens to persist it.
    #[test]
    fn ttl_counts_from_write_time_not_flush_time() {
        const TTL_T: TtlTableDefinition<u64, String> = TtlTableDefinition::new("ttl_from_write");
        let (db, store, _dir) = setup();
        store.register_ttl(TTL_T, Duration::from_secs(3600));

        let k = serialize_value(&1u64);
        let v = serialize_value(&"v".to_string());
        store
            .push_with_ttl(
                "ttl_from_write",
                k,
                v,
                &db,
                false,
                Some(Duration::from_millis(1500)),
            )
            .expect("push");
        std::thread::sleep(Duration::from_millis(1000));
        store.flush(&db).expect("flush");

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let tx = db.begin_read().expect("read");
        let table = tx.open_ttl_table(TTL_T).expect("open");
        let guard = table.get(1u64).expect("get").expect("not yet expired");
        let remaining = guard.expires_at_ms().saturating_sub(now_ms);
        assert!(
            remaining <= 800,
            "TTL restarted at flush: {remaining} ms left of a 1500 ms TTL written 1000 ms ago"
        );
    }

    // -- Flush order ---------------------------------------------------------

    /// Like `setup()`, but without the 100-entry auto-flush: a test decides
    /// when a batch is flushed.
    fn setup_unbounded() -> (Arc<Database>, BufferStore, String) {
        let (db, _store, dir) = setup();
        let store = BufferStore::new(
            0,
            crate::group_commit::CommitPolicyCell::new(crate::group_commit::CommitPolicy::Durable),
        );
        store.register_regular(TEST_TABLE);
        (db, store, dir)
    }

    fn xorshift(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    /// Entries pushed in random order, of several key types, all land in the
    /// database intact: the key-ordered flush must use each table's own key
    /// comparator, not byte order.
    #[test]
    fn flush_in_key_order_roundtrips_for_each_key_type() {
        const U: TableDefinition<u64, String> = TableDefinition::new("ord_u64");
        const S: TableDefinition<String, u64> = TableDefinition::new("ord_string");
        const T: TableDefinition<(u32, u64), String> = TableDefinition::new("ord_tuple");
        const I: TableDefinition<i64, u64> = TableDefinition::new("ord_i64");

        let (db, store, _dir) = setup_unbounded();
        store.register_regular(U);
        store.register_regular(S);
        store.register_regular(T);
        store.register_regular(I);
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        let mut keys: Vec<u64> = (0..3_000).collect();
        // Fisher-Yates with a fixed seed.
        for i in (1..keys.len()).rev() {
            let j = (xorshift(&mut rng) % (i as u64 + 1)) as usize;
            keys.swap(i, j);
        }
        for k in &keys {
            let signed = (*k as i64) - 1_500; // negative and positive keys
            store
                .push(
                    U.name(),
                    serialize_value(k),
                    serialize_value(&format!("v{k}")),
                    &db,
                    false,
                )
                .unwrap();
            store
                .push(
                    S.name(),
                    serialize_value(&format!("key-{k:05}")),
                    serialize_value(k),
                    &db,
                    false,
                )
                .unwrap();
            store
                .push(
                    T.name(),
                    serialize_value(&((*k % 7) as u32, *k)),
                    serialize_value(&format!("t{k}")),
                    &db,
                    false,
                )
                .unwrap();
            store
                .push(
                    I.name(),
                    serialize_value(&signed),
                    serialize_value(k),
                    &db,
                    false,
                )
                .unwrap();
        }
        assert_eq!(store.flush(&db).unwrap(), 4 * 3_000);

        let tx = db.begin_read().unwrap();
        let u = tx.open_table(U).unwrap();
        let s = tx.open_table(S).unwrap();
        let t = tx.open_table(T).unwrap();
        let i = tx.open_table(I).unwrap();
        use shodh_redb::{ReadableTable, ReadableTableMetadata};
        for k in 0..3_000u64 {
            assert_eq!(u.get(k).unwrap().unwrap().value(), format!("v{k}"));
            assert_eq!(s.get(format!("key-{k:05}")).unwrap().unwrap().value(), k);
            assert_eq!(
                t.get(((k % 7) as u32, k)).unwrap().unwrap().value(),
                format!("t{k}")
            );
            assert_eq!(i.get((k as i64) - 1_500).unwrap().unwrap().value(), k);
        }
        assert_eq!(u.len().unwrap(), 3_000);
        let ordered: Vec<i64> = i.iter().unwrap().map(|r| r.unwrap().0.value()).collect();
        assert!(
            ordered.windows(2).all(|w| w[0] < w[1]),
            "i64 keys out of order"
        );
    }

    /// Sequential keys flushed batch after batch pack their leaves: this is
    /// the case the key-ordered flush exists for.
    #[test]
    fn sequential_keys_across_flushes_pack_leaves() {
        const U: TableDefinition<u64, Vec<u8>> = TableDefinition::new("pack_u64");
        let (db, store, _dir) = setup_unbounded();
        store.register_regular(U);

        let value = serialize_value(&vec![7u8; 150]);
        let mut next = 0u64;
        for _ in 0..20 {
            // Insert each batch in descending order: only the flush ordering
            // can turn it into an ascending load.
            for k in (next..next + 1_000).rev() {
                store
                    .push(U.name(), serialize_value(&k), value.clone(), &db, false)
                    .unwrap();
            }
            next += 1_000;
            store.flush(&db).unwrap();
        }

        use shodh_redb::ReadableTableMetadata;
        let tx = db.begin_read().unwrap();
        let t = tx.open_table(U).unwrap();
        assert_eq!(t.len().unwrap(), 20_000);
        let leaves = t.stats().unwrap().leaf_pages();
        // ~150B values: ~25 entries per 4K page when packed => ~800 leaves;
        // an even split of every leaf would need ~1600.
        assert!(leaves < 1_000, "{leaves} leaves for 20_000 sequential rows");
    }

    /// An overwrite of a pending key inside a batch still keeps only the last
    /// value after the ordered flush.
    #[test]
    fn ordered_flush_keeps_last_value_per_key() {
        const U: TableDefinition<u64, String> = TableDefinition::new("last_wins");
        let (db, store, _dir) = setup_unbounded();
        store.register_regular(U);
        for round in 0..5 {
            for k in (0..500u64).rev() {
                store
                    .push(
                        U.name(),
                        serialize_value(&k),
                        serialize_value(&format!("r{round}")),
                        &db,
                        false,
                    )
                    .unwrap();
            }
        }
        assert_eq!(store.flush(&db).unwrap(), 500);
        let tx = db.begin_read().unwrap();
        let t = tx.open_table(U).unwrap();
        for k in 0..500u64 {
            assert_eq!(t.get(k).unwrap().unwrap().value(), "r4");
        }
    }
}
