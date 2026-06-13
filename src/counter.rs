//! In-memory atomic counter with periodic blob flush to redb.
//!
//! Hot path (`next`) is a single `AtomicU64::fetch_add` — no locks, no hashing.
//! Counters are persisted as a single 2048-byte blob under one key.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use shodh_redb::{Database, ReadableDatabase, TableDefinition};

use crate::DbError;
use crate::db;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Blob table: stores the entire `[u64; 256]` array as `Vec<u8>`.
pub(crate) const COUNTER_BLOB: TableDefinition<u8, Vec<u8>> =
    TableDefinition::new("counter_blob");

/// Fixed key for the single blob entry.
pub(crate) const COUNTER_BLOB_KEY: u8 = 0;

/// Number of independent counter slots (`counter_id: u8` → 0..=255).
pub(crate) const COUNTER_SLOTS: usize = 256;

/// How often the maintenance loop flushes buffers + counters (seconds).
pub const BUFFER_FLUSH_SECS: u64 = 10;

/// Max buffer entries before force-flush (OOM guard).
pub const BUFFER_MAX_ENTRIES: usize = 10_000;

// ---------------------------------------------------------------------------
// CounterStore
// ---------------------------------------------------------------------------

/// Holds the hot in-memory counter values and flushes them to redb
/// periodically.  Only writes values that changed since the last flush.
pub(crate) struct CounterStore {
    values: Box<[AtomicU64; COUNTER_SLOTS]>,
    flushed: Mutex<[u64; COUNTER_SLOTS]>,
    db: Arc<Database>,
}

impl CounterStore {
    pub(crate) fn new(db: Arc<Database>) -> Result<Self, DbError> {
        let loaded = Self::load_all(&db)?;
        let values = Box::new(std::array::from_fn(|i| AtomicU64::new(loaded[i])));
        let flushed = Mutex::new(loaded);
        log::debug!("CounterStore: initialised {COUNTER_SLOTS} slots from disk");
        Ok(Self { values, flushed, db })
    }

    fn load_all(db: &Database) -> Result<[u64; COUNTER_SLOTS], DbError> {
        let tx = db!(db.begin_read())?;
        let table = db!(tx.open_table(COUNTER_BLOB))?;
        let bytes: Vec<u8> = match table.get(COUNTER_BLOB_KEY).map_err(|e| DbError::Redb(e.into()))? {
            Some(v) => v.value().clone(),
            None => return Ok([0u64; COUNTER_SLOTS]),
        };
        let mut arr = [0u64; COUNTER_SLOTS];
        for (i, chunk) in bytes.chunks_exact(8).enumerate() {
            let chunk: [u8; 8] = chunk.try_into().unwrap();
            arr[i] = u64::from_le_bytes(chunk);
        }
        Ok(arr)
    }

    #[inline(always)]
    pub(crate) fn next(&self, counter_id: u8) -> u64 {
        self.values[counter_id as usize].fetch_add(1, Ordering::Relaxed) + 1
    }

    pub(crate) fn flush(&self) -> Result<usize, DbError> {
        let mut snapshot = [0u64; COUNTER_SLOTS];
        for (i, v) in self.values.iter().enumerate() {
            snapshot[i] = AtomicU64::load(v, Ordering::Relaxed);
        }
        let mut flushed = self.flushed.lock().unwrap();
        if snapshot == *flushed {
            return Ok(0);
        }
        let bytes: Vec<u8> = snapshot.iter().flat_map(|v| v.to_le_bytes()).collect();
        let tx = db!(self.db.begin_write())?;
        {
            let mut table = db!(tx.open_table(COUNTER_BLOB))?;
            db!(table.insert(COUNTER_BLOB_KEY, bytes))?;
        }
        db!(tx.commit())?;
        *flushed = snapshot;
        let dirty = snapshot.iter().filter(|&&v| v != 0).count();
        log::debug!("CounterStore::flush blob written ({dirty} non-zero counters)");
        Ok(1)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::AtomicU32;

    static TEST_ID: AtomicU32 = AtomicU32::new(0);
    fn unique_dir(label: &str) -> std::path::PathBuf {
        let id = TEST_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!("teamdb_test_{label}_{id}"))
    }

    fn temp_db() -> (Arc<Database>, String) {
        let dir = unique_dir("counter");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.redb.counter");
        let path_s = path.to_str().unwrap().to_owned();
        let db = Arc::new(Database::create(&path_s).expect("create db"));
        let tx = db.begin_write().expect("begin write");
        tx.open_table(COUNTER_BLOB).expect("open table");
        tx.commit().expect("commit");
        (db, dir.to_str().unwrap().to_owned())
    }

    #[test]
    fn new_starts_at_zero() {
        let (db, _dir) = temp_db();
        let store = CounterStore::new(db).expect("new");
        assert_eq!(store.next(0), 1);
        assert_eq!(store.next(0), 2);
        assert_eq!(store.next(5), 1);
    }

    #[test]
    fn flush_and_restore() {
        let (db, _dir) = temp_db();
        let store = CounterStore::new(db.clone()).expect("new");
        for _ in 0..10 { store.next(0); }
        for _ in 0..5 { store.next(1); }
        store.next(255);
        let written = store.flush().expect("flush");
        assert_eq!(written, 1);
        let store2 = CounterStore::new(db).expect("new");
        assert_eq!(store2.next(0) - 1, 10);
        assert_eq!(store2.next(1) - 1, 5);
        assert_eq!(store2.next(2) - 1, 0);
        assert_eq!(store2.next(255) - 1, 1);
    }

    #[test]
    fn flush_skips_when_no_changes() {
        let (db, _dir) = temp_db();
        let store = CounterStore::new(db).expect("new");
        assert_eq!(store.flush().unwrap(), 0);
        store.next(0);
        assert_eq!(store.flush().unwrap(), 1);
        assert_eq!(store.flush().unwrap(), 0);
    }

    #[test]
    fn concurrent_next_and_flush() {
        let (db, _dir) = temp_db();
        let store = Arc::new(CounterStore::new(db).expect("new"));
        let store2 = store.clone();
        let handle = std::thread::spawn(move || {
            for _ in 0..10_000 { store2.next(0); }
        });
        for _ in 0..100 { store.flush().ok(); }
        handle.join().unwrap();
        assert!(store.next(0) > 10_000);
    }

    #[test]
    fn all_256_slots_independent() {
        let (db, _dir) = temp_db();
        let store = CounterStore::new(db).expect("new");
        for id in 0..=255u8 { assert_eq!(store.next(id), 1); }
        for id in 0..=255u8 { assert_eq!(store.next(id), 2); }
        store.flush().expect("flush");
        let store2 = CounterStore::new(store.db.clone()).expect("new");
        for id in 0..=255u8 { assert_eq!(store2.next(id) - 1, 2); }
    }

    #[test]
    fn load_all_empty_db_returns_zeros() {
        let dir = unique_dir("counter_empty");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("empty.redb.counter");
        let path_s = path.to_str().unwrap();
        let db = Database::create(path_s).expect("create");
        let tx = db.begin_write().expect("begin_write");
        tx.open_table(COUNTER_BLOB).expect("open_table");
        tx.commit().expect("commit");
        let store = CounterStore::new(Arc::new(db)).expect("new");
        for id in 0..=255u8 { assert_eq!(store.next(id) - 1, 0); }
        let _ = fs::remove_dir_all(&dir);
    }
}
