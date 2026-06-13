//! Integration tests for team-db — full lifecycle: open → write → flush → restore → verify.

use std::fs;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use shodh_redb::{ReadableDatabase, ReadableTable, TableDefinition, TableHandle, Value};
use db_wrapper::DBWrapper;

static TEST_ID: AtomicU32 = AtomicU32::new(0);
fn unique_dir() -> String {
    let id = TEST_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("teamdb_integration_{id}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir.to_str().unwrap().to_owned()
}

const TEST_TABLE: TableDefinition<u64, String> = TableDefinition::new("integration_test");

fn setup() -> (DBWrapper, String) {
    let dir = unique_dir();
    let path = format!("{dir}/test.redb");
    let db = DBWrapper::new(&path).expect("open");
    db.register_table(TEST_TABLE);
    (db, dir)
}

#[test]
fn full_lifecycle_write_flush_restore_read() {
    let (db, dir) = setup();

    // 1. Write some data.
    db.write(TEST_TABLE, 1u64, "one".to_string()).expect("w1");
    db.write(TEST_TABLE, 2u64, "two".to_string()).expect("w2");
    db.next(0); // allocate a counter

    // 2. Read from buffer (before flush).
    let k1 = db_wrapper::serialize_value(&1u64);
    let val = db.get_buffered(TEST_TABLE.name(), &k1).expect("in buffer");
    assert_eq!(String::from_bytes(&val), "one");

    // 3. Flush both buffers and counters.
    db.flush_counters().expect("flush counters");
    db.flush_buffers().expect("flush buffers");

    // 4. Buffer should be empty now.
    let k2 = db_wrapper::serialize_value(&2u64);
    assert!(db.get_buffered(TEST_TABLE.name(), &k2).is_none());

    // 5. Data is in DB.
    let tx = db.db.begin_read().expect("read");
    let table = tx.open_table(TEST_TABLE).expect("open");
    assert_eq!(table.get(1u64).unwrap().unwrap().value(), "one");
    assert_eq!(table.get(2u64).unwrap().unwrap().value(), "two");

    // 6. Counter value after another increment (not flushed yet).
    let counter_val = db.next(0);
    assert!(counter_val > 0);

    // 7. Flush counters again before close.
    db.flush_counters().expect("flush counters 2");
    drop(db);
    let path = format!("{dir}/test.redb");
    let db2 = DBWrapper::new(&path).expect("reopen");
    db2.register_table(TEST_TABLE);

    let tx = db2.db.begin_read().expect("read");
    let table = tx.open_table(TEST_TABLE).expect("open");
    assert_eq!(table.get(1u64).unwrap().unwrap().value(), "one");
    assert_eq!(table.get(2u64).unwrap().unwrap().value(), "two");

    let restored_counter = db2.next(0);
    assert_eq!(restored_counter, counter_val + 1); // +1 because we called next() after flush

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn counter_independence() {
    let (db, _dir) = setup();
    // Different counter IDs are independent.
    let a = db.next(10);
    let b = db.next(20);
    let c = db.next(10);
    assert_eq!(a, 1);
    assert_eq!(b, 1);
    assert_eq!(c, 2);
}

#[test]
fn concurrent_writes() {
    let (db, _dir) = setup();
    let db = Arc::new(db);

    let b1 = db.clone();
    let b2 = db.clone();

    let h1 = std::thread::spawn(move || {
        for i in 0u64..500 {
            b1.write(TEST_TABLE, i, format!("thread1_{i}")).expect("w");
        }
    });
    let h2 = std::thread::spawn(move || {
        for i in 500u64..1000 {
            b2.write(TEST_TABLE, i, format!("thread2_{i}")).expect("w");
        }
    });

    h1.join().unwrap();
    h2.join().unwrap();

    db.flush_buffers().expect("flush");

    let tx = db.db.begin_read().expect("read");
    let table = tx.open_table(TEST_TABLE).expect("open");
    assert!(table.get(0u64).unwrap().is_some());
    assert!(table.get(999u64).unwrap().is_some());
}

#[test]
fn auto_flush_on_max_size_prevents_oom() {
    // Create DB with very small buffer max.
    let dir = unique_dir();
    let path = format!("{dir}/test.redb");
    let db = DBWrapper::new(&path).expect("open");
    db.register_table(TEST_TABLE);

    // Write more than the default BUFFER_MAX_ENTRIES.
    // Each write pushes to buffer; when max is hit, auto-flush triggers.
    for i in 0u64..15_000 {
        db.write(TEST_TABLE, i, format!("val_{i}")).expect("write");
    }
    // Should not OOM — auto-flush kept buffer under max_size.
    assert!(db.pending_writes() < 10_000);

    // All data should be in DB after final flush.
    db.flush_buffers().expect("final flush");
    let tx = db.db.begin_read().expect("read");
    let table = tx.open_table(TEST_TABLE).expect("open");
    assert_eq!(table.get(0u64).unwrap().unwrap().value(), "val_0");
    assert_eq!(table.get(14_999u64).unwrap().unwrap().value(), "val_14999");

    let _ = fs::remove_dir_all(&dir);
}
