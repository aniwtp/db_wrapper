//! Benchmarks for team-db.
//!
//! Run: `cargo bench`

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use db_wrapper::DBWrapper;

fn temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("teamdb_bench_{label}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn open_db(dir: &Path) -> DBWrapper {
    DBWrapper::new(dir.join("bench.redb").to_str().unwrap()).expect("open")
}

// -- 1. Hot path ------------------------------------------------------------

fn bench_next_hot(c: &mut Criterion) {
    let dir = temp_dir("hot");
    let db = open_db(&dir);
    let mut g = c.benchmark_group("1-hot");
    g.throughput(Throughput::Elements(1));
    g.bench_function("next(0)", |b| b.iter(|| black_box(db.next(black_box(0)))));
    g.finish();
    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

fn bench_atomic_raw(c: &mut Criterion) {
    let ctr = AtomicU64::new(0);
    let mut g = c.benchmark_group("1-hot");
    g.throughput(Throughput::Elements(1));
    g.bench_function("AtomicU64::fetch_add", |b| {
        b.iter(|| black_box(ctr.fetch_add(1, Ordering::Relaxed)));
    });
    g.finish();
}

// -- 2. During flush --------------------------------------------------------

fn bench_next_during_flush(c: &mut Criterion) {
    let dir = temp_dir("during_flush");
    let db = Arc::new(open_db(&dir));
    for id in 0..255u8 {
        db.next(id);
    }
    let db2 = db.clone();

    let mut g = c.benchmark_group("2-during-flush");
    g.throughput(Throughput::Elements(1));
    g.bench_function("next() while flush_counters runs", |b| {
        let stop = Arc::new(AtomicU64::new(0));
        let stop2 = stop.clone();
        let flush_db = db2.clone();
        let t = thread::spawn(move || {
            while stop2.load(Ordering::Relaxed) == 0 {
                let _ = flush_db.flush_counters();
            }
        });
        b.iter(|| black_box(db.next(black_box(0))));
        stop.store(1, Ordering::Relaxed);
        let _ = t.join();
    });
    g.finish();
    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

// -- 3. Restore -------------------------------------------------------------

fn bench_restore(c: &mut Criterion) {
    let dir = temp_dir("restore");
    let expected = {
        let db = open_db(&dir);
        for id in 0..255u8 {
            for _ in 0..(id as u64 + 1) {
                db.next(id);
            }
        }
        db.flush_counters().expect("flush");
        let vals: Vec<u64> = (0..255u8).map(|id| db.next(id) - 1).collect();
        drop(db);
        vals
    };

    let mut g = c.benchmark_group("3-restore");
    g.throughput(Throughput::Elements(256));
    g.bench_function("DBWrapper::new() + validate 256 counters", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let db = DBWrapper::new(dir.join("bench.redb").to_str().unwrap()).expect("restore");
                for id in 0..255u8 {
                    assert_eq!(db.next(id) - 1, expected[id as usize]);
                }
                drop(db);
            }
            start.elapsed()
        });
    });
    g.finish();
    let _ = fs::remove_dir_all(&dir);
}

// -- 4. Buffer write + flush ------------------------------------------------

fn bench_buffer_write_flush(c: &mut Criterion) {
    use shodh_redb::TableDefinition;
    type TeamValue = (String, String, Vec<String>, Vec<(u8, String)>, bool);
    const T: TableDefinition<u64, TeamValue> = TableDefinition::new("bench_team");

    let dir = temp_dir("buffer_flush");

    let mut g = c.benchmark_group("4-buffer");
    g.throughput(Throughput::Elements(1));

    g.bench_function("write 1000 + flush", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let db = open_db(&dir);
                db.register_table(T);
                let start = Instant::now();
                for i in 0u64..1000 {
                    db.write(
                        T,
                        i,
                        (
                            "n".to_string(),
                            "s".to_string(),
                            vec!["a".to_string()],
                            vec![(0, "l".to_string())],
                            false,
                        ),
                    )
                    .expect("write");
                }
                db.flush_buffers().expect("flush");
                total += start.elapsed();
                drop(db);
            }
            total
        });
    });
    g.finish();
    let _ = fs::remove_dir_all(&dir);
}

fn bench_buffer_write_overwrite(c: &mut Criterion) {
    use shodh_redb::TableDefinition;
    const T: TableDefinition<u64, String> = TableDefinition::new("bench_overwrite");

    let dir = temp_dir("buffer_overwrite");
    let db = open_db(&dir);
    db.register_table(T);

    let mut g = c.benchmark_group("4-buffer");
    g.throughput(Throughput::Elements(1));
    g.bench_function("write 1000 keys (overwrite, no flush)", |b| {
        // Re-writing the same 1000 keys keeps the buffer at a constant size:
        // with deduplication this never triggers the auto-flush threshold.
        b.iter(|| {
            for i in 0u64..1000 {
                db.write(T, i, "value".to_string()).expect("write");
            }
        });
    });
    g.finish();
    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

fn bench_buffer_flush_9000(c: &mut Criterion) {
    use shodh_redb::TableDefinition;
    const T: TableDefinition<u64, String> = TableDefinition::new("bench_flush9k");

    let dir = temp_dir("buffer_flush9k");
    let db = open_db(&dir);
    db.register_table(T);

    let mut g = c.benchmark_group("4-buffer");
    g.throughput(Throughput::Elements(9_000));
    g.bench_function("flush 9000 buffered entries", |b| {
        b.iter_batched(
            || {
                for i in 0u64..9_000 {
                    db.write(T, i, format!("value_{i}")).expect("write");
                }
                &db
            },
            |db| db.flush_buffers().expect("flush"),
            criterion::BatchSize::SmallInput,
        );
    });
    g.finish();
    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

// -- 5. Read: buffer hit/miss -----------------------------------------------

fn bench_read_buffer_hit(c: &mut Criterion) {
    use shodh_redb::{TableDefinition, TableHandle};
    const T: TableDefinition<u64, String> = TableDefinition::new("read_test");

    let dir = temp_dir("read_buf_hit");
    let db = open_db(&dir);
    db.register_table(T);

    // Pre-populate buffer with one entry.
    db.write(T, 1u64, "value".to_string()).expect("write");
    let key_bytes = db_wrapper::serialize_value(&1u64);

    let mut g = c.benchmark_group("5-read");
    g.throughput(Throughput::Elements(1));
    g.bench_function("get_buffered() — key in buffer", |b| {
        b.iter(|| black_box(db.get_buffered(T.name(), &key_bytes)));
    });
    g.finish();

    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

fn bench_read_buffer_hit_10k(c: &mut Criterion) {
    use shodh_redb::{TableDefinition, TableHandle};
    const T: TableDefinition<u64, String> = TableDefinition::new("read_10k");

    let dir = temp_dir("read_buf_hit_10k");
    let db = open_db(&dir);
    db.register_table(T);

    // Near-max buffer (9_999 of 10_000) — a lookup that would be O(N) in a
    // linear buffer and is O(1) in the hashmap buffer.
    for i in 0u64..9_999 {
        db.write(T, i, format!("value_{i}")).expect("write");
    }
    let key_bytes = db_wrapper::serialize_value(&9_998u64);
    let missing = db_wrapper::serialize_value(&u64::MAX);

    let mut g = c.benchmark_group("5-read");
    g.throughput(Throughput::Elements(1));
    g.bench_function("get_buffered() hit — 9999-entry buffer", |b| {
        b.iter(|| black_box(db.get_buffered(T.name(), &key_bytes)));
    });
    g.bench_function("get_buffered() miss — 9999-entry buffer", |b| {
        b.iter(|| black_box(db.get_buffered(T.name(), &missing)));
    });
    g.finish();

    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

fn bench_read_db_miss(c: &mut Criterion) {
    use shodh_redb::{ReadableDatabase, TableDefinition, TableHandle};
    const T: TableDefinition<u64, String> = TableDefinition::new("read_test2");

    let dir = temp_dir("read_db_miss");
    let db = open_db(&dir);
    db.register_table(T);

    // Write + flush so data is only on disk, not in buffer.
    db.write(T, 1u64, "value".to_string()).expect("write");
    db.flush_buffers().expect("flush");
    let key_bytes = db_wrapper::serialize_value(&1u64);

    let mut g = c.benchmark_group("5-read");
    g.throughput(Throughput::Elements(1));
    g.bench_function("get_buffered() miss → DB read", |b| {
        b.iter(|| {
            // Buffer miss.
            let _ = black_box(db.get_buffered(T.name(), &key_bytes));
            // Fallback: DB read.
            let tx = db.db.begin_read().expect("read");
            let table = tx.open_table(T).expect("open");
            black_box(table.get(1u64).unwrap());
        });
    });
    g.finish();

    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

fn bench_read_db_raw(c: &mut Criterion) {
    use shodh_redb::{ReadableDatabase, TableDefinition};
    const T: TableDefinition<u64, String> = TableDefinition::new("read_test3");

    let dir = temp_dir("read_db_raw");
    let db = open_db(&dir);
    db.register_table(T);

    // Write + flush so data is on disk.
    db.write(T, 1u64, "value".to_string()).expect("write");
    db.flush_buffers().expect("flush");

    let mut g = c.benchmark_group("5-read");
    g.throughput(Throughput::Elements(1));
    g.bench_function("raw DB read (no buffer check)", |b| {
        b.iter(|| {
            let tx = db.db.begin_read().expect("read");
            let table = tx.open_table(T).expect("open");
            black_box(table.get(1u64).unwrap());
        });
    });
    g.finish();

    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

fn bench_get_wrapper(c: &mut Criterion) {
    use shodh_redb::TableDefinition;
    const T: TableDefinition<u64, String> = TableDefinition::new("read_wrapper");

    let dir = temp_dir("read_wrapper");
    let db = open_db(&dir);
    db.register_table(T);

    let mut g = c.benchmark_group("5-read");
    g.throughput(Throughput::Elements(1));
    g.bench_function("get() — buffer hit", |b| {
        db.write(T, 1u64, "value".to_string()).expect("write");
        b.iter(|| black_box(db.get(T, 1u64)));
    });
    g.bench_function("get() — DB hit", |b| {
        db.flush_buffers().expect("flush");
        b.iter(|| black_box(db.get(T, 1u64)));
    });
    g.finish();

    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

// -- 6. Remove ---------------------------------------------------------------

fn bench_remove_buffered(c: &mut Criterion) {
    use shodh_redb::TableDefinition;
    const T: TableDefinition<u64, String> = TableDefinition::new("remove_test");

    let dir = temp_dir("remove_buffered");
    let db = open_db(&dir);
    db.register_table(T);

    let mut g = c.benchmark_group("6-remove");
    g.throughput(Throughput::Elements(1));
    g.bench_function("remove() — key in buffer", |b| {
        b.iter_batched(
            || {
                db.write(T, 1u64, "value".to_string()).expect("write");
                &db
            },
            |db| db.remove(T, 1u64).expect("remove"),
            criterion::BatchSize::SmallInput,
        );
    });
    g.finish();

    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

// -- 7. Counters -------------------------------------------------------------

fn bench_counter_flush(c: &mut Criterion) {
    let dir = temp_dir("counter_flush");
    let db = open_db(&dir);

    let mut g = c.benchmark_group("7-counter-flush");
    g.throughput(Throughput::Elements(1));
    g.bench_function("flush_counters after next()", |b| {
        b.iter_batched(
            || {
                db.next(0);
                &db
            },
            |db| db.flush_counters().expect("flush"),
            criterion::BatchSize::SmallInput,
        );
    });
    g.finish();

    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

// -- 8. TTL ------------------------------------------------------------------

fn bench_write_ttl(c: &mut Criterion) {
    use shodh_redb::ttl_table::TtlTableDefinition;
    const TTL_T: TtlTableDefinition<u64, String> = TtlTableDefinition::new("bench_ttl");

    let dir = temp_dir("ttl_write");
    let db = open_db(&dir);
    db.register_ttl_table(TTL_T, Duration::from_secs(3600));

    let mut g = c.benchmark_group("8-ttl");
    g.throughput(Throughput::Elements(1));
    g.bench_function("write_ttl (buffered, ttl=60s)", |b| {
        b.iter(|| {
            db.write_ttl(TTL_T, 1u64, "value".to_string(), Duration::from_secs(60))
                .expect("write_ttl");
        });
    });
    g.finish();

    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

// -- 9. Compaction -----------------------------------------------------------

fn bench_compact(c: &mut Criterion) {
    use shodh_redb::TableDefinition;
    const T: TableDefinition<u64, String> = TableDefinition::new("bench_compact");

    let dir = temp_dir("compact");
    let db = open_db(&dir);
    db.register_table(T);
    for i in 0u64..5_000 {
        db.write(T, i, format!("value_{i}")).expect("write");
    }
    db.flush_buffers().expect("flush");

    let mut g = c.benchmark_group("9-compact");
    g.throughput(Throughput::Elements(1));
    g.bench_function("compact() after 5000 rows", |b| {
        b.iter(|| db.compact().expect("compact"));
    });
    g.finish();

    drop(db);
    let _ = fs::remove_dir_all(&dir);
}

criterion_group!(
    benches,
    bench_next_hot,
    bench_atomic_raw,
    bench_next_during_flush,
    bench_restore,
    bench_buffer_write_flush,
    bench_buffer_write_overwrite,
    bench_buffer_flush_9000,
    bench_read_buffer_hit,
    bench_read_buffer_hit_10k,
    bench_read_db_miss,
    bench_read_db_raw,
    bench_get_wrapper,
    bench_remove_buffered,
    bench_counter_flush,
    bench_write_ttl,
    bench_compact,
);
criterion_main!(benches);
