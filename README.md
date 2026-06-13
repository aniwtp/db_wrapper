# db-wrapper

Embedded database layer for [shodh-redb](https://crates.io/crates/shodh-redb) with:

- **Lock-free atomic counters** — `next(id)` in ~8 ns, persisted as a single blob.
- **Unified write buffers** — one buffer for all tables, no per-table pools.
- **Read-through lookups** — check buffer first, then fall back to DB.
- **No async runtime** — flush is externally driven (timer/spawn in host app).

## Usage

```rust
use db_wrapper::DBWrapper;

let db = DBWrapper::new("my_app.redb")?;

// Register tables once at startup.
db.init_tables(|db| {
    db.register_table(MY_TABLE);
});

// Allocate IDs and buffer writes.
let id = db.next(0);
db.write(MY_TABLE, id, my_data)?;

// Read: buffer first.
let key = db_wrapper::serialize_value(&id);
if let Some(bytes) = db.get_buffered(MY_TABLE.name(), &key) {
    let value = String::from_bytes(&bytes);
}

// Flush to disk (call periodically, e.g. every 10s).
db.flush_buffers()?;
```

## Benchmarks

| Operation | Latency | Throughput |
|---|---|---|
| `next(id)` (counter) | 8.4 ns | 119M /s |
| `next(id)` during flush | 8.7 ns | 115M /s |
| Buffer hit (`get_buffered`) | 33 ns | 30M /s |
| Buffer miss → DB read | 739 ns | 1.35M /s |
| Raw DB read | 683 ns | 1.46M /s |
| Restore 256 counters | 733 µs | — |
| Write 1000 + flush | 2.4 ms | — |

Run: `cargo bench`

## License

MIT
