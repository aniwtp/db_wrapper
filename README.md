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

Measured on ext4 (real fsync), 16 threads, shodh-redb 0.6, `cargo bench`
(data dir `/var/tmp`, override with `BENCH_DIR`):

| Operation | Latency |
|---|---|
| `next(id)` (counter) | 1.6 ns |
| `next(id)` ×4 threads, distinct ids (800k ops) | 0.39 ms total |
| Buffer hit (`get_buffered`) | 25 ns |
| `get()` — buffer hit / DB hit | 32 ns / 393 ns |
| `write()` overwriting a pending key | 30 ns |
| `write_ttl()` (buffered) | 44 ns |
| Raw DB read | 353 ns |
| Flush 9000 entries (no fsync) | 6.2 ms |
| Open + restore 256 counters + close | 13.5 ms (2 × ~3.3 ms is redb `Database` drop) |

End to end (500k rows of 200 B, flushed in 10k batches, fork vs. the original crate on
shodh-redb 0.5): insert 880 -> 585 ms, overwrite 900 -> 470 ms, peak RSS 227 -> 163 MB;
`commit_policy` per write: Durable 1.63 -> 1.12 us, Grouped 1.58 -> 1.03 us,
GroupedNonDurable 1.30 -> 0.82 us.

`shodh-redb` comes from the `umzi2/shodh-redb-fork` branch `perf/upstream-port` (see
`Cargo.toml`): the key-ordered flush needs its ascending-insert leaf packing.

Run: `cargo bench` (`--bench commit_policy` for the commit-policy comparison)

## License

MIT
