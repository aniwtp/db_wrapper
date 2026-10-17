# AGENT.md — db-wrapper

Embedded database crate: **redb** (shodh-redb) with in-memory atomic counters,
unified write buffers, and read-through lookups.  No async runtime dependency.

---

## Project tree

```
db-wrapper/
├── Cargo.toml              # shodh-redb + hashbrown + parking_lot + log + thiserror
├── AGENT.md                # ← this file
├── README.md
│
├── src/
│   ├── lib.rs              # crate root: re-exports, db! macro
│   ├── counter.rs          # CounterStore — lock-free AtomicU64[256] + blob persist
│   ├── buffer.rs           # BufferStore — unified regular + TTL buffers
│   ├── wrapper.rs          # DBWrapper — open, compact, backup, next, write, get_buffered
│   └── error.rs            # DbError (Redb, Io, TableNotFound)
│
├── tests/
│   └── integration.rs      # full lifecycle: write → flush → restore → verify
│
└── benches/
    └── bench.rs            # hot path, during-flush, restore, read, buffer write
```

---

## Quick commands

```sh
# Dev build
cargo build

# Tests (67 unit + 8 integration)
cargo test

# Benchmarks (data dir: /var/tmp, override BENCH_DIR — NOT tmpfs, fsync must be real)
cargo bench

# Lint
cargo clippy

# Format
cargo fmt
```

---

## Architecture

### Counters — `CounterStore`

- **256 independent slots** (`u8` → `0..=255`), each backed by `AtomicU64`.
- Hot path: `array[id].fetch_add(1, Relaxed) + 1` → ~8 ns, no locks, no hashing.
- Persisted as a **single 2048-byte blob** under one key (`counter_blob`).
- Flush skips write entirely if nothing changed since last flush.

```rust
let id = db.next(0);  // → 1, 2, 3, …
db.flush_counters()?; // write dirty counters to redb
```

### Buffers — `BufferStore`

- **Two unified buffers** (regular + TTL), not per-table.
- All writes across all tables land in the same buffer.
- **No background async tasks** — flush is triggered manually or by size threshold.

```rust
db.register_table(MY_TABLE);       // once at startup
db.write(MY_TABLE, key, value)?;   // → buffer
db.flush_buffers()?;               // → redb

// Read: buffer first, fall back to DB yourself
if let Some(bytes) = db.get_buffered(MY_TABLE.name(), &key_bytes) {
    // found in buffer
}
```

### Auto-flush (OOM guard)

- `BufferStore::push` auto-flushes when `len >= max_size` (default: 10 000).
- Flush calls `on_flush` callback first → counters synced to disk alongside data.

### Read-through

- `get_buffered(table_name, key_bytes)` scans regular buffer → TTL buffer.
- Returns `None` if not found — caller falls back to direct redb read via `db.db`.

### Flush timing

- Externally driven (caller spawns a timer loop).
- Recommended: every 10 seconds (`BUFFER_FLUSH_SECS`).
- Daily: compact + backup.

```rust
// In the host application's main loop:
loop {
    sleep(Duration::from_secs(BUFFER_FLUSH_SECS)).await;
    db.flush_buffers()?;
}
```

### Counters + buffers sync

- `BufferStore` has an `on_flush` callback.
- When buffers flush (manual or auto-max-size), counters flush first.
- Result: counter values always hit disk before or with buffered data.

### Schema migrations — `migrations.rs`

- Schema version stored in the internal `_schema` table (`u8 key 0 → u64 version`),
  created automatically in `DBWrapper::new`.
- `Migration` trait: `from_version()`, `to_version()` (= from+1), `migrate(&DBWrapper)`.
- Steps must be **idempotent**: data is rewritten first, the version bump is a
  separate commit — a crash between them replays the step on next open.
- `run_migrations()` is **explicit** (never runs on open); refuses downgrade
  with `DbError::SchemaVersionTooNew`; serialized by an internal run-lock.

```rust
let db = DBWrapper::new("app.redb")?;
db.register_migration_boxed(MigrateV1ToV2);
db.register_migration_boxed(MigrateV2ToV3);
let applied = db.run_migrations()?; // e.g. 2 on first launch after upgrade
```

---

## API summary

| Method | Description |
|---|---|
| `DBWrapper::new(path)` | Open/create database files |
| `db.init_tables(\|db\| { ... })` | Register tables via closure |
| `db.register_table(def)` | Register a regular table |
| `db.register_ttl_table(def, ttl)` | Register a TTL table |
| `db.next(id) -> u64` | Atomic counter increment (~8 ns) |
| `db.write(def, key, value)` | Buffer a write |
| `db.write_ttl(def, key, value, ttl)` | Buffer a TTL write (per-write TTL preserved on flush) |
| `db.get_buffered(table, key_bytes) -> Option<Vec<u8>>` | Look up in buffers |
| `db.register_migration(Arc<dyn Migration>)` | Register a schema migration step |
| `db.register_migration_boxed(m)` | Register an owned migration |
| `db.schema_version() -> u64` | Current schema version (0 = fresh) |
| `db.run_migrations() -> usize` | Apply pending migrations sequentially |
| `db.flush_counters()` | Persist dirty counters |
| `db.flush_buffers()` | Persist all buffered writes |
| `db.compact()` | Run redb compaction |
| `db.backup()` | Flush buffers, then create timestamped backup (`BACKUP_DIR` env, default `backups/`) |
| `DBWrapper::with_cache_size(path, policy, bytes)` | Open with a capped page cache for the main DB (default: 25 % RAM, 16 MiB–1 GiB) |
| `db.pending_writes() -> usize` | Buffered entries count |

---

## Conventions

| Area | Convention |
|------|-----------|
| Naming | `snake_case` modules/files, `CamelCase` types |
| Errors | `thiserror` derive, `#[from]` for auto-conversion |
| Logging | `log` crate — `debug!` for flush counts, `error!` for failures |
| Visibility | `pub` for public API, `pub(crate)` for internals |
| Unsafe | `as_self_type_ref` for redb Value serialisation only |

---

## Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `shodh-redb` | 0.6 (umzi2 fork, `perf/upstream-port`) | Embedded K/V database (TTL fork) |
| `hashbrown` | 0.15 | Buffer maps: `entry_ref` (one hash, lazy key allocation) + foldhash |
| `parking_lot` | 0.12 | Non-poisoning, faster `Mutex` |
| `log` | 0.4 | Lightweight logging facade |
| `thiserror` | 2.0 | Derive `Error` for enums |
| `criterion` | 0.5 | Dev — benchmarks |

---

## Performance notes

- Buffer maps use `hashbrown` (foldhash, per-process random seed): one hash per
  `push`; an overwrite of a pending key reuses its allocations (zero allocs).
- `BufferStore` keeps the total entry count incrementally (`Inner.len`), so
  `push` is O(1) regardless of table count.
- Flush inserts each table's batch in **ascending key order** (`sorted_by_key`, using the table's
  own `Key::compare`).  This only pays off with the fork's ascending-insert leaf packing: sequential
  ids then append to dense leaves (500k rows: 585 ms vs 773-1035 ms hash order, RSS 163 vs 227 MB).
  On crates.io `shodh-redb 0.6.0` the same order halves leaf fill (257 -> 514 MiB file), so
  `Cargo.toml` points at the fork.  Random keys gain ~3 % from locality.
- `GroupedNonDurable` makes every 256th flush a durable checkpoint.  shodh-redb refuses further
  `Durability::None` commits after 10 000 pending ones; at the default 10 s flush interval that is
  ~28 h, after which every flush failed.
- `CounterStore` slots are cache-line padded (`#[repr(align(64))]`, 16 KiB):
  4 threads on distinct ids scale ~23× better than with packed `AtomicU64`s.
- Open/register skip write transactions when tables already exist
  (`ensure_table` checks in a read txn) — no fsync per table on restart.
- Counter DB uses a fixed 1 MiB page cache.
- TTL entries expire `ttl` after the **write**, not after the flush that
  persists them (`PendingValue.deadline`).
- Known non-optimisable cost: `Database` drop = ~3.3 ms per file (redb marks the
  file clean with an fsync); two files per `DBWrapper`.
