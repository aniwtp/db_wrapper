# AGENT.md — db-wrapper

Embedded database crate: **redb** (shodh-redb) with in-memory atomic counters,
unified write buffers, and read-through lookups.  No async runtime dependency.

---

## Project tree

```
db-wrapper/
├── Cargo.toml              # shodh-redb + log + thiserror
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

# Tests (20 unit + 4 integration)
cargo test

# Benchmarks
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
| `db.write_ttl(def, key, value, ttl)` | Buffer a TTL write |
| `db.get_buffered(table, key_bytes) -> Option<Vec<u8>>` | Look up in buffers |
| `db.flush_counters()` | Persist dirty counters |
| `db.flush_buffers()` | Persist all buffered writes |
| `db.compact()` | Run redb compaction |
| `db.backup()` | Create timestamped backup |
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
| `shodh-redb` | 0.5 | Embedded K/V database (TTL fork) |
| `log` | 0.4 | Lightweight logging facade |
| `thiserror` | 2.0 | Derive `Error` for enums |
| `criterion` | 0.5 | Dev — benchmarks |
