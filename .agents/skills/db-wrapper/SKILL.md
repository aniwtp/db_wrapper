---
name: db-wrapper
description: Best practices, usage patterns, and anti-patterns for the db-wrapper crate (embedded redb with atomic counters, unified buffers, read-through lookups).
---

# db-wrapper — Best Practices & Anti-patterns

Use this skill when writing, reviewing, or refactoring code that uses `db-wrapper`.

---

## Best Practices

### 1. Register tables once at startup

```rust
// ✅ Good: single init_tables call in main()
db.init_tables(|db| {
    db.register_table(TEAM_PASS);
    db.register_table(USERS);
});
```

```rust
// ❌ Bad: register_table scattered across handlers
async fn create_team(db: State<DBWrapper>) {
    db.register_table(TEAM_PASS); // every request!
    db.write(TEAM_PASS, ...)
}
```

### 2. Flush buffers externally on a timer

`db-wrapper` has **no internal async loop**. The host app is responsible:

```rust
// ✅ Good: spawned timer in main()
drop(ntex::rt::spawn(async move {
    let interval = Duration::from_secs(BUFFER_FLUSH_SECS);
    loop {
        ntex::time::sleep(interval).await;
        db.flush_buffers()?;
    }
}));
```

```rust
// ❌ Bad: flushing on every write
db.write(TABLE, key, val)?;
db.flush_buffers()?; // defeats the purpose of buffering
```

### 3. Use `get_buffered` + DB fallback for reads

```rust
// ✅ Good: buffer-first, then DB
let key = serialize_value(&id);
if let Some(bytes) = db.get_buffered(TABLE.name(), &key) {
    return String::from_bytes(&bytes);
}
// fallback
let tx = db.db.begin_read()?;
let table = tx.open_table(TABLE)?;
table.get(id)?.map(|v| v.value())
```

```rust
// ❌ Bad: reading DB directly, ignoring buffer
let tx = db.db.begin_read()?;
table.get(id) // may miss unflushed writes
```

### 4. Keep counter IDs stable and documented

```rust
// ✅ Good: named constants
const COUNTER_TEAM_ID: u8 = 0;
const COUNTER_USER_ID: u8 = 1;

let team_id = db.next(COUNTER_TEAM_ID);
let user_id = db.next(COUNTER_USER_ID);
```

```rust
// ❌ Bad: magic numbers scattered everywhere
db.next(0);  // what is 0?
db.next(7);  // what is 7?
```

### 5. Use `TeamRow` type aliases for complex table values

```rust
// ✅ Good
pub type TeamRow = (String, String, Vec<String>, Vec<(u8, String)>, bool);
pub const TEAM_PASS: TableDefinition<u64, TeamRow> = ...;
```

```rust
// ❌ Bad: inline complex types
pub const TEAM_PASS: TableDefinition<u64, (String, String, Vec<String>, Vec<(u8, String)>, bool)> = ...;
```

### 6. Flush buffers + counters before shutdown

```rust
// ✅ Good: graceful shutdown
db.flush_buffers()?;
db.flush_counters()?;
// now safe to exit
```

```rust
// ❌ Bad: exit without flushing
// unrecoverable data loss
```

---

## Lower-priority Practices

### 7. Adjust `BUFFER_MAX_ENTRIES` for your workload

Default is 10 000. For high-throughput apps, increase. For memory-constrained, decrease.

```rust
// When creating a custom BufferStore:
BufferStore::new(50_000) // larger batches, fewer disk writes
```

### 8. Use `write_ttl` for temporary data

```rust
db.register_ttl_table(SESSIONS, Duration::from_secs(3600));
db.write_ttl(SESSIONS, token, data, Duration::from_secs(3600))?;
// Auto-expires in redb after 1 hour
```

### 9. Monitor buffer size

```rust
let pending = db.pending_writes();
if pending > 5_000 {
    log::warn!("buffer backlog: {pending} entries");
}
```

### 10. Separate counter DB from data DB

Already done automatically — `{path}` for data, `{path}.counter` for counters. No action needed.

---

## Anti-patterns

### ❌ Calling `register_table` per request

Table registration writes to redb (creates the table if needed). Doing this on every request adds unnecessary latency.

```rust
// ❌ Anti-pattern
async fn handler(db: State<DBWrapper>) {
    db.register_table(MY_TABLE); // every request!
}
```

### ❌ Flushing after every write

Buffering exists to batch writes. Flushing after each write is equivalent to direct DB writes.

```rust
// ❌ Anti-pattern
for item in items {
    db.write(TABLE, item.id, item)?;
    db.flush_buffers()?; // defeats buffering
}
```

### ❌ Skipping `get_buffered` and reading DB directly after a write

After `db.write()`, the data is in the buffer, not on disk. Reading DB directly returns stale data.

```rust
// ❌ Anti-pattern
db.write(TABLE, 1, "hello")?;
let tx = db.db.begin_read()?; // won't see "hello" yet
```

### ❌ Using counter IDs beyond 0..=255

`CounterStore` has 256 slots (`u8` range). `db.next(256)` will panic (index out of bounds).

```rust
// ❌ Anti-pattern
db.next(255); // OK — last slot
db.next(256); // PANIC
```

### ❌ Sharing one DBWrapper across multiple redb paths

`DBWrapper::new(path)` opens `{path}` and `{path}.counter`. Using different paths for different instances is fine, but sharing one instance across unrelated data is not.

```rust
// ❌ Anti-pattern
let db = DBWrapper::new("teams.redb")?;
db.register_table(USERS); // users in teams.redb — confusing
```

### ❌ Blocking the async runtime with long flush

`flush_buffers()` is synchronous and may take time with many entries. Don't call it inside an async handler on the main thread.

```rust
// ❌ Anti-pattern
async fn handler(db: State<DBWrapper>) -> impl Responder {
    db.flush_buffers()?; // blocks the async thread
}
```

### ❌ Ignoring flush errors

Flush failures mean data loss. Always log and handle.

```rust
// ❌ Anti-pattern
let _ = db.flush_buffers(); // silent data loss
```

```rust
// ✅ Good
if let Err(e) = db.flush_buffers() {
    log::error!("flush failed: {e}");
}
```
