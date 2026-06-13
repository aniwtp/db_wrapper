# AGENT.md — micro_tamplate

Backend microservice template: **ntex** (async web framework on compio),
**redb** (embedded K/V database), **flatbuffers** (zero-copy serialisation).

---

## Project tree

```
tamplate/
├── build.rs                 # flatc codegen with caching
├── Cargo.toml               # deps + compile-time log level features
├── clippy.toml
├── rustfmt.toml
├── .env.example             # env reference (secrets/ → env → .env)
├── AGENT.md                 # ← this file
│
├── flatbuffers/             # IDL schemas (source of truth)
│   ├── dto/
│   │   └── login.fbs        # LoginRequest / TokenResponse
│   └── types/
│       └── tokens.fbs       # RSTokens, Bytes21, Bytes11
│
└── src/
    ├── main.rs              # entrypoint: logger init, config, DB, server
    ├── config.rs            # config! macro (secrets → env → .env)
    ├── logging.rs           # stderr logger (compile-time level via features)
    │
    ├── bd/
    │   └── mod.rs           # DBWrapper + WriteBuffer (redb)
    │
    ├── errors/
    │   ├── mod.rs           # AppError (umbrella) + re-exports
    │   ├── db.rs            # DbError
    │   ├── config.rs        # ConfigError
    │   └── auth.rs          # AuthError
    │
    ├── routes/
    │   ├── mod.rs           # /v1 scope
    │   └── auth/
    │       ├── mod.rs       # /v1/auth scope
    │       └── login.rs     # POST /v1/auth/login + #[test] manual test
    │
    ├── logic/
    │   └── mod.rs           # business logic (auth, validation, …)
    │
    └── generated/           # flatc output — DO NOT EDIT
        ├── mod.rs
        ├── dto/
        │   ├── mod.rs
        │   └── login_generated.rs
        └── types/
            ├── mod.rs
            └── tokens_generated.rs
```

---

## Quick commands

```sh
# Dev build (info+ logging)
cargo build

# All logging levels
cargo build --no-default-features --features log-trace

# Production build (error only)
cargo build --release --no-default-features --features log-error

# Lint
cargo clippy

# Format
cargo fmt

# Manual test (start server first, then in another terminal)
cargo test -- test_login_manual --nocapture
```

---

## Architecture

### Web framework — ntex + compio

- **ntex** v3 — async actor-less web framework (like actix-web but lighter).
- Runs on **compio** (io-uring / IOCP), not tokio.
- `#[ntex::main]` macro starts the compio runtime.
- Service config uses `web::scope("/v1").service(auth::scope())`.

### Database — redb (embedded)

- **shodh-redb** v0.5 — fork of redb with TTL tables.
- Single-file embedded K/V, no external process.
- `DBWrapper` wraps `Arc<Database>` + write buffers.
- Write buffers batch inserts by **size** or **time** (auto-flush).

### Serialisation — FlatBuffers

- Schema files live in `flatbuffers/` (IDL source of truth).
- `build.rs` runs `flatc --rust` **only when schemas change** (hash cache in `OUT_DIR`).
- Generated code lands in `src/generated/` (`.gitignore`d).
- Patch step fixes `crate::` paths and adds `#![allow(…)]`.

### Logging — compile-time level

- Levels controlled by **Cargo features**, not runtime config.
- `log-trace`, `log-debug`, `log-info` (default), `log-warn`, `log-error`, `log-off`.
- Code **below** the chosen level is stripped by the compiler (zero runtime cost).
- `trace!` = very detailed (buffer internals, serialisation).
- `debug!` = developer info (buffer sizes, flush counts).
- `info!` = operational milestones (startup, login, maintenance).
- `warn!` = recoverable problems (missing config, force-flush TTL).
- `error!` = failures (compaction fail, flush error).

### Config — `config!` macro

```rust
config!("DB_PATH")              // → Option<String>
config!("BIND_ADDR", "127.0.0.1") // → String with default
```

Priority: **secrets/ files → env vars → .env file** (first match wins).

### Errors — thiserror hierarchy

```text
AppError (umbrella)
├── Db(#[from] DbError)
├── Config(#[from] ConfigError)
├── Auth(#[from] AuthError)
├── Io(#[from] std::io::Error)
├── Logger(#[from] SetLoggerError)
├── Flatbuffer(#[from] InvalidFlatbuffer)
└── Other(String)
```

`main()` returns `Result<(), AppError>` — all `?` work transparently.

---

## How to add a new endpoint

1. **Define the schema** (if needed) — create `flatbuffers/dto/new_thing.fbs`.
2. **Build** — `cargo build` regenerates `src/generated/`.
3. **Add a handler** — create `src/routes/new_module/action.rs`.
4. **Wire the route** — register in the parent `mod.rs`.
5. **Add errors** (if needed) — extend the enum in `src/errors/`.
6. **Add business logic** — implement in `src/logic/`.
7. **Log** at appropriate levels: `debug!` for request/response, `warn!` for bad input, `error!` for failures.

---

## Conventions

| Area | Convention |
|------|-----------|
| Naming | `snake_case` for modules/files, `CamelCase` for types |
| Imports | `use crate::module::Type` — no `super::` |
| Errors | `thiserror` derive, `#[from]` for auto-conversion |
| Logging | Always include table/endpoint name in log messages |
| Async | `ntex::web` handlers are `async fn` returning `impl Responder` |
| State | Pass via `ntex::web::types::State<T>` |

---

## Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `ntex` | 3.9 | Web framework (compio runtime) |
| `shodh-redb` | 0.5 | Embedded K/V database with TTL |
| `flatbuffers` | 25.12 | Zero-copy serialisation |
| `log` | 0.4 | Lightweight logging facade |
| `thiserror` | 2.0 | Derive `Error` for enums |
| `blake3` | 1 | Build-time hash caching |
| `serde` / `serde_json` | 1 | Build-script config |
| `reqwest` | 0.12 | Dev — manual HTTP test client (blocking) |
