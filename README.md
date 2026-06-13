# micro_tamplate

Backend microservice template on **ntex** + **redb** + **FlatBuffers**.

---

## Quick start

```sh
# 1. Copy and edit config (optional — defaults work out of the box)
cp .env.example .env

# 2. Dev build (info-level logging by default)
cargo build

# 3. Run the server
cargo run

# 4. Manual login test (in another terminal)
cargo test -- test_login_manual --nocapture
```

---

## Project structure

```
tamplate/
├── build.rs                 # flatc codegen with hash-based caching
├── Cargo.toml               # deps + compile-time log level features
├── clippy.toml              # linter rules
├── rustfmt.toml             # formatter rules
├── .env.example             # config reference
├── AGENT.md                 # AI agent instructions
├── README.md                # ← this file
│
├── flatbuffers/             # IDL schemas (source of truth)
│   ├── dto/login.fbs        # LoginRequest / TokenResponse
│   └── types/tokens.fbs     # RSTokens, Bytes21, Bytes11
│
└── src/
    ├── main.rs              # entrypoint
    ├── config.rs            # config! macro (secrets → env → .env)
    ├── logging.rs           # compile-time logger
    │
    ├── bd/mod.rs            # redb wrapper + write buffers
    │
    ├── errors/
    │   ├── mod.rs           # AppError (umbrella)
    │   ├── db.rs            # DbError
    │   ├── config.rs        # ConfigError
    │   └── auth.rs          # AuthError
    │
    ├── routes/
    │   ├── mod.rs           # /v1 scope
    │   └── auth/
    │       ├── mod.rs       # /v1/auth scope
    │       └── login.rs     # POST /v1/auth/login + manual test
    │
    ├── logic/mod.rs         # business logic
    └── generated/           # flatc output (gitignored)
```

---

## Stack

| Component | Choice | Why |
|-----------|--------|-----|
| Runtime | **ntex** on compio (io-uring) | Async, actor-less, no tokio dependency |
| Database | **shodh-redb** (embedded K/V) | Single-file, TTL tables, no daemon |
| Serialisation | **FlatBuffers** | Zero-copy, schema-first, compact wire format |
| Logging | **log** + compile-time levels | Zero runtime cost for stripped levels |
| Config | secrets → env → `.env` | Docker/K8s ready, local dev friendly |
| Errors | **thiserror** | Derive macros, `#[from]` auto-conversion |

---

## Configuration

Priority (first match wins): **`/run/secrets/<KEY>` → `secrets/<KEY>` → env var → `.env`**

```rust
// Returns Option<String> if key not found
let val: Option<String> = config!("SOME_KEY");

// Returns String with fallback default
let host: String = config!("BIND_ADDR", "localhost:8080");
```

| Key | Default | Description |
|-----|---------|-------------|
| `DB_PATH` | `test.redb` | Database file path |
| `BIND_ADDR` | `localhost:8080` | HTTP listen address |

---

## Logging levels

Compile-time feature flags — pick exactly **one**:

```sh
cargo build                                          # info+ (default)
cargo build --no-default-features --features log-trace  # all levels
cargo build --no-default-features --features log-debug  # debug+
cargo build --no-default-features --features log-warn   # warn+
cargo build --no-default-features --features log-error  # error only
cargo build --no-default-features --features log-off    # all stripped
```

Code **below** the chosen level is removed by the compiler — zero runtime overhead.

---

## Endpoints

| Method | Path | Status |
|--------|------|--------|
| `POST` | `/v1/auth/login` | ✅ working (placeholder logic) |

### Login request/response

**Request** — FlatBuffer `LoginRequest`:
- `username: string`
- `password: string`

**Response** — FlatBuffer `TokenResponse`:
- `token: RSTokens { refresh: [u8; 21], session: [u8; 11] }`

Manual test sends a FlatBuffer request via `reqwest`:

```sh
# Terminal 1: start server
cargo run

# Terminal 2: run manual test
cargo test -- test_login_manual --nocapture
```

Example output:
```
→ POST http://localhost:8080/v1/auth/login
  request body: 52 bytes
← 200 OK  (1.23ms)
  response body: 68 bytes
  ✓ FlatBuffer parsed: ...
```

---

## Development commands

```sh
cargo build            # default (info logging)
cargo run              # start server
cargo test             # run all tests
cargo test -- --nocapture  # show println! output
cargo clippy           # lint
cargo fmt              # format
```

---

## Conventions

- `snake_case` for modules/files, `CamelCase` for types
- `use crate::module::Type` — no `super::`
- Errors via `thiserror`, `#[from]` for auto-conversion
- Always include module/endpoint name in log messages
- Handlers are `async fn` returning `impl Responder`
- State via `ntex::web::types::State<T>`

---

## Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `ntex` | 3.9 | Web framework (compio) |
| `shodh-redb` | 0.5 | Embedded K/V with TTL |
| `flatbuffers` | 25.12 | Zero-copy serialisation |
| `log` | 0.4 | Logging facade |
| `thiserror` | 2.0 | Error derive |
| `blake3` | 1 | Build-time hash caching |
| `serde` / `serde_json` | 1 | Build-script config |
| `reqwest` | 0.12 | Dev — manual HTTP test client |
