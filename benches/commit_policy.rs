//! Commit-policy benchmark: sequential write throughput under the three
//! commit policies (Durable / Grouped / GroupedNonDurable).
//!
//! Run: cargo bench --bench commit_policy
//! Env: POLICY_BENCH_N — writes per policy (default 100_000), BENCH_DIR — data dir (default /var/tmp)

use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use db_wrapper::{CommitPolicy, DBWrapper};

fn dir_for(label: &str) -> PathBuf {
    // Real disk by default (/var/tmp is ext4 here, /tmp is tmpfs where fsync
    // is free).  Override with BENCH_DIR.
    let dir = std::env::var_os("BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/var/tmp"))
        .join(format!("commit_bench_{label}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

const TABLE: shodh_redb::TableDefinition<u64, String> =
    shodh_redb::TableDefinition::new("policy_bench");

fn bench_policy(n: u64, policy: CommitPolicy) -> std::time::Duration {
    let dir = dir_for(&format!("{policy:?}").to_lowercase());
    let path = dir.join("bench.redb");
    let db = DBWrapper::with_commit_policy(path.to_str().unwrap(), policy).expect("open");
    db.register_table(TABLE);

    // value ~200 bytes to look like a realistic record
    let value = "x".repeat(200);

    let t = Instant::now();
    for i in 0..n {
        db.write(TABLE, i, value.clone()).unwrap();
    }
    db.flush_buffers().unwrap();
    let elapsed = t.elapsed();

    drop(db);
    let _ = fs::remove_dir_all(&dir);
    elapsed
}

fn main() {
    let n: u64 = std::env::var("POLICY_BENCH_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);

    println!("commit-policy benchmark: {n} writes (~200B values) per policy\n");
    println!(
        "{:<22} {:>12} {:>12} {:>10}",
        "policy", "total", "per write", "ops/s"
    );

    for policy in [
        CommitPolicy::Durable,
        CommitPolicy::Grouped,
        CommitPolicy::GroupedNonDurable,
    ] {
        let d = bench_policy(n, policy);
        println!(
            "{:<22} {:>9.2?} {:>9.2} µs {:>10.0}",
            format!("{policy:?}"),
            d,
            d.as_micros() as f64 / n as f64,
            n as f64 / d.as_secs_f64()
        );
    }
}
