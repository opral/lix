//! Opt-in SQL profiling probe for registered-entity `RETURNING` writes.
//!
//! The timer encloses only the target write. Each sample receives a fresh
//! in-memory lix, registered schema, and (for UPDATE) seeded rows before
//! timing starts, so fixture construction and seed writes are excluded. Cases
//! are rotated on every round to avoid a fixed run-order advantage.
//!
//! ```text
//! LIX_RETURNING_PROFILE=1 cargo bench -p lix --bench registered_entity_returning
//!
//! LIX_RETURNING_PROFILE=1 LIX_RETURNING_PROFILE_ROWS=10000 \
//! LIX_RETURNING_PROFILE_ROUNDS=15 LIX_RETURNING_PROFILE_OPERATIONS=insert \
//!   cargo bench -p lix --bench registered_entity_returning
//! ```
//!
//! `LIX_RETURNING_PROFILE_ROWS`, `LIX_RETURNING_PROFILE_ROUNDS`, and
//! `LIX_RETURNING_PROFILE_WARMUP_ROUNDS` are positive integer environment
//! variables. `LIX_RETURNING_PROFILE_OPERATIONS` accepts a comma-separated
//! subset of `insert,update`; each selected operation always measures both
//! its ordinary and RETURNING forms. Set
//! `LIX_RETURNING_PROFILE_PROJECTION=wildcard` to exercise `RETURNING *`,
//! which includes transaction-derived audit fields and its staged postimage
//! lookup; the default `columns` measures `RETURNING id, payload`. Set
//! `LIX_RETURNING_PROFILE_EXPLICIT_PRESTAGED=1` to also measure one write in
//! an explicit transaction after `LIX_RETURNING_PROFILE_ROWS` prior rows have
//! been staged. With `projection=wildcard`, that isolates the statement
//! checkpoint needed by staged postimage projection.

use std::fmt::Write as _;
use std::hint::black_box;
use std::time::{Duration, Instant};

use lix::ExecuteResult;
use lix::storage::Memory;
use lix::{Lix, open_lix};

const DEFAULT_ROWS: usize = 1_000;
const DEFAULT_ROUNDS: usize = 11;
const DEFAULT_WARMUP_ROUNDS: usize = 1;
const ENTITY_TABLE: &str = "benchmark_returning_entity";
const REGISTER_SCHEMA_SQL: &str = "INSERT INTO lix_registered_schema \
    (schema_key, value, lixcol_global, lixcol_untracked) VALUES (\
    'benchmark_returning_entity', CAST('{\"$schema\":\"https://lix.dev/schema-v1.json\",\
    \"key\":\"benchmark_returning_entity\",\"columns\":[\
    {\"name\":\"id\",\"type\":\"text\",\"nullable\":false},\
    {\"name\":\"payload\",\"type\":\"text\",\"nullable\":false}],\
    \"primary_key\":[\"id\"]}' AS JSONB), false, false)";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    Insert,
    Update,
}

impl Operation {
    const fn name(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Update => "update",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Case {
    operation: Operation,
    returning: bool,
}

const CASES: [Case; 4] = [
    Case {
        operation: Operation::Insert,
        returning: false,
    },
    Case {
        operation: Operation::Insert,
        returning: true,
    },
    Case {
        operation: Operation::Update,
        returning: false,
    },
    Case {
        operation: Operation::Update,
        returning: true,
    },
];

#[derive(Clone, Copy, Debug)]
struct OperationSelection {
    insert: bool,
    update: bool,
}

impl OperationSelection {
    fn from_env() -> Self {
        let Some(raw) = std::env::var_os("LIX_RETURNING_PROFILE_OPERATIONS") else {
            return Self {
                insert: true,
                update: true,
            };
        };

        let raw = raw
            .into_string()
            .unwrap_or_else(|_| panic!("LIX_RETURNING_PROFILE_OPERATIONS must be valid UTF-8"));
        let mut selection = Self {
            insert: false,
            update: false,
        };
        for operation in raw
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
        {
            match operation {
                "insert" => selection.insert = true,
                "update" => selection.update = true,
                _ => panic!(
                    "LIX_RETURNING_PROFILE_OPERATIONS supports only 'insert' and 'update', got {operation:?}"
                ),
            }
        }
        assert!(
            selection.insert || selection.update,
            "LIX_RETURNING_PROFILE_OPERATIONS must select at least one operation"
        );
        selection
    }

    const fn includes(self, operation: Operation) -> bool {
        match operation {
            Operation::Insert => self.insert,
            Operation::Update => self.update,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReturningProjection {
    Columns,
    Wildcard,
}

impl ReturningProjection {
    fn from_env() -> Self {
        match std::env::var("LIX_RETURNING_PROFILE_PROJECTION") {
            Ok(value) if value == "wildcard" => Self::Wildcard,
            Ok(value) if value == "columns" => Self::Columns,
            Ok(value) => panic!(
                "LIX_RETURNING_PROFILE_PROJECTION supports only 'columns' or 'wildcard', got {value:?}"
            ),
            Err(std::env::VarError::NotPresent) => Self::Columns,
            Err(std::env::VarError::NotUnicode(_)) => {
                panic!("LIX_RETURNING_PROFILE_PROJECTION must be valid UTF-8")
            }
        }
    }

    const fn sql(self) -> &'static str {
        match self {
            Self::Columns => "id, payload",
            Self::Wildcard => "*",
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Columns => "columns",
            Self::Wildcard => "wildcard",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Config {
    rows: usize,
    rounds: usize,
    warmup_rounds: usize,
    operations: OperationSelection,
    returning_projection: ReturningProjection,
    explicit_pre_staged: bool,
}

impl Config {
    fn from_env() -> Self {
        Self {
            rows: positive_env_usize("LIX_RETURNING_PROFILE_ROWS", DEFAULT_ROWS),
            rounds: positive_env_usize("LIX_RETURNING_PROFILE_ROUNDS", DEFAULT_ROUNDS),
            warmup_rounds: positive_env_usize(
                "LIX_RETURNING_PROFILE_WARMUP_ROUNDS",
                DEFAULT_WARMUP_ROUNDS,
            ),
            operations: OperationSelection::from_env(),
            returning_projection: ReturningProjection::from_env(),
            explicit_pre_staged: enabled_env_flag("LIX_RETURNING_PROFILE_EXPLICIT_PRESTAGED"),
        }
    }
}

struct SqlPlans {
    rows: usize,
    returning_projection: ReturningProjection,
    insert: String,
    insert_returning: String,
    update: String,
    update_returning: String,
}

impl SqlPlans {
    fn new(rows: usize, returning_projection: ReturningProjection) -> Self {
        let mut values = String::with_capacity(rows.saturating_mul(32));
        for index in 0..rows {
            if index > 0 {
                values.push_str(", ");
            }
            write!(values, "('{}', 'before')", row_id(index)).expect("write benchmark INSERT row");
        }

        let insert = format!("INSERT INTO {ENTITY_TABLE} (id, payload) VALUES {values}");
        let insert_returning = format!("{insert} RETURNING {}", returning_projection.sql());
        let update = format!("UPDATE {ENTITY_TABLE} SET payload = 'after'");
        let update_returning = format!("{update} RETURNING {}", returning_projection.sql());
        Self {
            rows,
            returning_projection,
            insert,
            insert_returning,
            update,
            update_returning,
        }
    }

    fn sql(&self, case: Case) -> &str {
        match (case.operation, case.returning) {
            (Operation::Insert, false) => &self.insert,
            (Operation::Insert, true) => &self.insert_returning,
            (Operation::Update, false) => &self.update,
            (Operation::Update, true) => &self.update_returning,
        }
    }
}

fn main() {
    if std::env::var_os("LIX_RETURNING_PROFILE").is_none() {
        print_usage();
        return;
    }

    let config = Config::from_env();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create registered-entity RETURNING profile runtime");
    runtime.block_on(run(config));
}

fn print_usage() {
    println!(
        "registered_entity_returning is opt-in; run \
         LIX_RETURNING_PROFILE=1 cargo bench -p lix --bench registered_entity_returning"
    );
}

async fn run(config: Config) {
    let plans = SqlPlans::new(config.rows, config.returning_projection);
    println!(
        "registered_entity_returning suite=registered_entity_write rows={} rounds={} \
         warmup_rounds={} projection={} operations=insert:{},update:{}",
        config.rows,
        config.rounds,
        config.warmup_rounds,
        config.returning_projection.name(),
        config.operations.insert,
        config.operations.update,
    );

    for round in 0..config.warmup_rounds {
        run_round(&plans, config.operations, round, None).await;
    }

    let mut samples: [Vec<u128>; CASES.len()] =
        std::array::from_fn(|_| Vec::with_capacity(config.rounds));
    for round in 0..config.rounds {
        run_round(&plans, config.operations, round, Some(&mut samples)).await;
    }

    for (index, case) in CASES.into_iter().enumerate() {
        let sample_set = &samples[index];
        if sample_set.is_empty() {
            continue;
        }
        let p50 = median_ns(sample_set);
        println!(
            "registered_entity_returning summary operation={} returning={} rows={} \
             rounds={} p50_ns={p50} median_ns={p50} p50_ns_per_row={:.3}",
            case.operation.name(),
            case.returning,
            plans.rows,
            sample_set.len(),
            p50 as f64 / plans.rows as f64,
        );
    }

    print_returning_overhead(&samples, Operation::Insert, plans.rows);
    print_returning_overhead(&samples, Operation::Update, plans.rows);
    if config.explicit_pre_staged {
        run_explicit_pre_staged_profile(&plans, config.rounds, config.warmup_rounds).await;
    }
}

async fn run_round(
    plans: &SqlPlans,
    operations: OperationSelection,
    round: usize,
    mut samples: Option<&mut [Vec<u128>; CASES.len()]>,
) {
    for offset in 0..CASES.len() {
        let case_index = (round + offset) % CASES.len();
        let case = CASES[case_index];
        if !operations.includes(case.operation) {
            continue;
        }

        let elapsed = measure_case(plans, case).await;
        if let Some(samples) = samples.as_deref_mut() {
            samples[case_index].push(elapsed.as_nanos());
            println!(
                "registered_entity_returning sample operation={} returning={} rows={} \
                 round={round} elapsed_ns={}",
                case.operation.name(),
                case.returning,
                plans.rows,
                elapsed.as_nanos(),
            );
        }
    }
}

async fn measure_case(plans: &SqlPlans, case: Case) -> Duration {
    let session = match case.operation {
        Operation::Insert => new_fixture().await,
        Operation::Update => seeded_fixture(plans).await,
    };
    let sql = plans.sql(case);
    let started = Instant::now();
    let result = session.execute(sql, &[]).await.unwrap_or_else(|error| {
        panic!(
            "registered-entity {} benchmark SQL failed: {error:?}\\nSQL: {sql}",
            case.operation.name()
        )
    });
    let elapsed = started.elapsed();

    assert_result(case, &result, plans.rows, plans.returning_projection);
    black_box(result);
    elapsed
}

/// Measures the explicit-transaction path after it already owns a large
/// staged journal. The ordinary projection skips the checkpoint because it is
/// evaluated before stage; wildcard includes audit columns and exercises the
/// post-stage checkpoint path.
async fn run_explicit_pre_staged_profile(plans: &SqlPlans, rounds: usize, warmup_rounds: usize) {
    println!(
        "registered_entity_returning explicit_pre_staged rows={} rounds={} warmup_rounds={} \
         projection={}",
        plans.rows,
        rounds,
        warmup_rounds,
        plans.returning_projection.name(),
    );
    for round in 0..warmup_rounds {
        let returning = round % 2 == 1;
        measure_explicit_pre_staged_case(plans, returning).await;
    }

    let mut plain = Vec::with_capacity(rounds);
    let mut returning = Vec::with_capacity(rounds);
    for round in 0..rounds {
        for returning_case in [round % 2 == 1, round % 2 == 0] {
            let elapsed = measure_explicit_pre_staged_case(plans, returning_case).await;
            let samples = if returning_case {
                &mut returning
            } else {
                &mut plain
            };
            samples.push(elapsed.as_nanos());
            println!(
                "registered_entity_returning explicit_pre_staged sample returning={} \
                 rows={} round={round} elapsed_ns={}",
                returning_case,
                plans.rows,
                elapsed.as_nanos(),
            );
        }
    }
    let plain_p50 = median_ns(&plain);
    let returning_p50 = median_ns(&returning);
    println!(
        "registered_entity_returning explicit_pre_staged comparison rows={} \
         projection={} non_returning_p50_ns={} returning_p50_ns={} \
         returning_to_non_returning_ratio={:.3}x",
        plans.rows,
        plans.returning_projection.name(),
        plain_p50,
        returning_p50,
        returning_p50 as f64 / plain_p50 as f64,
    );
}

async fn measure_explicit_pre_staged_case(plans: &SqlPlans, returning: bool) -> Duration {
    let session = new_fixture().await;
    let mut transaction = session
        .begin_transaction()
        .await
        .expect("open explicit registered-entity RETURNING benchmark transaction");
    let seeded = transaction
        .execute(&plans.insert, &[])
        .await
        .expect("seed explicit registered-entity RETURNING benchmark rows");
    assert_eq!(
        seeded.rows_affected(),
        u64::try_from(plans.rows).expect("benchmark row count fits u64")
    );

    let mut sql = format!(
        "INSERT INTO {ENTITY_TABLE} (id, payload) \
         VALUES ('explicit-returning-checkpoint-target', 'after')"
    );
    if returning {
        write!(sql, " RETURNING {}", plans.returning_projection.sql())
            .expect("write explicit benchmark RETURNING projection");
    }
    let started = Instant::now();
    let result = transaction
        .execute(&sql, &[])
        .await
        .unwrap_or_else(|error| {
            panic!(
                "explicit registered-entity RETURNING benchmark SQL failed: {error:?}\nSQL: {sql}"
            )
        });
    let elapsed = started.elapsed();

    assert_eq!(result.rows_affected(), 1);
    if returning {
        assert_eq!(result.len(), 1);
    } else {
        assert!(result.is_empty());
    }
    black_box(result);
    transaction
        .rollback()
        .await
        .expect("rollback explicit registered-entity RETURNING benchmark transaction");
    elapsed
}

async fn new_fixture() -> Lix<Memory> {
    let storage = Memory::new();
    let session = open_lix()
        .with_storage(storage)
        .await
        .expect("open registered-entity RETURNING benchmark lix");
    let registration = session
        .execute(REGISTER_SCHEMA_SQL, &[])
        .await
        .expect("register benchmark entity schema");
    assert_eq!(registration.rows_affected(), 1);
    session
}

async fn seeded_fixture(plans: &SqlPlans) -> Lix<Memory> {
    let session = new_fixture().await;
    let seed = session
        .execute(&plans.insert, &[])
        .await
        .expect("seed registered-entity UPDATE benchmark rows");
    assert_eq!(
        seed.rows_affected(),
        u64::try_from(plans.rows).expect("benchmark row count fits u64")
    );
    session
}

fn assert_result(
    case: Case,
    result: &ExecuteResult,
    rows: usize,
    returning_projection: ReturningProjection,
) {
    assert_eq!(
        result.rows_affected(),
        u64::try_from(rows).expect("benchmark row count fits u64"),
        "{} benchmark should affect every fixture row",
        case.operation.name()
    );
    if case.returning {
        assert_eq!(result.len(), rows);
        match returning_projection {
            ReturningProjection::Columns => assert_eq!(
                result
                    .columns()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                vec!["id", "payload"],
                "RETURNING result should preserve its selected columns"
            ),
            ReturningProjection::Wildcard => {
                assert!(result.columns().iter().any(|column| column == "id"));
                assert!(result.columns().iter().any(|column| column == "payload"));
                assert!(
                    result
                        .columns()
                        .iter()
                        .any(|column| column == "lixcol_commit_id"),
                    "RETURNING * should exercise the staged audit postimage path"
                );
            }
        }
    } else {
        assert!(result.is_empty());
        assert!(result.columns().is_empty());
    }
}

fn print_returning_overhead(samples: &[Vec<u128>; CASES.len()], operation: Operation, rows: usize) {
    let plain_index = CASES
        .iter()
        .position(|case| case.operation == operation && !case.returning)
        .expect("non-RETURNING case is registered");
    let returning_index = CASES
        .iter()
        .position(|case| case.operation == operation && case.returning)
        .expect("RETURNING case is registered");
    let plain = &samples[plain_index];
    let returning = &samples[returning_index];
    if plain.is_empty() || returning.is_empty() {
        return;
    }

    let plain_p50 = median_ns(plain);
    let returning_p50 = median_ns(returning);
    println!(
        "registered_entity_returning comparison operation={} rows={rows} \
         non_returning_p50_ns={plain_p50} returning_p50_ns={returning_p50} \
         returning_to_non_returning_ratio={:.3}x",
        operation.name(),
        returning_p50 as f64 / plain_p50 as f64,
    );
}

fn median_ns(samples: &[u128]) -> u128 {
    assert!(!samples.is_empty(), "median needs at least one sample");
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let upper = sorted[sorted.len() / 2];
    let lower = sorted[(sorted.len() - 1) / 2];
    lower + (upper - lower) / 2
}

fn positive_env_usize(name: &str, default: usize) -> usize {
    let Some(value) = std::env::var_os(name) else {
        return default;
    };
    let value = value
        .into_string()
        .unwrap_or_else(|_| panic!("{name} must be valid UTF-8"));
    let parsed = value
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("{name} must be a positive integer, got {value:?}"));
    assert!(
        parsed > 0,
        "{name} must be a positive integer, got {value:?}"
    );
    parsed
}

fn enabled_env_flag(name: &str) -> bool {
    let Some(value) = std::env::var_os(name) else {
        return false;
    };
    match value.to_str() {
        Some("1" | "true") => true,
        Some("0" | "false") => false,
        Some(value) => panic!("{name} must be one of '1', '0', 'true', or 'false', got {value:?}"),
        None => panic!("{name} must be valid UTF-8"),
    }
}

fn row_id(index: usize) -> String {
    format!("row-{index:012}")
}
