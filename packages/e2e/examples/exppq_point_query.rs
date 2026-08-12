//! Attribution harness for the inlang `selectBundleNested(...).where("bundle.id","=",id)`
//! point lookup: a three-table (bundle -> message -> variant) join that returns two rows
//! but whose latency grows with total table size.
//!
//! Reports the deterministic `scan_rows_per_query` counter plus the disjoint SQL phase
//! profile for a family of controlled queries across fixture sizes, so the asymptotic
//! claim can be read off row counts rather than timings.
//!
//! Usage: `exppq_point_query [bundle_sizes_csv] [rounds]`
//! (defaults: `10,100,500,2000` and 200 rounds).

use std::time::Duration;

use lix::integration::Engine;
use lix::{Memory, SqlReadProfile, Value};

#[derive(Clone, Copy, PartialEq)]
enum ParamSet {
    /// No bound parameters.
    None,
    /// `$1` = the target bundle id.
    Bundle,
    /// `$1` = the target message id.
    Message,
    /// `$1` = bundle id, `$2` = first message id, `$3` = second message id.
    BundleAndMessages,
}

struct Scenario {
    name: &'static str,
    sql: &'static str,
    expected_rows: usize,
    params: ParamSet,
    /// Rows the provider must examine, when the declared-column index is
    /// expected to serve this query.
    ///
    /// This is an engagement gate, not a statistic. The index's entire value is
    /// conditional on the witness being written on the route real workloads
    /// take, and every functional test still passes when it is not — the
    /// benchmark just quietly reverts to scan speed. Asserting the examined
    /// count here fails loudly instead, because this number is only reachable
    /// through the index.
    index_must_serve: Option<u64>,
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "constant",
        sql: "SELECT 1 AS value",
        expected_rows: 1,
        params: ParamSet::None,
        index_must_serve: None,
    },
    Scenario {
        name: "bundle_pk",
        sql: "SELECT id, declarations FROM bundle WHERE id = $1",
        expected_rows: 1,
        params: ParamSet::Bundle,
        index_must_serve: None,
    },
    // No join. Isolates the foreign-key access path on its own: does an equality
    // predicate on a non-primary-key column have an indexed path?
    Scenario {
        name: "message_fk_only",
        sql: r#"SELECT id, locale FROM message WHERE "bundleId" = $1"#,
        expected_rows: 2,
        params: ParamSet::Bundle,
        index_must_serve: Some(2),
    },
    // Right side joined on its own PRIMARY KEY, driven by a point lookup on the left.
    // Separates "no predicate is propagated across the join" from "the propagated
    // predicate has no index".
    Scenario {
        name: "pk_join_reverse",
        sql: r#"SELECT message.id AS "messageId", bundle.id AS "bundleId" FROM message LEFT JOIN bundle ON bundle.id = message."bundleId" WHERE message.id = $1"#,
        expected_rows: 1,
        params: ParamSet::Message,
        index_must_serve: None,
    },
    Scenario {
        name: "two_table",
        sql: r#"SELECT bundle.id AS "bundleId", message.id AS "messageId", message.locale AS "messageLocale" FROM bundle LEFT JOIN message ON message."bundleId" = bundle.id WHERE bundle.id = $1"#,
        expected_rows: 2,
        params: ParamSet::Bundle,
        index_must_serve: Some(3),
    },
    Scenario {
        name: "two_table_inner",
        sql: r#"SELECT bundle.id AS "bundleId", message.id AS "messageId", message.locale AS "messageLocale" FROM bundle INNER JOIN message ON message."bundleId" = bundle.id WHERE bundle.id = $1"#,
        expected_rows: 2,
        params: ParamSet::Bundle,
        index_must_serve: None,
    },
    Scenario {
        name: "real",
        sql: r#"SELECT bundle.id AS "bundleId", bundle.declarations AS "bundleDeclarations", message.id AS "messageId", message.locale AS "messageLocale", message.selectors AS "messageSelectors", variant.id AS "variantId", variant.matches AS "variantMatches", variant.pattern AS "variantPattern" FROM bundle LEFT JOIN message ON message."bundleId" = bundle.id LEFT JOIN variant ON variant."messageId" = message.id WHERE bundle.id = $1"#,
        expected_rows: 2,
        params: ParamSet::Bundle,
        index_must_serve: None,
    },
    Scenario {
        name: "real_inner",
        sql: r#"SELECT bundle.id AS "bundleId", message.id AS "messageId", variant.id AS "variantId" FROM bundle INNER JOIN message ON message."bundleId" = bundle.id INNER JOIN variant ON variant."messageId" = message.id WHERE bundle.id = $1"#,
        expected_rows: 2,
        params: ParamSet::Bundle,
        index_must_serve: None,
    },
    // Same three tables, but every join predicate is served by an explicit literal.
    // Upper bound for what full predicate propagation could buy.
    Scenario {
        name: "real_manual_predicates",
        sql: r#"SELECT bundle.id AS "bundleId", message.id AS "messageId", variant.id AS "variantId" FROM bundle LEFT JOIN message ON message."bundleId" = bundle.id AND message."bundleId" = $1 LEFT JOIN variant ON variant."messageId" = message.id AND variant."messageId" IN ($2, $3) WHERE bundle.id = $1"#,
        expected_rows: 2,
        params: ParamSet::BundleAndMessages,
        index_must_serve: None,
    },
    // Non-primary-key, non-foreign-key equality. Shows whether the access-path
    // gap is about foreign keys specifically or about every non-PK column.
    Scenario {
        name: "message_nonfk_count",
        sql: "SELECT count(*) AS matches FROM message WHERE locale = 'en'",
        expected_rows: 1,
        params: ParamSet::None,
        index_must_serve: None,
    },
    // Unfiltered full scan of the same table: the floor a filtered scan is
    // compared against.
    Scenario {
        name: "message_count_all",
        sql: "SELECT count(*) AS matches FROM message",
        expected_rows: 1,
        params: ParamSet::None,
        index_must_serve: None,
    },
    // Primary-key equality on the same table as `message_fk_only`.
    Scenario {
        name: "message_pk_only",
        sql: "SELECT id, locale FROM message WHERE id = $1",
        expected_rows: 1,
        params: ParamSet::Message,
        index_must_serve: None,
    },
];

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut args = std::env::args().skip(1);
    let sizes = args
        .next()
        .unwrap_or_else(|| "10,100,500,2000".to_string())
        .split(',')
        .map(|value| value.trim().parse::<usize>().expect("bundle size"))
        .collect::<Vec<_>>();
    let rounds: u32 = args
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(200);
    // `row` writes one row per commit (the OLTP shape the reproduction uses).
    // `bulk` writes each table in one ordered multi-row INSERT, which is the
    // only shape that currently produces an entity columnar row-group layout.
    let bulk = args.next().is_some_and(|value| value == "bulk");

    println!(
        "# exppq point-query attribution | rounds={rounds} | sizes={sizes:?} | storage=Memory | fixture={}",
        if bulk { "bulk" } else { "row" }
    );
    for bundles in sizes {
        run_fixture(bundles, rounds, bulk).await;
    }
}

async fn run_fixture(bundles: usize, rounds: u32, bulk: bool) {
    let storage = Memory::default();
    Engine::initialize(storage.clone())
        .await
        .expect("initialize fixture");
    let engine = Engine::new(storage).await.expect("open engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open workspace session");

    for schema in schemas() {
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("register schema");
    }
    if bulk {
        seed_bulk(&session, bundles).await;
    } else {
        seed_row_by_row(&session, bundles).await;
    }

    let target = bundles / 2;
    let bundle_id = Value::Text(format!("bundle-{target}"));
    let message_en = Value::Text(format!("message-{target}-en"));
    let message_de = Value::Text(format!("message-{target}-de"));

    for scenario in SCENARIOS {
        let params: Vec<Value> = match scenario.params {
            ParamSet::None => vec![],
            ParamSet::Bundle => vec![bundle_id.clone()],
            ParamSet::Message => vec![message_en.clone()],
            ParamSet::BundleAndMessages => vec![
                bundle_id.clone(),
                message_en.clone(),
                message_de.clone(),
            ],
        };
        let params = params.as_slice();
        for _ in 0..50 {
            session
                .execute(scenario.sql, params)
                .await
                .unwrap_or_else(|error| panic!("warmup {}: {error}", scenario.name));
        }
        let mut total = SqlReadProfile::default();
        for _ in 0..rounds {
            let (result, profile) = session
                .execute_profiled(scenario.sql, params)
                .await
                .unwrap_or_else(|error| panic!("{}: {error}", scenario.name));
            assert_eq!(
                result.len(),
                scenario.expected_rows,
                "{} returned unexpected row count",
                scenario.name
            );
            accumulate(&mut total, profile);
        }
        if let Some(expected) = scenario.index_must_serve {
            let examined = total.provider_rows_examined / u64::from(rounds);
            assert_eq!(
                examined, expected,
                "{} examined {examined} rows per query at {bundles} bundles, expected {expected}: \
                 the declared-column index is not being consulted. Either no witness was written \
                 on this commit route, or the predicate was not recognised as indexable.",
                scenario.name
            );
        }
        report(bundles, scenario.name, total, rounds);
    }
}

async fn seed_row_by_row(session: &lix::integration::SessionContext<Memory>, bundles: usize) {
    for index in 0..bundles {
        let bundle = format!("bundle-{index}");
        session
            .execute(
                "INSERT INTO bundle (id, declarations) VALUES ($1, lix_json('[]'))",
                &[Value::Text(bundle.clone())],
            )
            .await
            .expect("insert bundle");
        for locale in ["en", "de"] {
            let message = format!("message-{index}-{locale}");
            session
                .execute(
                    r#"INSERT INTO message (id, "bundleId", locale, selectors) VALUES ($1, $2, $3, lix_json('[]'))"#,
                    &[
                        Value::Text(message.clone()),
                        Value::Text(bundle.clone()),
                        Value::Text(locale.into()),
                    ],
                )
                .await
                .expect("insert message");
            session
                .execute(
                    r#"INSERT INTO variant (id, "messageId", matches, pattern) VALUES ($1, $2, lix_json('[]'), lix_json('[{"type":"text","value":"fixture"}]'))"#,
                    &[
                        Value::Text(format!("variant-{index}-{locale}")),
                        Value::Text(message),
                    ],
                )
                .await
                .expect("insert variant");
        }
    }
}

async fn seed_bulk(session: &lix::integration::SessionContext<Memory>, bundles: usize) {
    let bundle_values = (0..bundles)
        .map(|index| format!("('bundle-{index}', lix_json('[]'))"))
        .collect::<Vec<_>>()
        .join(", ");
    session
        .execute(
            &format!("INSERT INTO bundle (id, declarations) VALUES {bundle_values}"),
            &[],
        )
        .await
        .expect("bulk insert bundles");

    let message_values = (0..bundles)
        .flat_map(|index| {
            ["de", "en"].into_iter().map(move |locale| {
                format!("('message-{index}-{locale}', 'bundle-{index}', '{locale}', lix_json('[]'))")
            })
        })
        .collect::<Vec<_>>()
        .join(", ");
    session
        .execute(
            &format!(
                r#"INSERT INTO message (id, "bundleId", locale, selectors) VALUES {message_values}"#
            ),
            &[],
        )
        .await
        .expect("bulk insert messages");

    let variant_values = (0..bundles)
        .flat_map(|index| {
            ["de", "en"].into_iter().map(move |locale| {
                format!(
                    r#"('variant-{index}-{locale}', 'message-{index}-{locale}', lix_json('[]'), lix_json('[{{"type":"text","value":"fixture"}}]'))"#
                )
            })
        })
        .collect::<Vec<_>>()
        .join(", ");
    session
        .execute(
            &format!(
                r#"INSERT INTO variant (id, "messageId", matches, pattern) VALUES {variant_values}"#
            ),
            &[],
        )
        .await
        .expect("bulk insert variants");
}

fn accumulate(out: &mut SqlReadProfile, value: SqlReadProfile) {
    out.total += value.total;
    out.logical_planning += value.logical_planning;
    out.physical_planning += value.physical_planning;
    out.arrow_execution += value.arrow_execution;
    out.public_result_materialization += value.public_result_materialization;
    out.scan_elapsed += value.scan_elapsed;
    out.scan_rows += value.scan_rows;
    out.scan_batches += value.scan_batches;
    out.scan_arrow_bytes += value.scan_arrow_bytes;
    out.provider_rows_examined += value.provider_rows_examined;
}

fn micros(value: Duration, rounds: u32) -> f64 {
    value.as_secs_f64() * 1_000_000.0 / f64::from(rounds)
}

fn report(bundles: usize, name: &str, profile: SqlReadProfile, rounds: u32) {
    let accounted = profile.logical_planning
        + profile.physical_planning
        + profile.arrow_execution
        + profile.public_result_materialization;
    println!(
        "bundles={bundles} scenario={name} rounds={rounds} \
         provider_rows_examined_per_query={:.1} \
         scan_rows_per_query={:.1} scan_batches_per_query={:.1} scan_arrow_bytes_per_query={:.1} \
         total_us={:.3} logical_planning_us={:.3} physical_planning_us={:.3} \
         arrow_execution_us={:.3} result_materialization_us={:.3} unattributed_us={:.3} \
         scan_elapsed_us={:.3}",
        profile.provider_rows_examined as f64 / f64::from(rounds),
        profile.scan_rows as f64 / f64::from(rounds),
        profile.scan_batches as f64 / f64::from(rounds),
        profile.scan_arrow_bytes as f64 / f64::from(rounds),
        micros(profile.total, rounds),
        micros(profile.logical_planning, rounds),
        micros(profile.physical_planning, rounds),
        micros(profile.arrow_execution, rounds),
        micros(profile.public_result_materialization, rounds),
        micros(profile.total.saturating_sub(accounted), rounds),
        micros(profile.scan_elapsed, rounds),
    );
}

fn schemas() -> [serde_json::Value; 3] {
    [
        serde_json::json!({
            "x-lix-key": "bundle",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "declarations": { "type": "array", "items": { "type": "object" }, "default": [] }
            },
            "required": ["id", "declarations"],
            "additionalProperties": false
        }),
        serde_json::json!({
            "x-lix-key": "message",
            "x-lix-primary-key": ["/id"],
            "x-lix-foreign-keys": [{
                "properties": ["/bundleId"],
                "references": { "schemaKey": "bundle", "properties": ["/id"] }
            }],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "bundleId": { "type": "string" },
                "locale": { "type": "string" },
                "selectors": { "type": "array", "items": { "type": "object" }, "default": [] }
            },
            "required": ["id", "bundleId", "locale", "selectors"],
            "additionalProperties": false
        }),
        serde_json::json!({
            "x-lix-key": "variant",
            "x-lix-primary-key": ["/id"],
            "x-lix-foreign-keys": [{
                "properties": ["/messageId"],
                "references": { "schemaKey": "message", "properties": ["/id"] }
            }],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "messageId": { "type": "string" },
                "matches": { "type": "array", "items": { "type": "object" }, "default": [] },
                "pattern": { "type": "array", "items": { "type": "object" }, "default": [] }
            },
            "required": ["id", "messageId", "matches", "pattern"],
            "additionalProperties": false
        }),
    ]
}
