//! Does a JSON-column equality reach the entity provider?
//!
//! Not a product module. `filterable_column_name` returns `None` for
//! `EntityColumnType::Json`, so a JSON-column equality cannot become an
//! `EntityRowFilter`. What that costs is the open question: the predicate is
//! still *answered*, by DataFusion's `FilterExec` one layer above the
//! provider, so the rows are materialised and then discarded.
//!
//! The text spelling (`WHERE v = '{"n":1}'`) is a type error
//! (`LIX_ERROR_TYPE_MISMATCH`) and never runs, so it cannot be the shape under
//! test. This probe uses the spelling that type-checks,
//! `WHERE v = CAST('...' AS JSONB)`, and a String-column arm as the control: same
//! fixture size, same one-row answer, on a column that *is* pushable.
//!
//! Counted at the materialisation boundary (`apply_entity_batch_filters`),
//! above its `filters.is_empty()` early return, because a profile of that
//! function reads identically for "nothing to filter" and "never ran".

use serde_json::json;

use crate::engine::Engine;
use crate::session::SessionContext;
use crate::storage_adapter::Memory;

async fn open_session() -> SessionContext<Memory> {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("engine should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("engine should open");
    engine.open_session().await.expect("session should open")
}

async fn register(session: &SessionContext<Memory>, key: &str) {
    let schema = json!({
        "x-lix-key": key,
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "properties": {
            "id": { "type": "string" },
            "k": { "type": "string" },
            "v": { "type": "object" }
        },
        "required": ["id", "k", "v"],
        "additionalProperties": false
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[crate::Value::Text(schema.to_string())],
        )
        .await
        .expect("schema should register");
}

async fn seed(session: &SessionContext<Memory>, table: &str, count: usize) {
    const CHUNK: usize = 250;
    let mut index = 0;
    while index < count {
        let end = (index + CHUNK).min(count);
        let values = (index..end)
            .map(|i| format!("('r-{i}', 'k-{i}', CAST('{{\"n\":{i}}}' AS JSONB))"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(
                &format!("INSERT INTO {table} (id, k, v) VALUES {values}"),
                &[],
            )
            .await
            .expect("rows should insert");
        index = end;
    }
}

/// The two spellings, same fixture, same one-row answer.
///
/// Counted with `provider_rows_examined`, the engine's own instrument, whose
/// documentation states the exact contract this probe needs: it is recorded
/// *before* a provider applies its row filters, at **every route an entity
/// surface can take**, so a route that never reaches
/// `apply_entity_batch_filters` is still counted. A census at that one
/// function is blind to the columnar and overlay routes, which is how a
/// "zero frames" reading can mean "different route" rather than "did nothing".
/// It is also task-local rather than process-global, so it cannot bleed.
///
/// `scan_rows` is reported alongside it because the pair is what makes the
/// claim falsifiable: a change that only moves filtering earlier in the plan
/// moves `scan_rows` and leaves `provider_rows_examined` alone.
#[tokio::test]
#[ignore = "measurement probe, not a gate"]
async fn json_column_equality_materialization() {
    let n: usize = std::env::var("LIX_JPP_ROWS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(10_000);

    println!("jpp | arm,n,answer_rows,provider_rows_examined,scan_rows,examined_per_answer_row");

    // Control: a String column, which IS pushable. Same fixture size, same
    // one-row answer, and no indexed access path either - so it isolates the
    // JSON refusal from "this column has no access path".
    {
        let session = open_session().await;
        register(&session, "jppstr").await;
        seed(&session, "jppstr", n).await;
        let (result, profile) = session
            .execute_profiled("SELECT id FROM jppstr WHERE k = 'k-7'", &[])
            .await
            .expect("string-column scan should run");
        println!(
            "string,{n},{},{},{},{}",
            result.rows().len(),
            profile.provider_rows_examined,
            profile.scan_rows,
            profile.provider_rows_examined as f64 / result.rows().len().max(1) as f64
        );
        assert_eq!(
            result.rows().len(),
            1,
            "the string arm must answer exactly one row"
        );
    }

    {
        let session = open_session().await;
        register(&session, "jppjson").await;
        seed(&session, "jppjson", n).await;

        // The text spelling is a type error and never runs. Asserted so this
        // probe cannot silently measure the wrong shape.
        let text_spelling = session
            .execute("SELECT id FROM jppjson WHERE v = '{\"n\":7}'", &[])
            .await;
        println!(
            "jpp | text_spelling_err={:?}",
            text_spelling.as_ref().err().map(|error| error.code.clone())
        );
        assert!(
            text_spelling.is_err(),
            "the text spelling must remain a type error"
        );

        let (result, profile) = session
            .execute_profiled(
                "SELECT id FROM jppjson WHERE v = CAST('{\"n\":7}' AS JSONB)",
                &[],
            )
            .await
            .expect("json-column scan should run");
        println!(
            "json,{n},{},{},{},{}",
            result.rows().len(),
            profile.provider_rows_examined,
            profile.scan_rows,
            profile.provider_rows_examined as f64 / result.rows().len().max(1) as f64
        );
        assert_eq!(
            result.rows().len(),
            1,
            "the json arm must answer exactly one row"
        );
    }

    // A primary-key equality, which DOES have an exact access path. Present as
    // the upper bound: it is what "the provider examined only what it needed"
    // looks like on this fixture, so the two arms above can be read against a
    // number the engine can actually reach rather than against zero.
    {
        let session = open_session().await;
        register(&session, "jpppk").await;
        seed(&session, "jpppk", n).await;
        let (result, profile) = session
            .execute_profiled("SELECT id FROM jpppk WHERE id = 'r-7'", &[])
            .await
            .expect("primary-key scan should run");
        println!(
            "primary_key,{n},{},{},{},{}",
            result.rows().len(),
            profile.provider_rows_examined,
            profile.scan_rows,
            profile.provider_rows_examined as f64 / result.rows().len().max(1) as f64
        );
        assert_eq!(result.rows().len(), 1, "the pk arm must answer one row");
    }
}
