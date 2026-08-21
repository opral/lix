//! A point lookup through two foreign-key joins must read a number of stored
//! rows set by its answer, not by the size of the collections it joins.
//!
//! The shape is the inlang `selectBundleNested(...).where("bundle.id","=",id)`
//! query from issue #1337: `bundle -> message -> variant`, two rows out. The
//! first join's equality reaches `message` by ordinary constant propagation,
//! but `variant."message_id"` can only be known once the message rows exist, so
//! before the probe-key access path the `variant` scan was unfiltered and the
//! query read `2N+3` rows at N bundles.
//!
//! `provider_rows_examined` is the metric under test because it counts stored
//! rows a provider *looked at*, before its own row filters. `scan_rows` alone
//! cannot falsify this claim: moving a filter earlier in the plan changes
//! `scan_rows` while the collection is still being read end to end.

use crate::engine::Engine;
use crate::session::SessionContext;
use crate::{Memory, Value};

const NESTED_BUNDLE_SQL: &str = r#"SELECT bundle.id AS "bundle_id", bundle.declarations AS "bundleDeclarations", message.id AS "message_id", message.locale AS "messageLocale", variant.id AS "variantId", variant.pattern AS "variantPattern" FROM bundle LEFT JOIN message ON message."bundle_id" = bundle.id LEFT JOIN variant ON variant."message_id" = message.id WHERE bundle.id = $1"#;

/// Rows the three scans must examine for a bundle with two messages and one
/// variant each: one bundle row, two message rows, two variant rows.
const ANSWER_SIZED_ROWS: u64 = 5;

#[tokio::test]
async fn nested_bundle_point_lookup_reads_rows_proportional_to_its_answer() {
        let mut examined_by_size = Vec::new();
        for bundles in [10_usize, 500] {
            let session = seeded_session(bundles).await;
            let target = Value::Text(format!("bundle-{}", bundles / 2));
            // Warm the statement and physical plan caches so the measured
            // execution takes the same route a repeated application query does.
            let (rows, _) = session
                .execute_profiled(NESTED_BUNDLE_SQL, std::slice::from_ref(&target))
                .await
                .expect("nested bundle query");
            assert_eq!(rows.len(), 2, "{bundles} bundles: two flat rows");
            let (rows, profile) = session
                .execute_profiled(NESTED_BUNDLE_SQL, std::slice::from_ref(&target))
                .await
                .expect("nested bundle query");
            assert_eq!(rows.len(), 2, "{bundles} bundles: two flat rows");
            examined_by_size.push((bundles, profile.provider_rows_examined));
        }

        for (bundles, examined) in &examined_by_size {
            assert_eq!(
                *examined, ANSWER_SIZED_ROWS,
                "at {bundles} bundles the point lookup examined {examined} stored rows; \
                 it must examine {ANSWER_SIZED_ROWS} — one per row its answer is built from. \
                 A count that grows with the fixture means a join key never reached an \
                 indexed access path and a collection was scanned in full."
            );
        }
        let [(_, small), (_, large)] = examined_by_size.as_slice() else {
            panic!("two fixture sizes");
        };
        assert_eq!(
            small, large,
            "rows examined must not grow with the collections being joined"
        );
}

#[tokio::test]
async fn nested_bundle_point_lookup_preserves_left_join_null_extension() {
        let session = seeded_session(4).await;
        // A bundle with no message at all, and a message with no variant. Both
        // are rows the probe-key restriction must not be able to remove.
        session
            .execute(
                "INSERT INTO bundle (id, declarations) VALUES ('bundle-lonely', CAST('[]' AS JSONB))",
                &[],
            )
            .await
            .expect("insert childless bundle");
        session
            .execute(
                "INSERT INTO bundle (id, declarations) VALUES ('bundle-variantless', CAST('[]' AS JSONB))",
                &[],
            )
            .await
            .expect("insert variantless bundle");
        session
            .execute(
                r#"INSERT INTO message (id, "bundle_id", locale, selectors) VALUES ('message-variantless', 'bundle-variantless', 'en', CAST('[]' AS JSONB))"#,
                &[],
            )
            .await
            .expect("insert variantless message");

        let lonely = select_ids(&session, "bundle-lonely").await;
        assert_eq!(
            lonely,
            vec![("bundle-lonely".to_string(), None, None)],
            "a bundle with no message must still produce one null-extended row"
        );

        let variantless = select_ids(&session, "bundle-variantless").await;
        assert_eq!(
            variantless,
            vec![(
                "bundle-variantless".to_string(),
                Some("message-variantless".to_string()),
                None,
            )],
            "a message with no variant must still produce one null-extended row"
        );

        let missing = select_ids(&session, "bundle-does-not-exist").await;
        assert!(
            missing.is_empty(),
            "an unmatched bundle id must produce no rows, got {missing:?}"
        );

        let present = select_ids(&session, "bundle-2").await;
        assert_eq!(
            present,
            vec![
                (
                    "bundle-2".to_string(),
                    Some("message-2-de".to_string()),
                    Some("variant-2-de".to_string()),
                ),
                (
                    "bundle-2".to_string(),
                    Some("message-2-en".to_string()),
                    Some("variant-2-en".to_string()),
                ),
            ],
            "a fully populated bundle must produce both of its rows"
        );
}

#[tokio::test]
async fn nested_bundle_point_lookup_reads_untracked_only_rows() {
    let session = seeded_untracked_session().await;
    let unfiltered = session
        .execute(
            r#"SELECT bundle.id AS bundle_id, message.id AS message_id, variant.id AS variant_id FROM bundle LEFT JOIN message ON message."bundle_id" = bundle.id LEFT JOIN variant ON variant."message_id" = message.id"#,
            &[],
        )
        .await
        .expect("unfiltered nested bundle query");
    assert_eq!(unfiltered.rows().len(), 1);

    let filtered = session
        .execute(
            r#"SELECT bundle.id AS bundle_id, message.id AS message_id, variant.id AS variant_id FROM bundle LEFT JOIN message ON message."bundle_id" = bundle.id LEFT JOIN variant ON variant."message_id" = message.id WHERE bundle.id = $1"#,
            &[Value::Text("bundle-untracked".to_string())],
        )
        .await
        .expect("point-filtered nested bundle query");
    assert_eq!(
        filtered.rows().len(),
        1,
        "an exact parent identity must preserve its untracked child join rows"
    );
    let row = &filtered.rows()[0];
    assert_eq!(
        text(row.value("message_id").expect("message_id column")),
        Some("message-untracked".to_string())
    );
    assert_eq!(
        text(row.value("variant_id").expect("variant_id column")),
        Some("variant-untracked".to_string())
    );
}

type NestedRow = (String, Option<String>, Option<String>);

async fn select_ids(session: &SessionContext<Memory>, bundle_id: &str) -> Vec<NestedRow> {
    let result = session
        .execute(NESTED_BUNDLE_SQL, &[Value::Text(bundle_id.to_string())])
        .await
        .expect("nested bundle query");
    let mut rows = result
        .rows()
        .iter()
        .map(|row| {
            (
                text(row.value("bundle_id").expect("bundle_id column"))
                    .expect("bundle id is never null"),
                text(row.value("message_id").expect("message_id column")),
                text(row.value("variantId").expect("variantId column")),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn text(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        Value::Null => None,
        other => panic!("expected a text or null id, got {other:?}"),
    }
}

async fn seeded_session(bundles: usize) -> SessionContext<Memory> {
    let storage = Memory::default();
    Engine::initialize(storage.clone())
        .await
        .expect("initialize fixture");
    let engine = Engine::new(storage).await.expect("open engine");
    let session = engine.open_session().await.expect("open session");
    for schema in schemas() {
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("register schema");
    }
    for index in 0..bundles {
        let bundle = format!("bundle-{index}");
        session
            .execute(
                "INSERT INTO bundle (id, declarations) VALUES ($1, CAST('[]' AS JSONB))",
                &[Value::Text(bundle.clone())],
            )
            .await
            .expect("insert bundle");
        for locale in ["en", "de"] {
            let message = format!("message-{index}-{locale}");
            session
                .execute(
                    r#"INSERT INTO message (id, "bundle_id", locale, selectors) VALUES ($1, $2, $3, CAST('[]' AS JSONB))"#,
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
                    r#"INSERT INTO variant (id, "message_id", matches, pattern) VALUES ($1, $2, CAST('[]' AS JSONB), CAST('[{"type":"text","value":"fixture"}]' AS JSONB))"#,
                    &[
                        Value::Text(format!("variant-{index}-{locale}")),
                        Value::Text(message),
                    ],
                )
                .await
                .expect("insert variant");
        }
    }
    session
}

async fn seeded_untracked_session() -> SessionContext<Memory> {
    let storage = Memory::default();
    Engine::initialize(storage.clone())
        .await
        .expect("initialize fixture");
    let engine = Engine::new(storage).await.expect("open engine");
    let session = engine.open_session().await.expect("open session");
    for schema in schemas() {
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("register schema");
    }
    session
        .execute(
            "INSERT INTO bundle (id, declarations, lixcol_untracked) VALUES ('bundle-untracked', CAST('[]' AS JSONB), true)",
            &[],
        )
        .await
        .expect("insert untracked bundle");
    session
        .execute(
            r#"INSERT INTO message (id, "bundle_id", locale, selectors, lixcol_untracked) VALUES ('message-untracked', 'bundle-untracked', 'en', CAST('[]' AS JSONB), true)"#,
            &[],
        )
        .await
        .expect("insert untracked message");
    session
        .execute(
            r#"INSERT INTO variant (id, "message_id", matches, pattern, lixcol_untracked) VALUES ('variant-untracked', 'message-untracked', CAST('[]' AS JSONB), CAST('[]' AS JSONB), true)"#,
            &[],
        )
        .await
        .expect("insert untracked variant");
    session
}

fn schemas() -> [serde_json::Value; 3] {
    [
        serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "bundle",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "declarations", "type": "jsonb", "nullable": false, "default_value": [] },
            ],
            "primary_key": ["id"],
        }),
        serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "message",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "bundle_id", "type": "text", "nullable": false },
                { "name": "locale", "type": "text", "nullable": false },
                { "name": "selectors", "type": "jsonb", "nullable": false, "default_value": [] },
            ],
            "primary_key": ["id"],
            "foreign_keys": [{
                "columns": ["bundle_id"],
                "references": { "schema_key": "bundle", "columns": ["id"] }
            }],
        }),
        serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "variant",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "message_id", "type": "text", "nullable": false },
                { "name": "matches", "type": "jsonb", "nullable": false, "default_value": [] },
                { "name": "pattern", "type": "jsonb", "nullable": false, "default_value": [] },
            ],
            "primary_key": ["id"],
            "foreign_keys": [{
                "columns": ["message_id"],
                "references": { "schema_key": "message", "columns": ["id"] }
            }],
        }),
    ]
}
