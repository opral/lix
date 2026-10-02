//! Production span descriptors. Each stable name is declared once.

use super::{TelemetrySpanClass, TelemetrySpanDescriptor};
use opentelemetry::trace::SpanKind;

macro_rules! define_production_spans {
    ($(
        $(#[$meta:meta])*
        $ident:ident {
            name: $name:literal,
            class: $class:ident,
            kind: $kind:ident,
            otel_kind: $otel_kind:literal,
            target: $target:literal,
            attributes: [$($attr:literal),* $(,)?]
        }
    )*) => {
        paste::paste! {
            $(
                #[allow(non_snake_case)]
                fn [<create_ $ident>]() -> tracing::Span {
                    tracing::info_span!(
                        target: $target,
                        $name,
                        otel.kind = $otel_kind,
                        otel.name = tracing::field::Empty,
                        $($attr = tracing::field::Empty,)*
                        "error.type" = tracing::field::Empty,
                        "rust_origin.kind" = tracing::field::Empty,
                        "code.filepath" = tracing::field::Empty,
                        "code.lineno" = tracing::field::Empty,
                        "code.column" = tracing::field::Empty,
                        "rust_stacktrace_status" = tracing::field::Empty,
                        "lix.receipt.version" = tracing::field::Empty,
                        "lix.receipt.expected_version" = tracing::field::Empty,
                        "lix.migration.from_version" = tracing::field::Empty,
                        "lix.migration.to_version" = tracing::field::Empty,
                        "lix.migration.phase" = tracing::field::Empty,
                        "lix.migration.failure_reason" = tracing::field::Empty,
                        "lix.migration.failure_path" = tracing::field::Empty,
                        "lix.migration.json_line" = tracing::field::Empty,
                        "lix.migration.json_column" = tracing::field::Empty,
                        "lix.migration.missing_field" = tracing::field::Empty,
                        "lix.payload.standalone_status" = tracing::field::Empty,
                        "lix.payload.physical_status" = tracing::field::Empty,
                        "lix.payload.failure_reason" = tracing::field::Empty,
                        "lix.payload.schema_kind" = tracing::field::Empty,
                        "lix.payload.recipe_count" = tracing::field::Empty,
                        "lix.payload.recipe_mask" = tracing::field::Empty,
                        "lix.payload.required_input_count" = tracing::field::Empty,
                        "lix.payload.attempted" = tracing::field::Empty,
                        "lix.payload.budget" = tracing::field::Empty,
                        "lix.payload.change_id" = tracing::field::Empty,
                        "lix.read.recipe_count" = tracing::field::Empty,
                        "lix.read.recipe_mask" = tracing::field::Empty,
                        "lix.payload.source_deferred" = tracing::field::Empty,
                        "lix.payload.physical_conflict" = tracing::field::Empty,
                        "lix.operation.cancelled" = tracing::field::Empty,
                    )
                }

                $(#[$meta])*
                pub static $ident: TelemetrySpanDescriptor = TelemetrySpanDescriptor {
                    name: $name,
                    class: TelemetrySpanClass::$class,
                    kind: SpanKind::$kind,
                    allowed_attributes: &[
                        $($attr,)*
                        "otel.name",
                        "error.type",
                        "rust_origin.kind",
                        "code.filepath",
                        "code.lineno",
                        "code.column",
                        "rust_stacktrace_status",
                        "lix.receipt.version",
                        "lix.receipt.expected_version",
                        "lix.migration.from_version",
                        "lix.migration.to_version",
                        "lix.migration.phase",
                        "lix.migration.failure_reason",
                        "lix.migration.failure_path",
                        "lix.migration.json_line",
                        "lix.migration.json_column",
                        "lix.migration.missing_field",
                        "lix.payload.standalone_status",
                        "lix.payload.physical_status",
                        "lix.payload.failure_reason",
                        "lix.payload.schema_kind",
                        "lix.payload.recipe_count",
                        "lix.payload.recipe_mask",
                        "lix.payload.required_input_count",
                        "lix.payload.phase",
                        "lix.payload.operation",
                        "lix.payload.attempted",
                        "lix.payload.budget",
                        "lix.payload.change_id",
                        "lix.read.recipe_count",
                        "lix.read.recipe_mask",
                        "lix.payload.source_deferred",
                        "lix.payload.physical_conflict",
                        "lix.operation.cancelled",
                    ],
                    create_tracing_span: [<create_ $ident>],
                };
            )*

            pub const ALL: &[&TelemetrySpanDescriptor] = &[$(&$ident),*];
        }
    };
}

define_production_spans! {
    ENGINE_OPEN {
        name: "lix.engine.open",
        class: Lifecycle,
        kind: Internal,
        otel_kind: "internal",
        target: "lix",
        attributes: []
    }
    SESSION_OPEN {
        name: "lix.session.open",
        class: Lifecycle,
        kind: Internal,
        otel_kind: "internal",
        target: "lix",
        attributes: []
    }
    REPOSITORY_OPENED {
        name: "lix.repository.opened",
        class: Lifecycle,
        kind: Internal,
        otel_kind: "internal",
        target: "lix",
        attributes: ["lix.id", "lix.branch_id", "lix.account_id"]
    }
    SQL_QUERY {
        name: "lix.sql.query",
        class: Sql,
        kind: Client,
        otel_kind: "client",
        target: "lix_sql",
        attributes: [
            "db.system.name",
            "db.operation.name",
            "db.query.summary",
            "db.query.text",
            "lix.sql.fingerprint",
            "lix.sql.query_text_truncated",
            "otel.name",
            "lix.execution.kind",
            "lix.batch.index",
            "db.response.returned_rows",
            "lix.rows_affected",
        ]
    }
    SQL_BATCH {
        name: "lix.sql.batch",
        class: Sql,
        kind: Client,
        otel_kind: "client",
        target: "lix_sql",
        attributes: [
            "db.system.name",
            "db.operation.batch.size",
            "lix.execution.kind",
            "db.operation.name",
            "db.query.summary",
            "db.query.text",
            "lix.sql.fingerprint",
            "lix.sql.query_text_truncated",
            "otel.name",
        ]
    }
    SQL_COHERENT_READ_BATCH {
        name: "lix.sql.coherent_read_batch",
        class: Sql,
        kind: Client,
        otel_kind: "client",
        target: "lix_sql",
        attributes: [
            "db.system.name",
            "db.operation.batch.size",
            "lix.execution.kind",
            "db.operation.name",
            "db.query.summary",
            "db.query.text",
            "lix.sql.fingerprint",
            "lix.sql.query_text_truncated",
            "otel.name",
        ]
    }
    CHECKPOINT_CREATE {
        name: "lix.checkpoint.create",
        class: Lifecycle,
        kind: Internal,
        otel_kind: "internal",
        target: "lix",
        attributes: ["lix.commit_id", "lix.parent_commit_id"]
    }
    TRANSACTION_WAIT {
        name: "lix.transaction.wait",
        class: Performance,
        kind: Internal,
        otel_kind: "internal",
        target: "lix_sql",
        attributes: ["lix.commit_cohort_id", "lix.wait.reason"]
    }
    TRANSACTION_MATERIALIZE {
        name: "lix.transaction.materialize",
        class: Performance,
        kind: Internal,
        otel_kind: "internal",
        target: "lix_sql",
        attributes: ["lix.commit_cohort_id", "lix.transaction.count"]
    }
    TRANSACTION_STORAGE {
        name: "lix.transaction.storage",
        class: Performance,
        kind: Internal,
        otel_kind: "internal",
        target: "lix_sql",
        attributes: ["lix.commit_cohort_id", "lix.transaction.count"]
    }
    TRANSACTION_NOTIFY {
        name: "lix.transaction.notify",
        class: Performance,
        kind: Internal,
        otel_kind: "internal",
        target: "lix_sql",
        attributes: ["lix.commit_cohort_id", "lix.transaction.count"]
    }
}

/// Stable production names. Host HTTP envelopes are not Lix's.
pub const PRODUCTION_NAMES: &[&str] = &[
    ENGINE_OPEN.name,
    SESSION_OPEN.name,
    REPOSITORY_OPENED.name,
    SQL_QUERY.name,
    SQL_BATCH.name,
    SQL_COHERENT_READ_BATCH.name,
    CHECKPOINT_CREATE.name,
    TRANSACTION_WAIT.name,
    TRANSACTION_MATERIALIZE.name,
    TRANSACTION_STORAGE.name,
    TRANSACTION_NOTIFY.name,
];

/// Former INFO names. Must not appear on the production plane.
pub const FORBIDDEN_PRODUCTION_NAMES: &[&str] = &[
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "SQL batch",
    "lix.opened",
    "lix.runtime.open",
    "lix.storage.open",
    "lix.transaction.commit",
    "storage writer wait",
    "storage lowering",
    "transaction storage prepare",
];
