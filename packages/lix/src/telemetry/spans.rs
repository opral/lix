//! Production span descriptors. Each stable name is declared once.

use super::{TelemetrySpanClass, TelemetrySpanDescriptor};
use opentelemetry::trace::SpanKind;

macro_rules! define_production_spans {
    ($(
        $(#[$meta:meta])*
        $ident:ident {
            name: $name:literal,
            class: $class:ident,
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
                        $($attr = tracing::field::Empty,)*
                        "error.type" = tracing::field::Empty,
                        "lix.operation.cancelled" = tracing::field::Empty,
                    )
                }

                $(#[$meta])*
                pub static $ident: TelemetrySpanDescriptor = TelemetrySpanDescriptor {
                    name: $name,
                    class: TelemetrySpanClass::$class,
                    kind: SpanKind::Internal,
                    allowed_attributes: &[
                        $($attr,)*
                        "error.type",
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
        target: "lix",
        attributes: []
    }
    SESSION_OPEN {
        name: "lix.session.open",
        class: Lifecycle,
        target: "lix",
        attributes: []
    }
    REPOSITORY_OPENED {
        name: "lix.repository.opened",
        class: Lifecycle,
        target: "lix",
        attributes: ["lix.id", "lix.branch_id", "lix.account_id"]
    }
    SQL_QUERY {
        name: "lix.sql.query",
        class: Sql,
        target: "lix_sql",
        attributes: [
            "db.system.name",
            "db.operation.name",
            "db.query.summary",
            "db.query.text",
            "lix.sql.fingerprint",
            "lix.execution.kind",
            "lix.batch.index",
            "db.response.returned_rows",
            "lix.rows_affected",
        ]
    }
    SQL_BATCH {
        name: "lix.sql.batch",
        class: Sql,
        target: "lix_sql",
        attributes: [
            "db.system.name",
            "db.operation.batch.size",
            "lix.execution.kind",
        ]
    }
    SQL_COHERENT_READ_BATCH {
        name: "lix.sql.coherent_read_batch",
        class: Sql,
        target: "lix_sql",
        attributes: [
            "db.system.name",
            "db.operation.batch.size",
            "lix.execution.kind",
        ]
    }
    CHECKPOINT_CREATE {
        name: "lix.checkpoint.create",
        class: Lifecycle,
        target: "lix",
        attributes: ["lix.commit_id", "lix.parent_commit_id"]
    }
    TRANSACTION_WAIT {
        name: "lix.transaction.wait",
        class: Performance,
        target: "lix_sql",
        attributes: ["lix.commit_cohort_id", "lix.wait.reason"]
    }
    TRANSACTION_MATERIALIZE {
        name: "lix.transaction.materialize",
        class: Performance,
        target: "lix_sql",
        attributes: ["lix.commit_cohort_id", "lix.transaction.count"]
    }
    TRANSACTION_STORAGE {
        name: "lix.transaction.storage",
        class: Performance,
        target: "lix_sql",
        attributes: ["lix.commit_cohort_id", "lix.transaction.count"]
    }
    TRANSACTION_NOTIFY {
        name: "lix.transaction.notify",
        class: Performance,
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
