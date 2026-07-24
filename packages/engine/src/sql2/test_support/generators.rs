//! Deterministic sql2 statement generators for differential tests.

#[cfg(test)]
use std::borrow::Cow;

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DifferentialExpectation {
    /// Semantic regression coverage. The candidate runs in normal auto mode,
    /// so this proves sql2 behavior but does not prove fast execution.
    SemanticParityMayFallback,
    /// Physical fast-path coverage. The candidate must produce a fast write
    /// plan, and the test fails if optimization declines the statement.
    FastRequiredParity,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DifferentialSqlCase {
    pub(crate) seed: Cow<'static, str>,
    pub(crate) setup_sql: &'static [&'static str],
    pub(crate) transaction_setup_sql: &'static [&'static str],
    pub(crate) sql: Cow<'static, str>,
    pub(crate) params: &'static [DifferentialParam],
    pub(crate) probes: &'static [DifferentialProbe],
    pub(crate) expectation: DifferentialExpectation,
    pub(crate) expected_execution: ExpectedExecution,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExpectedExecution {
    Ok,
    Err { code: &'static str },
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DifferentialParam {
    Json(&'static str),
    Text(&'static str),
    Blob(&'static [u8]),
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DifferentialProbe {
    RegisteredSchemaActive,
    RegisteredSchemaByBranch { branch_ids: &'static [&'static str] },
    LixFileActive { paths: &'static [&'static str] },
}

#[cfg(test)]
const EMPTY_PARAMS: &[DifferentialParam] = &[];

#[cfg(test)]
pub(crate) const ACTIVE_BRANCH_PROBE_ID: &str = "__active_branch__";

#[cfg(test)]
const REGISTERED_SCHEMA_PROBE: &[DifferentialProbe] = &[
    DifferentialProbe::RegisteredSchemaActive,
    DifferentialProbe::RegisteredSchemaByBranch {
        branch_ids: &[ACTIVE_BRANCH_PROBE_ID, "global", "branch-a", "branch-b"],
    },
];

#[cfg(test)]
const FILE_AND_REGISTERED_SCHEMA_PROBES: &[DifferentialProbe] = &[
    DifferentialProbe::LixFileActive {
        paths: &["/diff/insert.md", "/diff/existing.md", "/diff/tx.md"],
    },
    DifferentialProbe::RegisteredSchemaActive,
];

#[cfg(test)]
const PARAM_METADATA_JSON: &[DifferentialParam] =
    &[DifferentialParam::Json("{\"seen\":\"param\"}")];

#[cfg(test)]
const PARAM_FILE_PATH_AND_DATA: &[DifferentialParam] = &[
    DifferentialParam::Text("/diff/param.md"),
    DifferentialParam::Blob(b"param data"),
];

#[cfg(test)]
const PARAM_MULTI_FILE_PATH_AND_DATA: &[DifferentialParam] = &[
    DifferentialParam::Text("/diff/param-a.md"),
    DifferentialParam::Blob(b"param a"),
    DifferentialParam::Text("/diff/param-b.md"),
    DifferentialParam::Blob(b"param b"),
];

#[cfg(test)]
const SETUP_SEED_LIX_FILE_ROW: &[&str] = &[
    "INSERT INTO lix_file (id, path, data) VALUES ('diff-existing-file', '/diff/existing.md', X'6f6c64')",
];

#[cfg(test)]
const TX_SETUP_STAGED_LIX_FILE_ROW: &[&str] =
    &["INSERT INTO lix_file (id, path, data) VALUES ('diff-tx-file', '/diff/tx.md', X'62617365')"];

#[cfg(test)]
const SETUP_SEED_UNTRACKED_LIX_FILE_ROW: &[&str] = &[
    "INSERT INTO lix_file (id, path, data, lixcol_untracked) VALUES ('diff-untracked-file', '/diff/untracked.md', X'6f6c64', true)",
];

#[cfg(test)]
const LIX_FILE_PROBE: &[DifferentialProbe] = &[DifferentialProbe::LixFileActive {
    paths: &[
        "/diff/insert.md",
        "/diff/param.md",
        "/diff/param-a.md",
        "/diff/param-b.md",
        "/diff/upsert-new.md",
        "/diff/existing.md",
        "/diff/tx.md",
        "/diff/untracked.md",
        "/diff/multi-a.md",
        "/diff/multi-b.md",
    ],
}];

#[cfg(test)]
pub(crate) fn deterministic_repro_cases() -> Vec<DifferentialSqlCase> {
    vec![
        DifferentialSqlCase {
            seed: "known/unresolvable-assignment-target".into(),
            setup_sql: &[],
            transaction_setup_sql: &[],
            sql: "UPDATE lix_file SET no_such_column = 'x' WHERE false".into(),
            params: EMPTY_PARAMS,
            probes: FILE_AND_REGISTERED_SCHEMA_PROBES,
            expectation: DifferentialExpectation::SemanticParityMayFallback,
            expected_execution: ExpectedExecution::Err {
                code: "LIX_COLUMN_NOT_FOUND",
            },
        },
        DifferentialSqlCase {
            seed: "known/base-entity-branch-override".into(),
            setup_sql: &[],
            transaction_setup_sql: &[],
            sql: "UPDATE lix_registered_schema SET value = lix_json('{\"x-lix-key\":\"x\",\"type\":\"object\"}') WHERE lixcol_branch_id = 'branch-b'".into(),
            params: EMPTY_PARAMS,
            probes: REGISTERED_SCHEMA_PROBE,
            expectation: DifferentialExpectation::SemanticParityMayFallback,
            expected_execution: ExpectedExecution::Err {
                code: "LIX_COLUMN_NOT_FOUND",
            },
        },
        DifferentialSqlCase {
            seed: "known/base-entity-insert-hidden-branch-column".into(),
            setup_sql: &[],
            transaction_setup_sql: &[],
            sql: "INSERT INTO lix_registered_schema (value, lixcol_branch_id) VALUES (lix_json('{\"x-lix-key\":\"x\",\"type\":\"object\"}'), 'branch-b')".into(),
            params: EMPTY_PARAMS,
            probes: REGISTERED_SCHEMA_PROBE,
            expectation: DifferentialExpectation::SemanticParityMayFallback,
            expected_execution: ExpectedExecution::Err {
                code: "LIX_COLUMN_NOT_FOUND",
            },
        },
        DifferentialSqlCase {
            seed: "known/unknown-typed-entity-insert-column".into(),
            setup_sql: &[],
            transaction_setup_sql: &[],
            sql: "INSERT INTO lix_registered_schema (value, unknown_column) VALUES (lix_json('{\"x-lix-key\":\"x\",\"type\":\"object\"}'), 'x')".into(),
            params: EMPTY_PARAMS,
            probes: REGISTERED_SCHEMA_PROBE,
            expectation: DifferentialExpectation::SemanticParityMayFallback,
            expected_execution: ExpectedExecution::Err {
                code: "LIX_COLUMN_NOT_FOUND",
            },
        },
        DifferentialSqlCase {
            seed: "known/by-branch-update-without-branch-predicate".into(),
            setup_sql: &[],
            transaction_setup_sql: &[],
            sql: "UPDATE lix_registered_schema_by_branch SET value = lix_json('{\"x-lix-key\":\"x\",\"type\":\"object\"}')".into(),
            params: EMPTY_PARAMS,
            probes: REGISTERED_SCHEMA_PROBE,
            expectation: DifferentialExpectation::SemanticParityMayFallback,
            expected_execution: ExpectedExecution::Err {
                code: "LIX_UNSUPPORTED_SQL",
            },
        },
        DifferentialSqlCase {
            seed: "known/by-branch-delete-without-branch-predicate".into(),
            setup_sql: &[],
            transaction_setup_sql: &[],
            sql: "DELETE FROM lix_registered_schema_by_branch".into(),
            params: EMPTY_PARAMS,
            probes: REGISTERED_SCHEMA_PROBE,
            expectation: DifferentialExpectation::SemanticParityMayFallback,
            expected_execution: ExpectedExecution::Err {
                code: "LIX_UNSUPPORTED_SQL",
            },
        },
        DifferentialSqlCase {
            seed: "known/repeated-contradictory-predicates".into(),
            setup_sql: SETUP_SEED_LIX_FILE_ROW,
            transaction_setup_sql: &[],
            sql: "UPDATE lix_file SET data = X'6e6577' WHERE path = '/diff/existing.md' AND path = '/diff/other.md'".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::SemanticParityMayFallback,
            expected_execution: ExpectedExecution::Ok,
        },
        DifferentialSqlCase {
            seed: "known/duplicate-insert-target-columns".into(),
            setup_sql: &[],
            transaction_setup_sql: &[],
            sql: "INSERT INTO lix_file (path, path, data) VALUES ('/diff/dup.md', '/diff/dup.md', X'647570')".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::SemanticParityMayFallback,
            expected_execution: ExpectedExecution::Err {
                code: "LIX_INVALID_PARAM",
            },
        },
        DifferentialSqlCase {
            seed: "known/duplicate-update-assignments".into(),
            setup_sql: &[],
            transaction_setup_sql: &[],
            sql: "UPDATE lix_file SET data = NULL, data = NULL WHERE false".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::SemanticParityMayFallback,
            expected_execution: ExpectedExecution::Err {
                code: "LIX_INVALID_PARAM",
            },
        },
        DifferentialSqlCase {
            seed: "known/qualified-target-table-name".into(),
            setup_sql: &[],
            transaction_setup_sql: &[],
            sql: "UPDATE public.lix_file SET data = NULL WHERE false".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::SemanticParityMayFallback,
            expected_execution: ExpectedExecution::Err {
                code: "LIX_UNSUPPORTED_SQL",
            },
        },
        DifferentialSqlCase {
            seed: "known/empty-branch-filter-base-staged-dedupe".into(),
            setup_sql: SETUP_SEED_LIX_FILE_ROW,
            transaction_setup_sql: &[],
            sql: "UPDATE lix_file SET data = X'737461676564' WHERE path IN ('/diff/existing.md') AND path = '/diff/existing.md'".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::SemanticParityMayFallback,
            expected_execution: ExpectedExecution::Ok,
        },
        DifferentialSqlCase {
            seed: "known/parameter-binding-after-contradiction".into(),
            setup_sql: SETUP_SEED_LIX_FILE_ROW,
            transaction_setup_sql: &[],
            sql: "UPDATE lix_file SET data = $2 WHERE path = $1 AND path = '/diff/other.md'".into(),
            params: PARAM_FILE_PATH_AND_DATA,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::SemanticParityMayFallback,
            expected_execution: ExpectedExecution::Ok,
        },
        DifferentialSqlCase {
            seed: "known/staged-overlay-update-sees-prior-staged-row".into(),
            setup_sql: &[],
            transaction_setup_sql: TX_SETUP_STAGED_LIX_FILE_ROW,
            sql: "UPDATE lix_file SET data = X'75706461746564' WHERE path = '/diff/tx.md'".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::SemanticParityMayFallback,
            expected_execution: ExpectedExecution::Ok,
        },
    ]
}

#[cfg(test)]
pub(crate) fn generated_dml_cases() -> Vec<DifferentialSqlCase> {
    let mut cases = Vec::new();

    let target = "lix_file";
    cases.push(DifferentialSqlCase {
        seed: format!("generated/{target}/delete-false").into(),
        setup_sql: SETUP_SEED_LIX_FILE_ROW,
        transaction_setup_sql: &[],
        sql: format!("DELETE FROM {target} WHERE false").into(),
        params: EMPTY_PARAMS,
        probes: LIX_FILE_PROBE,
        expectation: DifferentialExpectation::SemanticParityMayFallback,
        expected_execution: ExpectedExecution::Ok,
    });
    cases.push(DifferentialSqlCase {
        seed: format!("generated/{target}/update-false").into(),
        setup_sql: SETUP_SEED_LIX_FILE_ROW,
        transaction_setup_sql: &[],
        sql: format!("UPDATE {target} SET lixcol_metadata = NULL WHERE false").into(),
        params: EMPTY_PARAMS,
        probes: LIX_FILE_PROBE,
        expectation: DifferentialExpectation::SemanticParityMayFallback,
        expected_execution: ExpectedExecution::Ok,
    });

    cases.extend([
        DifferentialSqlCase {
            seed: "generated/lix-file/insert-path-data-literal".into(),
            setup_sql: &[],
            transaction_setup_sql: &[],
            sql: "INSERT INTO lix_file (path, data) VALUES ('/diff/insert.md', X'696e73657274')".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::FastRequiredParity,
            expected_execution: ExpectedExecution::Ok,
        },
        DifferentialSqlCase {
            seed: "generated/lix-file/insert-path-data-params".into(),
            setup_sql: &[],
            transaction_setup_sql: &[],
            sql: "INSERT INTO lix_file (path, data) VALUES ($1, $2)".into(),
            params: PARAM_FILE_PATH_AND_DATA,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::FastRequiredParity,
            expected_execution: ExpectedExecution::Ok,
        },
        DifferentialSqlCase {
            seed: "generated/lix-file/upsert-path-data-insert".into(),
            setup_sql: &[],
            transaction_setup_sql: &[],
            sql: "INSERT INTO lix_file (path, data) VALUES ('/diff/upsert-new.md', X'6e6577') ON CONFLICT (path) DO UPDATE SET data = excluded.data".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::FastRequiredParity,
            expected_execution: ExpectedExecution::Ok,
        },
        DifferentialSqlCase {
            seed: "generated/lix-file/upsert-path-data-update".into(),
            setup_sql: SETUP_SEED_LIX_FILE_ROW,
            transaction_setup_sql: &[],
            sql: "INSERT INTO lix_file (path, data) VALUES ('/diff/existing.md', X'6e6577') ON CONFLICT (path) DO UPDATE SET data = excluded.data".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::FastRequiredParity,
            expected_execution: ExpectedExecution::Ok,
        },
        DifferentialSqlCase {
            seed: "generated/lix-file/upsert-path-data-do-nothing".into(),
            setup_sql: SETUP_SEED_LIX_FILE_ROW,
            transaction_setup_sql: &[],
            sql: "INSERT INTO lix_file (path, data) VALUES ('/diff/existing.md', X'736b6970') ON CONFLICT (path) DO NOTHING".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::FastRequiredParity,
            expected_execution: ExpectedExecution::Ok,
        },
        DifferentialSqlCase {
            seed: "generated/lix-file/upsert-path-data-do-nothing-duplicate-existing".into(),
            setup_sql: SETUP_SEED_LIX_FILE_ROW,
            transaction_setup_sql: &[],
            sql: "INSERT INTO lix_file (path, data) VALUES ('/diff/existing.md', X'736b6970'), ('/diff/existing.md', X'736b6970') ON CONFLICT (path) DO NOTHING".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::FastRequiredParity,
            expected_execution: ExpectedExecution::Ok,
        },
        DifferentialSqlCase {
            seed: "generated/lix-file/upsert-path-data-rejects-untracked-update".into(),
            setup_sql: SETUP_SEED_UNTRACKED_LIX_FILE_ROW,
            transaction_setup_sql: &[],
            sql: "INSERT INTO lix_file (path, data) VALUES ('/diff/untracked.md', X'6e6577') ON CONFLICT (path) DO UPDATE SET data = excluded.data".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::FastRequiredParity,
            expected_execution: ExpectedExecution::Err {
                code: "LIX_CONSTRAINT_VIOLATION",
            },
        },
        DifferentialSqlCase {
            seed: "generated/lix-file/upsert-path-data-rejects-untracked-do-nothing".into(),
            setup_sql: SETUP_SEED_UNTRACKED_LIX_FILE_ROW,
            transaction_setup_sql: &[],
            sql: "INSERT INTO lix_file (path, data) VALUES ('/diff/untracked.md', X'736b6970') ON CONFLICT (path) DO NOTHING".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::FastRequiredParity,
            expected_execution: ExpectedExecution::Err {
                code: "LIX_CONSTRAINT_VIOLATION",
            },
        },
        DifferentialSqlCase {
            seed: "generated/lix-file/multi-row-path-data-fast".into(),
            setup_sql: &[],
            transaction_setup_sql: &[],
            sql: "INSERT INTO lix_file (path, data) VALUES ('/diff/multi-a.md', X'61'), ('/diff/multi-b.md', X'62')".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::FastRequiredParity,
            expected_execution: ExpectedExecution::Ok,
        },
        DifferentialSqlCase {
            seed: "generated/lix-file/multi-row-path-data-params-fast".into(),
            setup_sql: &[],
            transaction_setup_sql: &[],
            sql: "INSERT INTO lix_file (path, data) VALUES ($1, $2), ($3, $4)".into(),
            params: PARAM_MULTI_FILE_PATH_AND_DATA,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::FastRequiredParity,
            expected_execution: ExpectedExecution::Ok,
        },
        DifferentialSqlCase {
            seed: "generated/lix-file/multi-row-upsert-path-data-update".into(),
            setup_sql: SETUP_SEED_LIX_FILE_ROW,
            transaction_setup_sql: &[],
            sql: "INSERT INTO lix_file (path, data) VALUES ('/diff/existing.md', X'6e6577'), ('/diff/multi-a.md', X'61') ON CONFLICT (path) DO UPDATE SET data = excluded.data".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::FastRequiredParity,
            expected_execution: ExpectedExecution::Ok,
        },
        DifferentialSqlCase {
            seed: "generated/lix-file/multi-row-upsert-path-data-do-nothing".into(),
            setup_sql: SETUP_SEED_LIX_FILE_ROW,
            transaction_setup_sql: &[],
            sql: "INSERT INTO lix_file (path, data) VALUES ('/diff/existing.md', X'736b6970'), ('/diff/multi-b.md', X'62') ON CONFLICT (path) DO NOTHING".into(),
            params: EMPTY_PARAMS,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::FastRequiredParity,
            expected_execution: ExpectedExecution::Ok,
        },
        DifferentialSqlCase {
            seed: "generated/entity-base/reject-hidden-branch".into(),
            setup_sql: &[],
            transaction_setup_sql: &[],
            sql: "DELETE FROM lix_registered_schema WHERE lixcol_branch_id = 'branch-a'".into(),
            params: EMPTY_PARAMS,
            probes: REGISTERED_SCHEMA_PROBE,
            expectation: DifferentialExpectation::SemanticParityMayFallback,
            expected_execution: ExpectedExecution::Err {
                code: "LIX_COLUMN_NOT_FOUND",
            },
        },
        DifferentialSqlCase {
            seed: "generated/lix-file/update-param-metadata".into(),
            setup_sql: SETUP_SEED_LIX_FILE_ROW,
            transaction_setup_sql: &[],
            sql: "UPDATE lix_file SET lixcol_metadata = $1 WHERE path = '/diff/existing.md'".into(),
            params: PARAM_METADATA_JSON,
            probes: LIX_FILE_PROBE,
            expectation: DifferentialExpectation::SemanticParityMayFallback,
            expected_execution: ExpectedExecution::Ok,
        },
    ]);

    cases
}
