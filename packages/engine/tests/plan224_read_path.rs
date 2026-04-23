use std::fs;
use std::path::PathBuf;
fn engine_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_engine_source(relative: &str) -> String {
    fs::read_to_string(engine_root().join("src").join(relative))
        .unwrap_or_else(|error| panic!("failed to read src/{relative}: {error}"))
}

fn assert_source_does_not_contain(relative: &str, source: &str, needle: &str) {
    assert!(
        !source.contains(needle),
        "src/{relative} should not contain `{needle}` under Plan 224",
    );
}

#[test]
fn plan224_public_read_path_deletes_lowered_sql_types() {
    let plan = read_engine_source("sql/physical_plan/plan.rs");
    let public_surface = read_engine_source("sql/prepare/public_surface/mod.rs");
    let prepared_read = read_engine_source("sql/prepare/prepared_read.rs");

    assert_source_does_not_contain("sql/physical_plan/plan.rs", &plan, "LoweredSql(");
    assert_source_does_not_contain("sql/physical_plan/plan.rs", &plan, "LoweredReadBatch");
    assert_source_does_not_contain(
        "sql/prepare/public_surface/mod.rs",
        &public_surface,
        "PublicReadPhysicalPlan::LoweredSql",
    );
    assert_source_does_not_contain(
        "sql/prepare/prepared_read.rs",
        &prepared_read,
        "PublicReadPhysicalPlan::LoweredSql",
    );
}

#[test]
fn plan224_public_read_path_has_no_sql1_or_backend_fallbacks() {
    let execution_read_public = read_engine_source("execution/read/public.rs");
    let prepared_read = read_engine_source("sql/prepare/prepared_read.rs");
    let public_surface_read = read_engine_source("sql/prepare/public_surface/read.rs");
    let public_surface_mod = read_engine_source("sql/prepare/public_surface/mod.rs");

    assert_source_does_not_contain(
        "execution/read/public.rs",
        &execution_read_public,
        "PreparedPublicReadPlanArtifact::PreparedBatch",
    );
    assert_source_does_not_contain(
        "execution/read/public.rs",
        &execution_read_public,
        "PreparedPublicReadPlanArtifact::HistoryRead",
    );
    assert_source_does_not_contain(
        "execution/read/public.rs",
        &execution_read_public,
        "execute_prepared_batch_with_backend",
    );
    assert_source_does_not_contain(
        "execution/read/public.rs",
        &execution_read_public,
        "execute_history_read_plan_with_backend",
    );
    assert_source_does_not_contain(
        "sql/prepare/prepared_read.rs",
        &prepared_read,
        "PreparedPublicReadPlanArtifact::PreparedBatch",
    );
    assert_source_does_not_contain(
        "sql/prepare/prepared_read.rs",
        &prepared_read,
        "PreparedPublicReadPlanArtifact::HistoryRead",
    );
    assert_source_does_not_contain(
        "sql/prepare/public_surface/read.rs",
        &public_surface_read,
        "ReadTimeProjectionPlan",
    );
    assert_source_does_not_contain(
        "sql/prepare/public_surface/read.rs",
        &public_surface_read,
        "HistoryReadPlan",
    );
    assert_source_does_not_contain(
        "sql/prepare/public_surface/read.rs",
        &public_surface_read,
        "prepare_public_read_via_surface_lowering",
    );
    assert_source_does_not_contain(
        "sql/prepare/public_surface/read.rs",
        &public_surface_read,
        "build_public_read_explain_artifacts",
    );
    assert_source_does_not_contain(
        "sql/prepare/public_surface/read.rs",
        &public_surface_read,
        "ExplainTimingCollector",
    );
    assert_source_does_not_contain(
        "sql/prepare/public_surface/mod.rs",
        &public_surface_mod,
        "ExplainPublicReadExecution",
    );
    assert_source_does_not_contain(
        "sql/prepare/public_surface/mod.rs",
        &public_surface_mod,
        "PublicReadPhysicalPlan::ReadTimeProjection",
    );
    assert_source_does_not_contain(
        "sql/prepare/public_surface/mod.rs",
        &public_surface_mod,
        "PublicReadPhysicalPlan::HistoryRead",
    );
    assert!(
        !engine_root().join("src/sql/physical_plan/lowerer.rs").exists(),
        "src/sql/physical_plan/lowerer.rs should be deleted under Plan 224",
    );
}
