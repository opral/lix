use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum SourceScope {
    Production,
    TestOnly,
}

impl SourceScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::Production => "production",
            Self::TestOnly => "test_only",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum ReferenceCategory {
    RootApi,
    TemporaryLogicalContract,
    ForbiddenInternalModule,
    ForbiddenInternalLiveTableSql,
}

impl ReferenceCategory {
    fn as_str(self) -> &'static str {
        match self {
            Self::RootApi => "root_api",
            Self::TemporaryLogicalContract => "temporary_logical_contract",
            Self::ForbiddenInternalModule => "forbidden_internal_module",
            Self::ForbiddenInternalLiveTableSql => "forbidden_internal_live_table_sql",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum ReferenceKind {
    RootApi,
    Shared,
    Tracked,
    Untracked,
    Constraints,
    Effective,
    SchemaAccess,
    Projection,
    PendingReads,
    FilesystemQueries,
    FilesystemProjection,
    KeyValueQueries,
    InternalLiveTableSql,
}

impl ReferenceKind {
    fn category(self) -> ReferenceCategory {
        match self {
            Self::RootApi => ReferenceCategory::RootApi,
            Self::Shared
            | Self::Tracked
            | Self::Untracked
            | Self::Constraints
            | Self::Effective => ReferenceCategory::TemporaryLogicalContract,
            Self::SchemaAccess
            | Self::Projection
            | Self::PendingReads
            | Self::FilesystemQueries
            | Self::FilesystemProjection
            | Self::KeyValueQueries => ReferenceCategory::ForbiddenInternalModule,
            Self::InternalLiveTableSql => ReferenceCategory::ForbiddenInternalLiveTableSql,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::RootApi => "root_api",
            Self::Shared => "shared",
            Self::Tracked => "tracked",
            Self::Untracked => "untracked",
            Self::Constraints => "constraints",
            Self::Effective => "effective",
            Self::SchemaAccess => "schema_access",
            Self::Projection => "projection",
            Self::PendingReads => "pending_reads",
            Self::FilesystemQueries => "filesystem_queries",
            Self::FilesystemProjection => "filesystem_projection",
            Self::KeyValueQueries => "key_value_queries",
            Self::InternalLiveTableSql => "internal_live_table_sql",
        }
    }
}

#[derive(Debug, Default)]
struct Audit {
    counts: BTreeMap<(SourceScope, ReferenceCategory, String), usize>,
    examples: BTreeMap<(SourceScope, ReferenceCategory, String), Vec<String>>,
}

impl Audit {
    fn record(
        &mut self,
        scope: SourceScope,
        kind: ReferenceKind,
        path: &str,
        line_no: usize,
        line: &str,
    ) {
        let category = kind.category();
        let key = (scope, category, format!("{path}::{}", kind.as_str()));
        *self.counts.entry(key.clone()).or_default() += 1;
        self.examples
            .entry(key)
            .or_default()
            .push(format!("{path}:{line_no}: {}", line.trim()));
    }

    fn totals(&self) -> BTreeMap<(SourceScope, ReferenceCategory), usize> {
        let mut totals = BTreeMap::new();
        for ((scope, category, _), count) in &self.counts {
            *totals.entry((*scope, *category)).or_default() += *count;
        }
        totals
    }

    fn assert_within_baseline(
        &self,
        baseline_keys: &BTreeSet<(SourceScope, ReferenceCategory, String)>,
        baseline_totals: &BTreeMap<(SourceScope, ReferenceCategory), usize>,
    ) {
        let mut unexpected = Vec::new();
        let mut regressions = Vec::new();

        for key in self.counts.keys() {
            if !baseline_keys.contains(key) {
                unexpected.push(format!(
                    "{} / {} / {}",
                    key.0.as_str(),
                    key.1.as_str(),
                    key.2
                ));
            }
        }

        for ((scope, category), actual_count) in self.totals() {
            let expected_count = baseline_totals
                .get(&(scope, category))
                .copied()
                .unwrap_or_default();
            if actual_count > expected_count {
                regressions.push(format!(
                    "{} / {} grew from {} to {}",
                    scope.as_str(),
                    category.as_str(),
                    expected_count,
                    actual_count
                ));
            }
        }

        if unexpected.is_empty() && regressions.is_empty() {
            return;
        }

        let mut message = String::from("live_state boundary audit regressed\n");
        if !unexpected.is_empty() {
            message.push_str("\nUnexpected entries:\n");
            for item in unexpected {
                message.push_str("  - ");
                message.push_str(&item);
                message.push('\n');
            }
        }
        if !regressions.is_empty() {
            message.push_str("\nCount regressions:\n");
            for item in regressions {
                message.push_str("  - ");
                message.push_str(&item);
                message.push('\n');
            }
        }

        message.push_str("\nCurrent totals:\n");
        for ((scope, category), count) in self.totals() {
            message.push_str(&format!(
                "  - {} / {} = {}\n",
                scope.as_str(),
                category.as_str(),
                count
            ));
        }

        message.push_str("\nExamples:\n");
        for (key, lines) in &self.examples {
            if !self.counts.contains_key(key) {
                continue;
            }
            message.push_str(&format!(
                "  - {} / {} / {}:\n",
                key.0.as_str(),
                key.1.as_str(),
                key.2
            ));
            for line in lines.iter().take(4) {
                message.push_str("      ");
                message.push_str(line);
                message.push('\n');
            }
        }

        panic!("{message}");
    }
}

fn manifest_dir() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn engine_root(relative: &str) -> PathBuf {
    manifest_dir().join(relative)
}

fn collect_rs_files(root: &Path, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(root)
        .unwrap_or_else(|error| panic!("failed to read dir '{}': {error}", root.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|error| {
            panic!(
                "failed to read dir entry under '{}': {error}",
                root.display()
            )
        });
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

fn tracked_source_files() -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rs_files(&engine_root("src"), &mut files);
    collect_rs_files(&engine_root("tests"), &mut files);
    files.sort();
    files
}

fn relative_to_manifest(path: &Path) -> String {
    path.strip_prefix(manifest_dir())
        .unwrap_or(path)
        .display()
        .to_string()
}

fn is_live_state_path(relative: &str) -> bool {
    relative.starts_with("src/live_state/")
}

fn is_audit_support_path(relative: &str) -> bool {
    matches!(
        relative,
        "tests/live_state_boundary_audit.rs" | "tests/cutover_audit.rs"
    )
}

fn is_test_only_path(relative: &str) -> bool {
    relative.starts_with("tests/")
        || relative.contains("/tests/")
        || relative == "src/test_support.rs"
}

fn split_source_regions<'a>(relative: &str, source: &'a str) -> Vec<(SourceScope, &'a str)> {
    if is_test_only_path(relative) {
        return vec![(SourceScope::TestOnly, source)];
    }
    if let Some(index) = source.find("#[cfg(test)]") {
        let (production, tests) = source.split_at(index);
        let mut out = vec![(SourceScope::Production, production)];
        if !tests.trim().is_empty() {
            out.push((SourceScope::TestOnly, tests));
        }
        out
    } else {
        vec![(SourceScope::Production, source)]
    }
}

fn classify_live_state_occurrence(text: &str, start: usize) -> ReferenceKind {
    let tail = &text[start..];
    for (prefix, kind) in [
        (
            "crate::live_state::schema_access",
            ReferenceKind::SchemaAccess,
        ),
        ("crate::live_state::projection", ReferenceKind::Projection),
        ("crate::live_state::shared", ReferenceKind::Shared),
        ("crate::live_state::tracked", ReferenceKind::Tracked),
        ("crate::live_state::untracked", ReferenceKind::Untracked),
        ("crate::live_state::constraints", ReferenceKind::Constraints),
        ("crate::live_state::effective", ReferenceKind::Effective),
        (
            "crate::live_state::pending_reads",
            ReferenceKind::PendingReads,
        ),
        (
            "crate::live_state::filesystem_queries",
            ReferenceKind::FilesystemQueries,
        ),
        (
            "crate::live_state::filesystem_projection",
            ReferenceKind::FilesystemProjection,
        ),
        (
            "crate::live_state::key_value_queries",
            ReferenceKind::KeyValueQueries,
        ),
    ] {
        if tail.starts_with(prefix) {
            let boundary = tail[prefix.len()..].chars().next();
            if boundary.is_none()
                || boundary.is_some_and(|ch| matches!(ch, ':' | '{' | '}' | ',' | ';' | ' ' | '('))
            {
                return kind;
            }
        }
    }
    ReferenceKind::RootApi
}

fn scan_live_state_references(audit: &mut Audit, path: &str, scope: SourceScope, source: &str) {
    for (index, line) in source.lines().enumerate() {
        let mut search_start = 0;
        while let Some(found) = line[search_start..].find("crate::live_state") {
            let absolute = search_start + found;
            let kind = classify_live_state_occurrence(line, absolute);
            audit.record(scope, kind, path, index + 1, line);
            search_start = absolute + "crate::live_state".len();
        }
    }
}

fn scan_internal_live_table_sql(audit: &mut Audit, path: &str, scope: SourceScope, source: &str) {
    for (index, line) in source.lines().enumerate() {
        let mut search_start = 0;
        while let Some(found) = line[search_start..].find("lix_internal_live_v1_") {
            let absolute = search_start + found;
            let before = &line[..absolute];
            if before.contains("//") {
                break;
            }
            audit.record(
                scope,
                ReferenceKind::InternalLiveTableSql,
                path,
                index + 1,
                line,
            );
            search_start = absolute + "lix_internal_live_v1_".len();
        }
    }
}

fn current_boundary_audit() -> Audit {
    let mut audit = Audit::default();
    for path in tracked_source_files() {
        let relative = relative_to_manifest(&path);
        if is_live_state_path(&relative) || is_audit_support_path(&relative) {
            continue;
        }
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("failed to read '{}': {error}", path.display()));
        for (scope, region) in split_source_regions(&relative, &source) {
            scan_live_state_references(&mut audit, &relative, scope, region);
            scan_internal_live_table_sql(&mut audit, &relative, scope, region);
        }
    }
    audit
}

fn insert_baseline_keys(
    out: &mut BTreeSet<(SourceScope, ReferenceCategory, String)>,
    scope: SourceScope,
    kind: ReferenceKind,
    paths: &[&str],
) {
    for path in paths {
        out.insert((scope, kind.category(), format!("{path}::{}", kind.as_str())));
    }
}

fn baseline_boundary_keys() -> BTreeSet<(SourceScope, ReferenceCategory, String)> {
    let mut keys = BTreeSet::new();

    insert_baseline_keys(
        &mut keys,
        SourceScope::Production,
        ReferenceKind::RootApi,
        &[
            "src/api.rs",
            "src/canonical/append.rs",
            "src/canonical/create_commit_preflight.rs",
            "src/canonical/pending_session.rs",
            "src/canonical/receipt.rs",
            "src/canonical/refs.rs",
            "src/canonical/version_state.rs",
            "src/checkpoint/create_checkpoint.rs",
            "src/filesystem/live_projection.rs",
            "src/filesystem/queries.rs",
            "src/init/run.rs",
            "src/init/seed.rs",
            "src/key_value/queries.rs",
            "src/plugin/runtime.rs",
            "src/schema/init.rs",
            "src/sql/executor/compiled.rs",
            "src/sql/executor/dependency_spec.rs",
            "src/sql/executor/public_runtime/read.rs",
            "src/sql/logical_plan/public_ir/mod.rs",
            "src/sql/physical_plan/lowerer.rs",
            "src/sql/physical_plan/lowerer/broad.rs",
            "src/sql/semantic_ir/semantics/effective_state_resolver.rs",
            "src/sql/semantic_ir/validation.rs",
            "src/sql/services/public_reads.rs",
            "src/sql/services/state_reader.rs",
            "src/transaction/contracts.rs",
            "src/transaction/coordinator.rs",
            "src/transaction/execution.rs",
            "src/transaction/live_state_write_state.rs",
            "src/transaction/overlay.rs",
            "src/transaction/read_context.rs",
            "src/transaction/sql_adapter/effects.rs",
            "src/transaction/sql_adapter/planned_write.rs",
            "src/transaction/sql_adapter/untracked_apply.rs",
            "src/transaction/sql_adapter/runtime.rs",
            "src/transaction/write_plan.rs",
            "src/transaction/write_runner.rs",
            "src/version/init.rs",
            "src/version/merge_version.rs",
            "src/workspace/writer_key.rs",
        ],
    );

    insert_baseline_keys(
        &mut keys,
        SourceScope::Production,
        ReferenceKind::Shared,
        &[],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::Production,
        ReferenceKind::Tracked,
        &[],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::Production,
        ReferenceKind::Untracked,
        &[],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::Production,
        ReferenceKind::Constraints,
        &[],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::Production,
        ReferenceKind::Effective,
        &[],
    );

    insert_baseline_keys(
        &mut keys,
        SourceScope::Production,
        ReferenceKind::SchemaAccess,
        &[],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::Production,
        ReferenceKind::Projection,
        &[],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::Production,
        ReferenceKind::PendingReads,
        &[],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::Production,
        ReferenceKind::FilesystemQueries,
        &[],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::Production,
        ReferenceKind::FilesystemProjection,
        &[],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::Production,
        ReferenceKind::KeyValueQueries,
        &[],
    );

    insert_baseline_keys(
        &mut keys,
        SourceScope::TestOnly,
        ReferenceKind::RootApi,
        &[
            "src/api.rs",
            "src/canonical/append.rs",
            "src/canonical/refs.rs",
            "src/session/mod.rs",
            "src/sql/executor/public_runtime/mod.rs",
            "src/sql/semantic_ir/semantics/write_resolver.rs",
            "src/test_support.rs",
            "src/transaction/tests/module.rs",
            "src/workspace/writer_key.rs",
        ],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::TestOnly,
        ReferenceKind::Shared,
        &["src/transaction/execution.rs"],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::TestOnly,
        ReferenceKind::Tracked,
        &[
            "src/transaction/execution.rs",
            "src/transaction/tests/module.rs",
        ],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::TestOnly,
        ReferenceKind::Untracked,
        &[
            "src/test_support.rs",
            "src/transaction/execution.rs",
            "src/transaction/tests/module.rs",
        ],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::TestOnly,
        ReferenceKind::Constraints,
        &["src/canonical/refs.rs"],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::TestOnly,
        ReferenceKind::SchemaAccess,
        &[],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::TestOnly,
        ReferenceKind::Projection,
        &[],
    );
    insert_baseline_keys(
        &mut keys,
        SourceScope::TestOnly,
        ReferenceKind::InternalLiveTableSql,
        &[
            "src/canonical/create_commit.rs",
            "src/canonical/graph_sql.rs",
            "src/canonical/history.rs",
            "src/canonical/state_source.rs",
            "src/errors/classification.rs",
            "src/sql/executor/public_runtime/mod.rs",
            "src/sql/physical_plan/lowerer.rs",
            "tests/cutover_reopen.rs",
            "tests/filesystem/file_materialization.rs",
            "tests/filesystem/materialization.rs",
            "tests/filesystem/writer_key.rs",
            "tests/init.rs",
            "tests/runtime/checkpoint.rs",
            "tests/runtime/deterministic_mode.rs",
            "tests/runtime/execute.rs",
            "tests/schema/registered_schema.rs",
            "tests/sql_surfaces/explain.rs",
            "tests/sql_surfaces/state.rs",
            "tests/sql_surfaces/working_changes.rs",
        ],
    );

    keys
}

fn baseline_boundary_totals() -> BTreeMap<(SourceScope, ReferenceCategory), usize> {
    BTreeMap::from([
        ((SourceScope::Production, ReferenceCategory::RootApi), 61),
        (
            (
                SourceScope::Production,
                ReferenceCategory::TemporaryLogicalContract,
            ),
            0,
        ),
        (
            (
                SourceScope::Production,
                ReferenceCategory::ForbiddenInternalModule,
            ),
            0,
        ),
        ((SourceScope::TestOnly, ReferenceCategory::RootApi), 24),
        (
            (
                SourceScope::TestOnly,
                ReferenceCategory::TemporaryLogicalContract,
            ),
            7,
        ),
        (
            (
                SourceScope::TestOnly,
                ReferenceCategory::ForbiddenInternalModule,
            ),
            0,
        ),
        (
            (
                SourceScope::TestOnly,
                ReferenceCategory::ForbiddenInternalLiveTableSql,
            ),
            84,
        ),
    ])
}

#[test]
fn live_state_boundary_imports_and_internal_live_table_sql_do_not_grow() {
    let audit = current_boundary_audit();
    audit.assert_within_baseline(&baseline_boundary_keys(), &baseline_boundary_totals());
}

fn is_visible_declaration_start(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("pub ") || trimmed.starts_with("pub(crate)")
}

fn declaration_is_complete(declaration: &str) -> bool {
    let trimmed = declaration.trim_end();
    if trimmed.contains('{') || trimmed.contains(';') {
        return true;
    }
    if !trimmed.contains("fn ") && trimmed.ends_with(',') {
        return true;
    }
    false
}

fn collect_visible_storage_leaks(relative: &str) -> Vec<String> {
    let source = fs::read_to_string(engine_root(relative))
        .unwrap_or_else(|error| panic!("failed to read '{}': {error}", relative));
    let lines = source.lines().collect::<Vec<_>>();
    let mut leaks = Vec::new();
    let mut index = 0;
    while index < lines.len() {
        if !is_visible_declaration_start(lines[index]) {
            index += 1;
            continue;
        }

        let start_line = index + 1;
        let mut declaration = lines[index].trim_start().to_string();
        while !declaration_is_complete(&declaration) && index + 1 < lines.len() {
            index += 1;
            declaration.push('\n');
            declaration.push_str(lines[index].trim_start());
        }

        if declaration.contains("storage::") {
            leaks.push(format!("{relative}:{start_line}: {declaration}"));
        }

        index += 1;
    }
    leaks
}

#[test]
fn live_state_visible_surfaces_do_not_expose_storage_module() {
    let leaks = ["src/live_state/mod.rs", "src/live_state/schema_access.rs"]
        .into_iter()
        .flat_map(collect_visible_storage_leaks)
        .collect::<Vec<_>>();

    assert!(
        leaks.is_empty(),
        "live_state visible surface leaked storage signatures:\n{}",
        leaks.join("\n")
    );
}

#[test]
fn live_state_storage_module_remains_private_to_live_state() {
    let leaks = tracked_source_files()
        .into_iter()
        .filter_map(|path| {
            let relative = relative_to_manifest(&path);
            if is_live_state_path(&relative) || is_audit_support_path(&relative) {
                return None;
            }
            let source = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read '{}': {error}", path.display()));
            (source.contains("crate::live_state::storage")
                || source.contains("live_state::storage::"))
            .then_some(relative)
        })
        .collect::<Vec<_>>();

    assert!(
        leaks.is_empty(),
        "live_state/storage escaped the subsystem boundary:\n{}",
        leaks.join("\n")
    );
}
