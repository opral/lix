use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
struct ForbiddenDependencyRule {
    from_scope: &'static str,
    reason: &'static str,
    forbidden_scopes: &'static [&'static str],
}

const FORBIDDEN_DEPENDENCY_RULES: &[ForbiddenDependencyRule] = &[
    ForbiddenDependencyRule {
        from_scope: "catalog",
        reason: "catalog is the semantic owner for public named relations and must not depend on lowering, orchestration, or sidecar owners",
        forbidden_scopes: &[
            "backend",
            "canonical",
            "api",
            "execution",
            "init",
            "services",
            "session",
            "sql",
        ],
    },
    ForbiddenDependencyRule {
        from_scope: "backend",
        reason: "backend is a lower persistence owner; it owns raw prepared statement DTOs but must not grow dependencies on higher workflow or sidecar roots",
        forbidden_scopes: &["services"],
    },
    ForbiddenDependencyRule {
        from_scope: "services",
        reason: "services are leaf sidecar capabilities and may depend only on neutral foundations like common, not on engine composition or semantic owner roots",
        forbidden_scopes: &[
            "api",
            "backend",
            "canonical",
            "catalog",
            "diagnostics",
            "execution",
            "init",
            "live_state",
            "schema",
            "session",
            "sql",
        ],
    },
    ForbiddenDependencyRule {
        from_scope: "live_state",
        reason: "live_state is the generic projection engine and must not reacquire services sidecars or write orchestration owners",
        forbidden_scopes: &["execution", "services"],
    },
    ForbiddenDependencyRule {
        from_scope: "sql2",
        reason: "sql2 is the compiler/runtime provider lane; it must not depend on workflow or higher orchestration roots directly",
        forbidden_scopes: &["execution", "services", "session"],
    },
    ForbiddenDependencyRule {
        from_scope: "execution",
        reason: "execution is the public SQL runner leaf; it may consume sql-owned prepared artifacts but must not depend on higher orchestration owners or transaction internals",
        forbidden_scopes: &[
            "canonical",
            "api",
            "init",
            "services",
            "session",
            "transaction",
        ],
    },
    ForbiddenDependencyRule {
        from_scope: "session",
        reason: "session owns orchestration and workflow code, but should not couple itself to the root API shell",
        forbidden_scopes: &["api"],
    },
];

const TARGET_CORE_MODULES: &[&str] = &["backend", "live_state", "session", "sql2", "transaction"];

#[derive(Debug, Clone, PartialEq, Eq)]
struct EngineDependencyGraph {
    modules_analyzed: Vec<String>,
    edges: Vec<DependencyEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DependencyEdge {
    from: String,
    to: String,
    via_files: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SealedOwnerViolation {
    importer_file: String,
    imported_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ImportPathViolation {
    importer_file: String,
    imported_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RawSqlExecutionViolation {
    file: String,
    pattern: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RawBackendTypeViolation {
    file: String,
    type_name: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TransactionLifecycleViolation {
    file: String,
    pattern: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SqlRuntimeOwnershipViolation {
    file: String,
    pattern: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum UseToken {
    DblColon,
    LBrace,
    RBrace,
    Comma,
    Star,
    As,
    Ident(String),
}

const ALLOWED_SERVICE_FOUNDATION_ROOTS: &[&str] = &["common"];

fn engine_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn src_root() -> PathBuf {
    engine_root().join("src")
}

fn lib_path() -> PathBuf {
    src_root().join("lib.rs")
}

fn read_engine_source(relative: &str) -> String {
    fs::read_to_string(src_root().join(relative)).expect("engine source file should be readable")
}

fn source_between<'a>(
    relative: &str,
    source: &'a str,
    start_needle: &str,
    end_needle: &str,
) -> &'a str {
    let start = source
        .find(start_needle)
        .unwrap_or_else(|| panic!("{relative} should contain `{start_needle}`"));
    let end = source[start..]
        .find(end_needle)
        .map(|end| start + end)
        .unwrap_or_else(|| {
            panic!("{relative} should contain `{end_needle}` after `{start_needle}`")
        });
    &source[start..end]
}

fn assert_source_contains_all(relative: &str, source: &str, needles: &[&str]) {
    for needle in needles {
        assert!(
            source.contains(needle),
            "{relative} should contain `{needle}`",
        );
    }
}

fn assert_source_contains_none(relative: &str, source: &str, needles: &[&str]) {
    for needle in needles {
        assert!(
            !source.contains(needle),
            "{relative} should not contain `{needle}`",
        );
    }
}

fn analyze_engine_dependency_graph() -> EngineDependencyGraph {
    let lib_source = fs::read_to_string(lib_path()).expect("src/lib.rs should be readable");
    let top_level_modules = parse_top_level_modules(&lib_source);
    let module_set: HashSet<String> = top_level_modules.iter().cloned().collect();
    let mut graph: BTreeMap<String, BTreeSet<String>> = top_level_modules
        .iter()
        .cloned()
        .map(|module| (module, BTreeSet::new()))
        .collect();
    let mut edge_provenance: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();

    for module_name in &top_level_modules {
        for absolute_path in rust_files_for_top_level_module(module_name) {
            let relative_path = absolute_path
                .strip_prefix(src_root())
                .expect("module source file should be inside src/")
                .to_string_lossy()
                .replace('\\', "/");
            if is_test_support_relative_path(&relative_path) {
                continue;
            }
            let source =
                fs::read_to_string(&absolute_path).expect("module source file should be readable");
            let current_module_path = module_path_for_file(&relative_path);
            let dependencies = collect_dependencies_from_source(
                &strip_test_code(&source),
                &current_module_path,
                &module_set,
            );

            for dependency in dependencies {
                if dependency == *module_name {
                    continue;
                }
                graph
                    .get_mut(module_name)
                    .expect("all top-level modules should have graph entries")
                    .insert(dependency.clone());
                edge_provenance
                    .entry((module_name.clone(), dependency))
                    .or_default()
                    .insert(relative_path.clone());
            }
        }
    }

    let edges: Vec<DependencyEdge> = edge_provenance
        .into_iter()
        .map(|((from, to), via_files)| DependencyEdge {
            from,
            to,
            via_files: via_files.into_iter().collect(),
        })
        .collect();

    EngineDependencyGraph {
        modules_analyzed: top_level_modules,
        edges,
    }
}

fn parse_top_level_modules(lib_source: &str) -> Vec<String> {
    let mut modules = Vec::new();
    let mut seen = BTreeSet::new();
    let mut pending_attributes = Vec::new();

    for line in lib_source.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with("#[") {
            pending_attributes.push(trimmed.to_string());
            continue;
        }

        let mut cursor = trimmed;
        if let Some(rest) = cursor.strip_prefix("pub(crate) ") {
            cursor = rest;
        } else if let Some(rest) = cursor.strip_prefix("pub ") {
            cursor = rest;
        } else if cursor.starts_with("pub(") {
            if let Some(idx) = cursor.find(") ") {
                cursor = &cursor[idx + 2..];
            }
        }

        if let Some(rest) = cursor.strip_prefix("mod ") {
            if let Some(module_name) = rest.strip_suffix(';') {
                let is_test_only = pending_attributes
                    .iter()
                    .any(|attribute| attribute.contains("cfg(test)"));
                if !is_test_only {
                    let name = module_name.trim();
                    if !name.is_empty() && seen.insert(name.to_string()) {
                        modules.push(name.to_string());
                    }
                }
            }
        }

        pending_attributes.clear();
    }

    modules
}

fn rust_files_for_top_level_module(module_name: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let module_file = src_root().join(format!("{module_name}.rs"));
    let module_directory = src_root().join(module_name);

    if module_file.exists() {
        files.push(module_file);
    }
    if module_directory.exists() {
        walk_rust_files(&module_directory, &mut files);
    }

    files.sort();
    files
}

fn walk_rust_files(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("directory should be readable") {
        let entry = entry.expect("directory entry should be readable");
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "tests") {
                continue;
            }
            walk_rust_files(&path, files);
            continue;
        }
        if !path.is_file() {
            continue;
        }
        if path.extension().is_some_and(|ext| ext == "rs")
            && path.file_name().is_none_or(|name| name != "tests.rs")
        {
            files.push(path);
        }
    }
}

fn module_path_for_file(relative_path: &str) -> Vec<String> {
    let normalized: Vec<&str> = relative_path.split('/').collect();
    if normalized.len() == 1 {
        return vec![normalized[0].trim_end_matches(".rs").to_string()];
    }

    if normalized.last() == Some(&"mod.rs") {
        return normalized[..normalized.len() - 1]
            .iter()
            .map(|segment| (*segment).to_string())
            .collect();
    }

    let mut parts: Vec<String> = normalized[..normalized.len() - 1]
        .iter()
        .map(|segment| (*segment).to_string())
        .collect();
    let filename = normalized
        .last()
        .expect("relative path should contain a file name")
        .trim_end_matches(".rs");
    parts.push(filename.to_string());
    parts
}

fn collect_dependencies_from_source(
    source: &str,
    current_module_path: &[String],
    module_set: &HashSet<String>,
) -> BTreeSet<String> {
    let without_tests = strip_test_code(source);
    let sanitized = mask_rust_source(&without_tests);
    let mut dependencies = BTreeSet::new();

    dependencies.extend(collect_use_dependencies(
        &sanitized,
        current_module_path,
        module_set,
    ));
    dependencies.extend(collect_explicit_path_dependencies(
        &sanitized,
        current_module_path,
        module_set,
    ));

    dependencies
}

fn strip_test_code(source: &str) -> String {
    let stripped = strip_cfg_test_items(source);
    let masked = mask_rust_source(&stripped);
    let mut ranges = Vec::new();
    let bytes = masked.as_bytes();
    let mut index = 0usize;

    while index < bytes.len() {
        if let Some((mod_start, after_mod)) = match_keyword(bytes, index, b"mod") {
            let after_whitespace = skip_whitespace(bytes, after_mod);
            if let Some((ident, after_ident)) = parse_identifier(bytes, after_whitespace) {
                let ident = normalize_identifier(&ident);
                let after_name = skip_whitespace(bytes, after_ident);
                if ident == "tests" && bytes.get(after_name) == Some(&b'{') {
                    if let Some(close_brace_index) = find_matching_brace(bytes, after_name) {
                        ranges.push((mod_start, close_brace_index + 1));
                        index = close_brace_index + 1;
                        continue;
                    }
                }
            }
        }
        index += 1;
    }

    let mut result = stripped;
    ranges.sort_by_key(|range| std::cmp::Reverse(range.0));
    for (start, end) in ranges {
        result.replace_range(start..end, "");
    }
    result
}

fn strip_cfg_test_items(source: &str) -> String {
    let lines: Vec<&str> = source.lines().collect();
    let mut output = String::new();
    let mut index = 0usize;

    while index < lines.len() {
        let line = lines[index];
        let trimmed = line.trim_start();
        if trimmed.starts_with("#[") && trimmed.contains("cfg(test)") {
            index += 1;
            while index < lines.len() && lines[index].trim_start().starts_with("#[") {
                index += 1;
            }
            skip_annotated_item(&lines, &mut index);
            continue;
        }

        output.push_str(line);
        output.push('\n');
        index += 1;
    }

    output
}

fn skip_annotated_item(lines: &[&str], index: &mut usize) {
    let mut brace_depth = 0i32;
    let mut saw_item_body = false;

    while *index < lines.len() {
        let line = lines[*index];
        brace_depth += brace_delta(line);
        saw_item_body |= line.contains('{') || line.trim_end().ends_with(';');
        *index += 1;

        if saw_item_body && brace_depth <= 0 {
            break;
        }
    }
}

fn brace_delta(line: &str) -> i32 {
    line.chars().fold(0, |count, ch| match ch {
        '{' => count + 1,
        '}' => count - 1,
        _ => count,
    })
}

fn mask_rust_source(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut result = vec![b' '; bytes.len()];
    let mut index = 0usize;
    let mut block_comment_depth = 0usize;

    while index < bytes.len() {
        let current = bytes[index];
        let next = bytes.get(index + 1).copied().unwrap_or_default();

        if block_comment_depth > 0 {
            if current == b'/' && next == b'*' {
                block_comment_depth += 1;
                index += 2;
                continue;
            }
            if current == b'*' && next == b'/' {
                block_comment_depth -= 1;
                index += 2;
                continue;
            }
            if current == b'\n' {
                result[index] = b'\n';
            }
            index += 1;
            continue;
        }

        if current == b'/' && next == b'/' {
            index += 2;
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }

        if current == b'/' && next == b'*' {
            block_comment_depth = 1;
            index += 2;
            continue;
        }

        if current == b'"' {
            result[index] = b' ';
            index += 1;
            while index < bytes.len() {
                let ch = bytes[index];
                if ch == b'\n' {
                    result[index] = b'\n';
                }
                index += 1;
                if ch == b'\\' {
                    if index < bytes.len() {
                        if bytes[index] == b'\n' {
                            result[index] = b'\n';
                        }
                        index += 1;
                    }
                    continue;
                }
                if ch == b'"' {
                    break;
                }
            }
            continue;
        }

        if current == b'r' {
            let mut probe = index + 1;
            while bytes.get(probe) == Some(&b'#') {
                probe += 1;
            }
            if bytes.get(probe) == Some(&b'"') {
                let hash_count = probe - index - 1;
                let closing_len = hash_count + 1;
                index = probe + 1;
                while index < bytes.len() {
                    if bytes[index] == b'\n' {
                        result[index] = b'\n';
                    }
                    if bytes[index] == b'"'
                        && bytes
                            .get(index + 1..index + 1 + hash_count)
                            .is_some_and(|suffix| suffix.iter().all(|byte| *byte == b'#'))
                    {
                        index += closing_len;
                        break;
                    }
                    index += 1;
                }
                continue;
            }
        }

        result[index] = current;
        index += 1;
    }

    String::from_utf8(result).expect("masked Rust source should stay valid UTF-8")
}

fn collect_use_dependencies(
    source: &str,
    current_module_path: &[String],
    module_set: &HashSet<String>,
) -> BTreeSet<String> {
    let bytes = source.as_bytes();
    let mut dependencies = BTreeSet::new();
    let mut index = 0usize;

    while index < bytes.len() {
        if let Some((_, after_use)) = match_keyword(bytes, index, b"use") {
            let mut cursor = after_use;
            while cursor < bytes.len() && bytes[cursor] != b';' {
                cursor += 1;
            }
            if cursor < bytes.len() {
                let spec = &source[after_use..cursor];
                dependencies.extend(resolve_use_dependencies(
                    spec,
                    current_module_path,
                    module_set,
                ));
                index = cursor + 1;
                continue;
            }
        }
        index += 1;
    }

    dependencies
}

fn resolve_use_dependencies(
    spec: &str,
    current_module_path: &[String],
    module_set: &HashSet<String>,
) -> BTreeSet<String> {
    let tokens = tokenize_use_spec(spec);
    let mut dependencies = BTreeSet::new();
    let mut index = 0usize;

    while index < tokens.len() {
        index = parse_use_tree(
            &tokens,
            index,
            current_module_path,
            None,
            module_set,
            &mut dependencies,
        );
        if matches!(tokens.get(index), Some(UseToken::Comma)) {
            index += 1;
        } else {
            break;
        }
    }

    dependencies
}

fn tokenize_use_spec(spec: &str) -> Vec<UseToken> {
    let bytes = spec.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0usize;

    while index < bytes.len() {
        let current = bytes[index];
        let next = bytes.get(index + 1).copied().unwrap_or_default();

        if current.is_ascii_whitespace() {
            index += 1;
            continue;
        }
        if current == b':' && next == b':' {
            tokens.push(UseToken::DblColon);
            index += 2;
            continue;
        }
        if current == b'{' {
            tokens.push(UseToken::LBrace);
            index += 1;
            continue;
        }
        if current == b'}' {
            tokens.push(UseToken::RBrace);
            index += 1;
            continue;
        }
        if current == b',' {
            tokens.push(UseToken::Comma);
            index += 1;
            continue;
        }
        if current == b'*' {
            tokens.push(UseToken::Star);
            index += 1;
            continue;
        }
        if let Some((ident, next_index)) = parse_identifier(bytes, index) {
            let normalized = normalize_identifier(&ident);
            if normalized == "as" {
                tokens.push(UseToken::As);
            } else {
                tokens.push(UseToken::Ident(normalized));
            }
            index = next_index;
            continue;
        }

        index += 1;
    }

    tokens
}

fn parse_use_tree(
    tokens: &[UseToken],
    index: usize,
    current_module_path: &[String],
    base_context: Option<&[String]>,
    module_set: &HashSet<String>,
    dependencies: &mut BTreeSet<String>,
) -> usize {
    let (path_parts, next_index) = parse_use_path(tokens, index);
    if path_parts.is_empty() {
        return skip_until_boundary(tokens, index);
    }

    let resolved_path = resolve_use_path(&path_parts, current_module_path, base_context);
    if let Some(dependency) = resolved_path.first() {
        if module_set.contains(dependency) {
            dependencies.insert(dependency.clone());
        }
    }

    let mut cursor = next_index;
    if matches!(tokens.get(cursor), Some(UseToken::DblColon))
        && matches!(tokens.get(cursor + 1), Some(UseToken::LBrace))
    {
        cursor += 2;
        while cursor < tokens.len() && !matches!(tokens.get(cursor), Some(UseToken::RBrace)) {
            cursor = parse_use_tree(
                tokens,
                cursor,
                current_module_path,
                Some(&resolved_path),
                module_set,
                dependencies,
            );
            if matches!(tokens.get(cursor), Some(UseToken::Comma)) {
                cursor += 1;
            }
        }
        if matches!(tokens.get(cursor), Some(UseToken::RBrace)) {
            cursor += 1;
        }
        return cursor;
    }

    if matches!(tokens.get(cursor), Some(UseToken::DblColon))
        && matches!(tokens.get(cursor + 1), Some(UseToken::Star))
    {
        return cursor + 2;
    }

    if matches!(tokens.get(cursor), Some(UseToken::As)) {
        return cursor
            + if matches!(tokens.get(cursor + 1), Some(UseToken::Ident(_))) {
                2
            } else {
                1
            };
    }

    cursor
}

fn parse_use_path(tokens: &[UseToken], index: usize) -> (Vec<String>, usize) {
    let mut path_parts = Vec::new();
    let mut cursor = index;

    while let Some(UseToken::Ident(value)) = tokens.get(cursor) {
        path_parts.push(value.clone());
        if matches!(tokens.get(cursor + 1), Some(UseToken::DblColon))
            && matches!(tokens.get(cursor + 2), Some(UseToken::Ident(_)))
        {
            cursor += 2;
            continue;
        }
        cursor += 1;
        break;
    }

    (path_parts, cursor)
}

fn resolve_use_path(
    path_parts: &[String],
    current_module_path: &[String],
    base_context: Option<&[String]>,
) -> Vec<String> {
    if let Some(base_context) = base_context {
        if path_parts.first().is_some_and(|part| part == "self") {
            let mut result = base_context.to_vec();
            result.extend(path_parts.iter().skip(1).cloned());
            return result;
        }
        if path_parts
            .first()
            .is_some_and(|part| part == "crate" || part == "super")
        {
            return resolve_relative_path(path_parts, current_module_path);
        }
        let mut result = base_context.to_vec();
        result.extend(path_parts.iter().cloned());
        return result;
    }

    if path_parts
        .first()
        .is_none_or(|part| part != "crate" && part != "self" && part != "super")
    {
        return Vec::new();
    }

    resolve_relative_path(path_parts, current_module_path)
}

fn resolve_relative_path(path_parts: &[String], current_module_path: &[String]) -> Vec<String> {
    if path_parts.first().is_some_and(|part| part == "crate") {
        return path_parts.iter().skip(1).cloned().collect();
    }
    if path_parts.first().is_some_and(|part| part == "self") {
        let mut result = current_module_path.to_vec();
        result.extend(path_parts.iter().skip(1).cloned());
        return result;
    }

    let super_count = path_parts
        .iter()
        .take_while(|part| *part == "super")
        .count();
    let mut result: Vec<String> = current_module_path
        .iter()
        .take(current_module_path.len().saturating_sub(super_count))
        .cloned()
        .collect();
    result.extend(path_parts.iter().skip(super_count).cloned());
    result
}

fn skip_until_boundary(tokens: &[UseToken], index: usize) -> usize {
    let mut cursor = index;
    while cursor < tokens.len()
        && !matches!(tokens.get(cursor), Some(UseToken::Comma | UseToken::RBrace))
    {
        cursor += 1;
    }
    cursor
}

fn collect_explicit_path_dependencies(
    source: &str,
    current_module_path: &[String],
    module_set: &HashSet<String>,
) -> BTreeSet<String> {
    let bytes = source.as_bytes();
    let mut dependencies = BTreeSet::new();
    let mut index = 0usize;

    while index < bytes.len() {
        let Some((prefix, after_prefix)) = parse_explicit_prefix(bytes, index) else {
            index += 1;
            continue;
        };

        let after_separator = skip_whitespace(bytes, after_prefix);
        if bytes.get(after_separator..after_separator + 2) != Some(&b"::"[..]) {
            index += 1;
            continue;
        }

        let after_double_colon = skip_whitespace(bytes, after_separator + 2);
        let Some((first_segment, after_first_segment)) =
            parse_identifier(bytes, after_double_colon)
        else {
            index += 1;
            continue;
        };

        let dependency = resolve_explicit_dependency(
            &prefix,
            &normalize_identifier(&first_segment),
            current_module_path,
        );
        if let Some(dependency) = dependency {
            if module_set.contains(&dependency) {
                dependencies.insert(dependency);
            }
        }

        index = after_first_segment;
    }

    dependencies
}

fn parse_explicit_prefix(bytes: &[u8], index: usize) -> Option<(Vec<String>, usize)> {
    let (ident, mut cursor) = parse_identifier(bytes, index)?;
    let normalized = normalize_identifier(&ident);
    if normalized != "crate" && normalized != "self" && normalized != "super" {
        return None;
    }

    let mut prefix = vec![normalized];
    loop {
        let after_whitespace = skip_whitespace(bytes, cursor);
        if bytes.get(after_whitespace..after_whitespace + 2) != Some(&b"::"[..]) {
            return Some((prefix, cursor));
        }
        let after_separator = skip_whitespace(bytes, after_whitespace + 2);
        let Some((next_ident, next_cursor)) = parse_identifier(bytes, after_separator) else {
            return Some((prefix, cursor));
        };
        let next_ident = normalize_identifier(&next_ident);
        if next_ident != "super" {
            return Some((prefix, cursor));
        }
        prefix.push(next_ident);
        cursor = next_cursor;
    }
}

fn resolve_explicit_dependency(
    prefix: &[String],
    first_segment: &str,
    current_module_path: &[String],
) -> Option<String> {
    match prefix.first()?.as_str() {
        "crate" => Some(first_segment.to_string()),
        "self" => current_module_path.first().cloned(),
        "super" => {
            let super_count = prefix.iter().filter(|segment| *segment == "super").count();
            let mut absolute_path: Vec<String> = current_module_path
                .iter()
                .take(current_module_path.len().saturating_sub(super_count))
                .cloned()
                .collect();
            absolute_path.push(first_segment.to_string());
            absolute_path.first().cloned()
        }
        _ => None,
    }
}

fn parse_identifier(bytes: &[u8], index: usize) -> Option<(String, usize)> {
    let current = *bytes.get(index)?;
    if current == b'r' && bytes.get(index + 1) == Some(&b'#') {
        let mut cursor = index + 2;
        if !bytes.get(cursor).is_some_and(|byte| is_ident_start(*byte)) {
            return None;
        }
        cursor += 1;
        while bytes
            .get(cursor)
            .is_some_and(|byte| is_ident_continue(*byte))
        {
            cursor += 1;
        }
        return Some((
            String::from_utf8(bytes[index..cursor].to_vec())
                .expect("raw identifier should stay valid UTF-8"),
            cursor,
        ));
    }

    if !is_ident_start(current) {
        return None;
    }

    let mut cursor = index + 1;
    while bytes
        .get(cursor)
        .is_some_and(|byte| is_ident_continue(*byte))
    {
        cursor += 1;
    }

    Some((
        String::from_utf8(bytes[index..cursor].to_vec())
            .expect("identifier should stay valid UTF-8"),
        cursor,
    ))
}

fn normalize_identifier(identifier: &str) -> String {
    identifier
        .strip_prefix("r#")
        .unwrap_or(identifier)
        .to_string()
}

fn is_ident_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_ident_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn skip_whitespace(bytes: &[u8], mut index: usize) -> usize {
    while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
        index += 1;
    }
    index
}

fn match_keyword(bytes: &[u8], index: usize, keyword: &[u8]) -> Option<(usize, usize)> {
    let end = index.checked_add(keyword.len())?;
    if bytes.get(index..end)? != keyword {
        return None;
    }

    let boundary_before = index == 0 || !is_ident_continue(bytes[index - 1]);
    let boundary_after = bytes.get(end).is_none_or(|byte| !is_ident_continue(*byte));
    if boundary_before && boundary_after {
        Some((index, end))
    } else {
        None
    }
}

fn find_matching_brace(bytes: &[u8], open_brace_index: usize) -> Option<usize> {
    let mut depth = 0i32;
    for (index, byte) in bytes.iter().copied().enumerate().skip(open_brace_index) {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

fn tarjan(nodes: &[String], graph: &BTreeMap<String, BTreeSet<String>>) -> Vec<Vec<String>> {
    fn strong_connect(
        node: &str,
        graph: &BTreeMap<String, BTreeSet<String>>,
        next_index: &mut usize,
        stack: &mut Vec<String>,
        on_stack: &mut HashSet<String>,
        index_by_node: &mut HashMap<String, usize>,
        low_link_by_node: &mut HashMap<String, usize>,
        components: &mut Vec<Vec<String>>,
    ) {
        index_by_node.insert(node.to_string(), *next_index);
        low_link_by_node.insert(node.to_string(), *next_index);
        *next_index += 1;
        stack.push(node.to_string());
        on_stack.insert(node.to_string());

        for neighbor in graph
            .get(node)
            .into_iter()
            .flat_map(|neighbors| neighbors.iter())
        {
            if !index_by_node.contains_key(neighbor) {
                strong_connect(
                    neighbor,
                    graph,
                    next_index,
                    stack,
                    on_stack,
                    index_by_node,
                    low_link_by_node,
                    components,
                );
                let new_low_link = low_link_by_node[node].min(low_link_by_node[neighbor]);
                low_link_by_node.insert(node.to_string(), new_low_link);
            } else if on_stack.contains(neighbor) {
                let new_low_link = low_link_by_node[node].min(index_by_node[neighbor]);
                low_link_by_node.insert(node.to_string(), new_low_link);
            }
        }

        if low_link_by_node[node] != index_by_node[node] {
            return;
        }

        let mut component = Vec::new();
        while let Some(member) = stack.pop() {
            on_stack.remove(&member);
            component.push(member.clone());
            if member == node {
                break;
            }
        }
        components.push(component);
    }

    let mut next_index = 0usize;
    let mut stack = Vec::new();
    let mut on_stack = HashSet::new();
    let mut index_by_node = HashMap::new();
    let mut low_link_by_node = HashMap::new();
    let mut components = Vec::new();

    for node in nodes {
        if !index_by_node.contains_key(node) {
            strong_connect(
                node,
                graph,
                &mut next_index,
                &mut stack,
                &mut on_stack,
                &mut index_by_node,
                &mut low_link_by_node,
                &mut components,
            );
        }
    }

    components
}

fn module_set(graph: &EngineDependencyGraph) -> BTreeSet<String> {
    graph.modules_analyzed.iter().cloned().collect()
}

fn forbidden_dependency_lookup() -> BTreeMap<&'static str, &'static ForbiddenDependencyRule> {
    let mut lookup = BTreeMap::new();
    for rule in FORBIDDEN_DEPENDENCY_RULES {
        let replaced = lookup.insert(rule.from_scope, rule);
        assert!(
            replaced.is_none(),
            "forbidden dependency map must define each source scope only once; duplicate `{}`",
            rule.from_scope,
        );
    }
    lookup
}

fn actual_architecture_violations<'a>(
    graph: &'a EngineDependencyGraph,
    forbidden_lookup: &BTreeMap<&'static str, &'static ForbiddenDependencyRule>,
) -> Vec<&'a DependencyEdge> {
    graph
        .edges
        .iter()
        .filter(|edge| {
            forbidden_lookup
                .get(edge.from.as_str())
                .is_some_and(|rule| rule.forbidden_scopes.contains(&edge.to.as_str()))
        })
        .collect()
}

fn target_core_graph(graph: &EngineDependencyGraph) -> BTreeMap<String, BTreeSet<String>> {
    let target_core_modules: BTreeSet<String> = TARGET_CORE_MODULES
        .iter()
        .map(|module| (*module).to_string())
        .collect();
    let mut filtered: BTreeMap<String, BTreeSet<String>> = target_core_modules
        .iter()
        .cloned()
        .map(|module| (module, BTreeSet::new()))
        .collect();

    for edge in &graph.edges {
        if !target_core_modules.contains(&edge.from) || !target_core_modules.contains(&edge.to) {
            continue;
        }
        if target_core_transition_allows_edge(edge) {
            continue;
        }
        filtered
            .get_mut(&edge.from)
            .expect("target core graph should contain every filtered source")
            .insert(edge.to.clone());
    }

    filtered
}

fn target_core_transition_allows_edge(edge: &DependencyEdge) -> bool {
    (edge.from == "transaction" && edge.to == "session")
        || (edge.from == "sql2" && edge.to == "transaction")
}

fn render_target_core_graph(graph: &BTreeMap<String, BTreeSet<String>>) -> String {
    let mut rendered = String::new();

    for (module, outgoing) in graph {
        let neighbors = outgoing.iter().cloned().collect::<Vec<_>>().join(", ");
        let _ = writeln!(&mut rendered, "{module} -> [{neighbors}]");
    }

    rendered
}

fn owner_root_cycles(graph: &BTreeMap<String, BTreeSet<String>>) -> Vec<Vec<String>> {
    let nodes = graph.keys().cloned().collect::<Vec<_>>();
    let mut cycles = tarjan(&nodes, graph)
        .into_iter()
        .filter(|component| {
            component.len() > 1
                || component.first().is_some_and(|node| {
                    graph
                        .get(node)
                        .is_some_and(|neighbors| neighbors.contains(node))
                })
        })
        .map(|mut component| {
            component.sort();
            component
        })
        .collect::<Vec<_>>();
    cycles.sort();
    cycles
}

fn render_owner_root_cycles(cycles: &[Vec<String>]) -> String {
    let mut rendered = String::new();
    for cycle in cycles {
        let _ = writeln!(&mut rendered, "  - {}", cycle.join(" -> "));
    }
    rendered
}

fn render_forbidden_dependency_violations(
    violations: &[&DependencyEdge],
    forbidden_lookup: &BTreeMap<&'static str, &'static ForbiddenDependencyRule>,
) -> String {
    let mut grouped: BTreeMap<&str, Vec<&DependencyEdge>> = BTreeMap::new();

    for violation in violations {
        grouped
            .entry(violation.from.as_str())
            .or_default()
            .push(*violation);
    }

    let mut rendered = String::new();
    for (from_scope, edges) in grouped {
        let rule = forbidden_lookup
            .get(from_scope)
            .expect("every forbidden violation should have a matching rule");
        let _ = writeln!(&mut rendered, "{from_scope}: {}", rule.reason);
        for edge in edges {
            let _ = writeln!(&mut rendered, "  - {} -> {}", edge.from, edge.to);
            for via_file in &edge.via_files {
                let _ = writeln!(&mut rendered, "    via {via_file}");
            }
        }
    }

    rendered
}

fn production_source_files() -> Vec<(String, String)> {
    let lib_source = fs::read_to_string(lib_path()).expect("src/lib.rs should be readable");
    let top_level_modules = parse_top_level_modules(&lib_source);
    let mut files = Vec::new();

    files.push(("lib.rs".to_string(), strip_test_code(&lib_source)));

    for module_name in top_level_modules {
        for absolute_path in rust_files_for_top_level_module(&module_name) {
            let relative_path = absolute_path
                .strip_prefix(src_root())
                .expect("module source file should be inside src/")
                .to_string_lossy()
                .replace('\\', "/");
            if is_test_support_relative_path(&relative_path) {
                continue;
            }
            let source =
                fs::read_to_string(&absolute_path).expect("module source file should be readable");
            files.push((relative_path, strip_test_code(&source)));
        }
    }

    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn source_test_and_bench_rust_files() -> Vec<(String, String)> {
    let mut files = production_source_files();

    for directory in ["tests", "benches"] {
        let root = engine_root().join(directory);
        if !root.exists() {
            continue;
        }
        let mut extra_files = Vec::new();
        walk_rust_files(&root, &mut extra_files);

        for absolute_path in extra_files {
            let relative_path = absolute_path
                .strip_prefix(engine_root())
                .expect("test or bench source file should be inside the engine root")
                .to_string_lossy()
                .replace('\\', "/");
            let source = fs::read_to_string(&absolute_path)
                .expect("test or bench source file should be readable");
            files.push((relative_path, source));
        }
    }

    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn is_test_support_relative_path(relative_path: &str) -> bool {
    let parts: Vec<&str> = relative_path.split('/').collect();
    parts.iter().any(|part| {
        *part == "tests"
            || *part == "test"
            || *part == "test_support.rs"
            || *part == "test_support"
            || part
                .strip_suffix(".rs")
                .is_some_and(|stem| stem.ends_with("_tests"))
            || part.ends_with("_tests")
    })
}

fn root_module_entry_relative_path(module_name: &str) -> Option<String> {
    let module_file = src_root().join(format!("{module_name}.rs"));
    if module_file.exists() {
        return Some(format!("{module_name}.rs"));
    }

    let module_mod_file = src_root().join(module_name).join("mod.rs");
    if module_mod_file.exists() {
        return Some(format!("{module_name}/mod.rs"));
    }

    None
}

fn parse_declared_modules(source: &str) -> Vec<String> {
    let mut modules = Vec::new();
    let mut pending_attributes = Vec::new();

    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.starts_with("#[") {
            pending_attributes.push(trimmed.to_string());
            continue;
        }

        let mut cursor = trimmed;
        if let Some(rest) = cursor.strip_prefix("pub(crate) ") {
            cursor = rest;
        } else if let Some(rest) = cursor.strip_prefix("pub ") {
            cursor = rest;
        } else if cursor.starts_with("pub(") {
            if let Some(idx) = cursor.find(") ") {
                cursor = &cursor[idx + 2..];
            }
        }

        if let Some(rest) = cursor.strip_prefix("mod ") {
            if let Some(module_name) = rest.strip_suffix(';') {
                let is_test_only = pending_attributes
                    .iter()
                    .any(|attribute| attribute.contains("cfg(test)"));
                if !is_test_only {
                    let name = module_name.trim();
                    if !name.is_empty() {
                        modules.push(name.to_string());
                    }
                }
            }
        }

        pending_attributes.clear();
    }

    modules
}

fn sealed_owner_child_modules() -> BTreeMap<String, BTreeSet<String>> {
    let lib_source = fs::read_to_string(lib_path()).expect("src/lib.rs should be readable");
    let top_level_modules = parse_top_level_modules(&lib_source);
    let mut child_modules = BTreeMap::new();

    for module_name in top_level_modules {
        let Some(relative_path) = root_module_entry_relative_path(&module_name) else {
            continue;
        };
        let source = read_engine_source(&relative_path);
        let declared_modules = parse_declared_modules(&strip_test_code(&source));
        child_modules.insert(module_name, declared_modules.into_iter().collect());
    }

    child_modules
}

fn collect_module_paths_from_source(
    source: &str,
    current_module_path: &[String],
    module_set: &HashSet<String>,
) -> BTreeSet<Vec<String>> {
    let without_tests = strip_test_code(source);
    let sanitized = mask_rust_source(&without_tests);
    let mut paths = BTreeSet::new();

    paths.extend(collect_use_paths_from_source(
        &sanitized,
        current_module_path,
        module_set,
    ));
    paths.extend(collect_explicit_paths_from_source(
        &sanitized,
        current_module_path,
        module_set,
    ));

    paths
}

fn collect_use_paths_from_source(
    source: &str,
    current_module_path: &[String],
    module_set: &HashSet<String>,
) -> BTreeSet<Vec<String>> {
    let bytes = source.as_bytes();
    let mut paths = BTreeSet::new();
    let mut index = 0usize;

    while index < bytes.len() {
        if let Some((_, after_use)) = match_keyword(bytes, index, b"use") {
            let mut cursor = after_use;
            while cursor < bytes.len() && bytes[cursor] != b';' {
                cursor += 1;
            }
            if cursor < bytes.len() {
                let spec = &source[after_use..cursor];
                paths.extend(resolve_use_paths(spec, current_module_path, module_set));
                index = cursor + 1;
                continue;
            }
        }
        index += 1;
    }

    paths
}

fn resolve_use_paths(
    spec: &str,
    current_module_path: &[String],
    module_set: &HashSet<String>,
) -> BTreeSet<Vec<String>> {
    let tokens = tokenize_use_spec(spec);
    let mut paths = BTreeSet::new();
    let mut index = 0usize;

    while index < tokens.len() {
        index = parse_use_tree_paths(
            &tokens,
            index,
            current_module_path,
            None,
            module_set,
            &mut paths,
        );
        if matches!(tokens.get(index), Some(UseToken::Comma)) {
            index += 1;
        } else {
            break;
        }
    }

    paths
}

fn parse_use_tree_paths(
    tokens: &[UseToken],
    index: usize,
    current_module_path: &[String],
    base_context: Option<&[String]>,
    module_set: &HashSet<String>,
    paths: &mut BTreeSet<Vec<String>>,
) -> usize {
    let (path_parts, next_index) = parse_use_path(tokens, index);
    if path_parts.is_empty() {
        return skip_until_boundary(tokens, index);
    }

    let resolved_path = resolve_use_path(&path_parts, current_module_path, base_context);
    if resolved_path
        .first()
        .is_some_and(|dependency| module_set.contains(dependency))
    {
        paths.insert(resolved_path.clone());
    }

    let mut cursor = next_index;
    if matches!(tokens.get(cursor), Some(UseToken::DblColon))
        && matches!(tokens.get(cursor + 1), Some(UseToken::LBrace))
    {
        cursor += 2;
        while cursor < tokens.len() && !matches!(tokens.get(cursor), Some(UseToken::RBrace)) {
            cursor = parse_use_tree_paths(
                tokens,
                cursor,
                current_module_path,
                Some(&resolved_path),
                module_set,
                paths,
            );
            if matches!(tokens.get(cursor), Some(UseToken::Comma)) {
                cursor += 1;
            }
        }
        if matches!(tokens.get(cursor), Some(UseToken::RBrace)) {
            cursor += 1;
        }
        return cursor;
    }

    if matches!(tokens.get(cursor), Some(UseToken::DblColon))
        && matches!(tokens.get(cursor + 1), Some(UseToken::Star))
    {
        return cursor + 2;
    }

    if matches!(tokens.get(cursor), Some(UseToken::As)) {
        return cursor
            + if matches!(tokens.get(cursor + 1), Some(UseToken::Ident(_))) {
                2
            } else {
                1
            };
    }

    cursor
}

fn collect_explicit_paths_from_source(
    source: &str,
    current_module_path: &[String],
    module_set: &HashSet<String>,
) -> BTreeSet<Vec<String>> {
    let bytes = source.as_bytes();
    let mut paths = BTreeSet::new();
    let mut index = 0usize;

    while index < bytes.len() {
        let Some((prefix, after_prefix)) = parse_explicit_prefix(bytes, index) else {
            index += 1;
            continue;
        };

        let after_separator = skip_whitespace(bytes, after_prefix);
        if bytes.get(after_separator..after_separator + 2) != Some(&b"::"[..]) {
            index += 1;
            continue;
        }

        let mut cursor = skip_whitespace(bytes, after_separator + 2);
        let mut segments = Vec::new();

        while let Some((segment, after_segment)) = parse_identifier(bytes, cursor) {
            segments.push(normalize_identifier(&segment));
            let after_whitespace = skip_whitespace(bytes, after_segment);
            if bytes.get(after_whitespace..after_whitespace + 2) == Some(&b"::"[..]) {
                cursor = skip_whitespace(bytes, after_whitespace + 2);
                continue;
            }
            cursor = after_segment;
            break;
        }

        if segments.is_empty() {
            index += 1;
            continue;
        }

        let resolved_path = resolve_explicit_path(&prefix, &segments, current_module_path);
        if resolved_path
            .first()
            .is_some_and(|dependency| module_set.contains(dependency))
        {
            paths.insert(resolved_path);
        }

        index = cursor.max(index + 1);
    }

    paths
}

fn resolve_explicit_path(
    prefix: &[String],
    segments: &[String],
    current_module_path: &[String],
) -> Vec<String> {
    match prefix.first().map(String::as_str) {
        Some("crate") => segments.to_vec(),
        Some("self") => {
            let mut result = current_module_path.to_vec();
            result.extend(segments.iter().cloned());
            result
        }
        Some("super") => {
            let super_count = prefix.iter().filter(|segment| *segment == "super").count();
            let mut result: Vec<String> = current_module_path
                .iter()
                .take(current_module_path.len().saturating_sub(super_count))
                .cloned()
                .collect();
            result.extend(segments.iter().cloned());
            result
        }
        _ => Vec::new(),
    }
}

fn current_sealed_owner_violations() -> Vec<SealedOwnerViolation> {
    let lib_source = fs::read_to_string(lib_path()).expect("src/lib.rs should be readable");
    let top_level_modules = parse_top_level_modules(&lib_source);
    let module_set: HashSet<String> = top_level_modules.iter().cloned().collect();
    let child_modules = sealed_owner_child_modules();
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        let current_module_path = module_path_for_file(&relative_path);
        let Some(current_root) = current_module_path.first() else {
            continue;
        };

        for imported_path in
            collect_module_paths_from_source(&source, &current_module_path, &module_set)
        {
            if imported_path.len() < 2 {
                continue;
            }
            let owner_root = imported_path[0].as_str();
            if owner_root == current_root {
                continue;
            }
            if sealed_owner_allows_importer(owner_root, &relative_path) {
                continue;
            }
            if sealed_owner_allows_import_path(owner_root, &imported_path) {
                continue;
            }

            if !violates_sealed_owner_boundary(owner_root, &imported_path, &child_modules) {
                continue;
            }

            violations.insert(SealedOwnerViolation {
                importer_file: relative_path.clone(),
                imported_path: imported_path.join("::"),
            });
        }
    }

    violations.into_iter().collect()
}

fn violates_sealed_owner_boundary(
    owner_root: &str,
    imported_path: &[String],
    child_modules: &BTreeMap<String, BTreeSet<String>>,
) -> bool {
    if sealed_owner_root_facade_owners().contains(owner_root) {
        return true;
    }

    child_modules
        .get(owner_root)
        .is_some_and(|owner_child_modules| owner_child_modules.contains(&imported_path[1]))
}

fn sealed_owner_root_facade_owners() -> BTreeSet<&'static str> {
    ["api"].into_iter().collect()
}

fn sealed_owner_allows_importer(owner_root: &str, importer_file: &str) -> bool {
    (matches!(owner_root, "api") && importer_file == "lib.rs")
        || importer_file == "storage_bench.rs"
}

fn sealed_owner_allows_import_path(owner_root: &str, imported_path: &[String]) -> bool {
    owner_root == "transaction"
        && imported_path
            .get(1)
            .is_some_and(|segment| segment == "types")
}

fn render_grouped_sealed_owner_violations(violations: &[SealedOwnerViolation]) -> String {
    let mut grouped: BTreeMap<&str, BTreeMap<&str, Vec<&str>>> = BTreeMap::new();

    for violation in violations {
        let owner_root = violation
            .imported_path
            .split("::")
            .next()
            .expect("imported path should include an owner root");
        grouped
            .entry(owner_root)
            .or_default()
            .entry(violation.importer_file.as_str())
            .or_default()
            .push(violation.imported_path.as_str());
    }

    let mut rendered = String::new();
    for (owner_root, files) in grouped {
        let _ = writeln!(&mut rendered, "{owner_root}:");
        for (file, imported_paths) in files {
            let _ = writeln!(&mut rendered, "  {file}:");
            for imported_path in imported_paths {
                let _ = writeln!(&mut rendered, "    - {imported_path}");
            }
        }
    }

    rendered
}

fn render_grouped_import_path_violations(violations: &[ImportPathViolation]) -> String {
    let mut grouped: BTreeMap<&str, Vec<&str>> = BTreeMap::new();

    for violation in violations {
        grouped
            .entry(violation.importer_file.as_str())
            .or_default()
            .push(violation.imported_path.as_str());
    }

    let mut rendered = String::new();
    for (file, imported_paths) in grouped {
        let _ = writeln!(&mut rendered, "{file}:");
        for imported_path in imported_paths {
            let _ = writeln!(&mut rendered, "  - {imported_path}");
        }
    }

    rendered
}

fn render_grouped_raw_sql_execution_violations(violations: &[RawSqlExecutionViolation]) -> String {
    let mut grouped: BTreeMap<&str, Vec<&str>> = BTreeMap::new();

    for violation in violations {
        grouped
            .entry(violation.file.as_str())
            .or_default()
            .push(violation.pattern);
    }

    let mut rendered = String::new();
    for (file, patterns) in grouped {
        let _ = writeln!(&mut rendered, "{file}:");
        for pattern in patterns {
            let _ = writeln!(&mut rendered, "  - {pattern}");
        }
    }

    rendered
}

fn render_grouped_raw_backend_type_violations(violations: &[RawBackendTypeViolation]) -> String {
    let mut grouped: BTreeMap<&str, Vec<&str>> = BTreeMap::new();

    for violation in violations {
        grouped
            .entry(violation.file.as_str())
            .or_default()
            .push(violation.type_name);
    }

    let mut rendered = String::new();
    for (file, type_names) in grouped {
        let _ = writeln!(&mut rendered, "{file}:");
        for type_name in type_names {
            let _ = writeln!(&mut rendered, "  - {type_name}");
        }
    }

    rendered
}

fn render_grouped_transaction_lifecycle_violations(
    violations: &[TransactionLifecycleViolation],
) -> String {
    let mut grouped: BTreeMap<&str, Vec<&str>> = BTreeMap::new();

    for violation in violations {
        grouped
            .entry(violation.file.as_str())
            .or_default()
            .push(violation.pattern);
    }

    let mut rendered = String::new();
    for (file, patterns) in grouped {
        let _ = writeln!(&mut rendered, "{file}:");
        for pattern in patterns {
            let _ = writeln!(&mut rendered, "  - {pattern}");
        }
    }

    rendered
}

fn render_grouped_sql_runtime_ownership_violations(
    violations: &[SqlRuntimeOwnershipViolation],
) -> String {
    let mut grouped: BTreeMap<&str, Vec<&str>> = BTreeMap::new();

    for violation in violations {
        grouped
            .entry(violation.file.as_str())
            .or_default()
            .push(violation.pattern);
    }

    let mut rendered = String::new();
    for (file, patterns) in grouped {
        let _ = writeln!(&mut rendered, "{file}:");
        for pattern in patterns {
            let _ = writeln!(&mut rendered, "  - {pattern}");
        }
    }

    rendered
}

fn top_level_module_set() -> HashSet<String> {
    let lib_source = fs::read_to_string(lib_path()).expect("src/lib.rs should be readable");
    parse_top_level_modules(&lib_source).into_iter().collect()
}

fn services_child_modules() -> BTreeSet<String> {
    let Some(relative_path) = root_module_entry_relative_path("services") else {
        return BTreeSet::new();
    };
    let source = read_engine_source(&relative_path);
    parse_declared_modules(&strip_test_code(&source))
        .into_iter()
        .collect()
}

fn current_services_direct_child_import_violations() -> Vec<ImportPathViolation> {
    let module_set = top_level_module_set();
    let service_children = services_child_modules();
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        let current_module_path = module_path_for_file(&relative_path);
        if current_module_path
            .first()
            .is_some_and(|root| root == "services")
        {
            continue;
        }

        for imported_path in
            collect_module_paths_from_source(&source, &current_module_path, &module_set)
        {
            if imported_path.first().is_none_or(|root| root != "services") {
                continue;
            }

            let imported_child = imported_path.get(1);
            let imports_declared_service_child =
                imported_child.is_some_and(|child| service_children.contains(child));
            let stays_within_direct_child_surface = imported_path.len() <= 3;
            if imports_declared_service_child && stays_within_direct_child_surface {
                continue;
            }

            violations.insert(ImportPathViolation {
                importer_file: relative_path.clone(),
                imported_path: imported_path.join("::"),
            });
        }
    }

    violations.into_iter().collect()
}

fn current_services_external_dependency_violations() -> Vec<ImportPathViolation> {
    let module_set = top_level_module_set();
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        let current_module_path = module_path_for_file(&relative_path);
        if current_module_path
            .first()
            .is_none_or(|root| root != "services")
        {
            continue;
        }

        for imported_path in
            collect_module_paths_from_source(&source, &current_module_path, &module_set)
        {
            let Some(imported_root) = imported_path.first() else {
                continue;
            };
            if imported_root == "services" {
                continue;
            }
            if ALLOWED_SERVICE_FOUNDATION_ROOTS.contains(&imported_root.as_str()) {
                continue;
            }

            violations.insert(ImportPathViolation {
                importer_file: relative_path.clone(),
                imported_path: imported_path.join("::"),
            });
        }
    }

    violations.into_iter().collect()
}

fn current_services_sibling_dependency_violations() -> Vec<ImportPathViolation> {
    let module_set = top_level_module_set();
    let service_children = services_child_modules();
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        let current_module_path = module_path_for_file(&relative_path);
        if current_module_path
            .first()
            .is_none_or(|root| root != "services")
        {
            continue;
        }
        let Some(current_child) = current_module_path.get(1) else {
            continue;
        };

        for imported_path in
            collect_module_paths_from_source(&source, &current_module_path, &module_set)
        {
            if imported_path.first().is_none_or(|root| root != "services") {
                continue;
            }
            let Some(imported_child) = imported_path.get(1) else {
                continue;
            };
            if !service_children.contains(imported_child) {
                continue;
            }
            if imported_child == current_child {
                continue;
            }

            violations.insert(ImportPathViolation {
                importer_file: relative_path.clone(),
                imported_path: imported_path.join("::"),
            });
        }
    }

    violations.into_iter().collect()
}

fn is_engine_owned_persistence_path(relative_path: &str) -> bool {
    let in_scope_owner_root = relative_path.starts_with("live_state/")
        || relative_path.starts_with("canonical/")
        || relative_path.starts_with("binary_cas/")
        || relative_path.starts_with("session/branch_ops/");
    let is_allowed_adapter_surface = relative_path.ends_with("/store.rs")
        || relative_path.ends_with("/store_sql.rs")
        || relative_path.ends_with("/storage.rs");

    in_scope_owner_root && !is_allowed_adapter_surface
}

fn current_engine_owned_persistence_raw_sql_execution_violations() -> Vec<RawSqlExecutionViolation>
{
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        if !is_engine_owned_persistence_path(&relative_path) {
            continue;
        }

        let masked_source = mask_rust_source(&source);
        for pattern in [".execute("] {
            if masked_source.contains(pattern) {
                violations.insert(RawSqlExecutionViolation {
                    file: relative_path.clone(),
                    pattern,
                });
            }
        }
    }

    violations.into_iter().collect()
}

fn contains_identifier(source: &str, identifier: &str) -> bool {
    let bytes = source.as_bytes();
    let needle = identifier.as_bytes();
    let mut index = 0usize;

    while index + needle.len() <= bytes.len() {
        if &bytes[index..index + needle.len()] != needle {
            index += 1;
            continue;
        }

        let boundary_before = index == 0 || !is_ident_continue(bytes[index - 1]);
        let boundary_after =
            index + needle.len() == bytes.len() || !is_ident_continue(bytes[index + needle.len()]);
        if boundary_before && boundary_after {
            return true;
        }

        index += 1;
    }

    false
}

fn current_engine_owned_persistence_raw_backend_type_violations() -> Vec<RawBackendTypeViolation> {
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        if !is_engine_owned_persistence_path(&relative_path) {
            continue;
        }

        let masked_source = mask_rust_source(&source);
        for type_name in [
            "Backend",
            "BackendReadTransaction",
            "BackendWriteTransaction",
        ] {
            if contains_identifier(&masked_source, type_name) {
                violations.insert(RawBackendTypeViolation {
                    file: relative_path.clone(),
                    type_name,
                });
            }
        }
    }

    violations.into_iter().collect()
}

fn is_owner_persistence_root_path(relative_path: &str) -> bool {
    relative_path.starts_with("live_state/")
        || relative_path.starts_with("canonical/")
        || relative_path.starts_with("binary_cas/")
}

fn is_owner_sql_adapter_path(relative_path: &str) -> bool {
    relative_path.ends_with("/store_sql.rs") || relative_path.ends_with("/storage.rs")
}

fn current_owner_persistence_backend_root_dependency_violations() -> Vec<ImportPathViolation> {
    let module_set = top_level_module_set();
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        if !is_owner_persistence_root_path(&relative_path)
            || is_owner_sql_adapter_path(&relative_path)
        {
            continue;
        }

        let masked_source = mask_rust_source(&source);
        if !contains_identifier(&masked_source, "Backend")
            && !contains_identifier(&masked_source, "BackendReadTransaction")
            && !contains_identifier(&masked_source, "BackendWriteTransaction")
        {
            continue;
        }

        let current_module_path = module_path_for_file(&relative_path);
        for imported_path in
            collect_module_paths_from_source(&source, &current_module_path, &module_set)
        {
            if imported_path.first().is_none_or(|root| root != "backend") {
                continue;
            }

            violations.insert(ImportPathViolation {
                importer_file: relative_path.clone(),
                imported_path: imported_path.join("::"),
            });
        }
    }

    violations.into_iter().collect()
}

fn current_backend_import_outside_storage_violations() -> Vec<ImportPathViolation> {
    let module_set = top_level_module_set();
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        if relative_path.starts_with("backend/") || relative_path.starts_with("storage/") {
            continue;
        }

        let current_module_path = module_path_for_file(&relative_path);
        for imported_path in
            collect_module_paths_from_source(&source, &current_module_path, &module_set)
        {
            if imported_path.first().is_none_or(|root| root != "backend") {
                continue;
            }

            violations.insert(ImportPathViolation {
                importer_file: relative_path.clone(),
                imported_path: imported_path.join("::"),
            });
        }
    }

    violations.into_iter().collect()
}

fn current_store_sql_import_boundary_violations() -> Vec<ImportPathViolation> {
    let module_set = top_level_module_set();
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        let current_module_path = module_path_for_file(&relative_path);
        let current_root = current_module_path.first().map(String::as_str);

        for imported_path in
            collect_module_paths_from_source(&source, &current_module_path, &module_set)
        {
            if imported_path
                .get(1)
                .is_none_or(|segment| segment != "store_sql")
            {
                continue;
            }

            let owner_root = imported_path.first().map(String::as_str);
            if current_root == owner_root {
                continue;
            }

            violations.insert(ImportPathViolation {
                importer_file: relative_path.clone(),
                imported_path: imported_path.join("::"),
            });
        }
    }

    violations.into_iter().collect()
}

fn current_owner_persistence_transaction_lifecycle_violations() -> Vec<TransactionLifecycleViolation>
{
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        if !is_owner_persistence_root_path(&relative_path)
            || is_owner_sql_adapter_path(&relative_path)
        {
            continue;
        }

        let masked_source = mask_rust_source(&source);
        for pattern in [
            ".begin_read_transaction(",
            "begin_write_transaction(",
            ".commit().await",
            ".rollback().await",
        ] {
            if masked_source.contains(pattern) {
                violations.insert(TransactionLifecycleViolation {
                    file: relative_path.clone(),
                    pattern,
                });
            }
        }
    }

    violations.into_iter().collect()
}

fn is_owner_local_storage_path(relative_path: &str) -> bool {
    relative_path.ends_with("/storage.rs")
}

fn is_allowed_raw_execute_boundary_path(relative_path: &str) -> bool {
    is_owner_local_storage_path(relative_path)
        || relative_path.starts_with("sql/")
        || relative_path.starts_with("execution/")
        || relative_path.starts_with("backend/")
        || relative_path == "transaction/backend.rs"
        || relative_path == "transaction/buffered_write_transaction.rs"
        || relative_path == "transaction/live_state_write_transaction.rs"
}

fn current_raw_execute_outside_owner_storage_or_public_sql_boundary_violations()
-> Vec<RawSqlExecutionViolation> {
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        if is_allowed_raw_execute_boundary_path(&relative_path) {
            continue;
        }

        let masked_source = mask_rust_source(&source);
        for pattern in [
            "backend.execute(",
            "transaction.execute(",
            "executor.execute(",
            "self.base.execute(",
            "self.backend.execute(",
            "self.backend_transaction.execute(",
        ] {
            if masked_source.contains(pattern) {
                violations.insert(RawSqlExecutionViolation {
                    file: relative_path.clone(),
                    pattern,
                });
            }
        }
    }

    violations.into_iter().collect()
}

fn is_orchestration_runtime_path(relative_path: &str) -> bool {
    relative_path.starts_with("api/")
        || relative_path.starts_with("init/")
        || relative_path.starts_with("session/")
        || relative_path.starts_with("transaction/")
}

fn current_scattered_internal_metadata_crud_outside_owner_storage_violations()
-> Vec<RawSqlExecutionViolation> {
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        if !is_orchestration_runtime_path(&relative_path)
            || is_owner_local_storage_path(&relative_path)
        {
            continue;
        }

        let masked_source = mask_rust_source(&source);
        for pattern in [
            "SELECT value FROM lix_internal_workspace_metadata",
            "INSERT INTO lix_internal_workspace_metadata",
            "CREATE TABLE lix_internal_workspace_metadata",
            "FROM lix_internal_commit_idempotency",
            "INSERT INTO lix_internal_commit_idempotency",
            "CREATE TABLE IF NOT EXISTS lix_internal_commit_idempotency",
            "FROM lix_internal_undo_redo_operation",
            "INSERT INTO lix_internal_undo_redo_operation",
            "CREATE TABLE IF NOT EXISTS lix_internal_undo_redo_operation",
        ] {
            if masked_source.contains(pattern) {
                violations.insert(RawSqlExecutionViolation {
                    file: relative_path.clone(),
                    pattern,
                });
            }
        }
    }

    violations.into_iter().collect()
}

fn current_owner_storage_public_sql_shaped_api_violations() -> Vec<RawSqlExecutionViolation> {
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        if !is_owner_local_storage_path(&relative_path) {
            continue;
        }

        let masked_source = mask_rust_source(&source);
        for pattern in [
            "pub(crate) async fn execute_query_with_",
            "pub(crate) async fn execute_ddl_batch_with_",
            "pub(crate) async fn add_column_if_missing_with_",
            "pub(crate) async fn begin_write_transaction",
            "pub(crate) fn executor_from_transaction",
        ] {
            if masked_source.contains(pattern) {
                violations.insert(RawSqlExecutionViolation {
                    file: relative_path.clone(),
                    pattern,
                });
            }
        }
    }

    violations.into_iter().collect()
}

fn current_shared_persistence_root_files() -> Vec<String> {
    production_source_files()
        .into_iter()
        .filter_map(|(relative_path, _)| {
            relative_path
                .starts_with("persistence/")
                .then_some(relative_path)
        })
        .collect()
}

fn is_sql2_runtime_owner_path(relative_path: &str) -> bool {
    relative_path == "sql2/runtime.rs"
}

fn current_sql2_datafusion_physical_execution_owner_violations() -> Vec<SqlRuntimeOwnershipViolation>
{
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        if !relative_path.starts_with("sql2/") || is_sql2_runtime_owner_path(&relative_path) {
            continue;
        }

        let stripped = strip_test_code(&source);
        let masked_source = mask_rust_source(&stripped);
        for pattern in [
            ".collect().await",
            ".create_physical_plan().await",
            ".execute(partition,",
            "execute_input_stream(",
        ] {
            if masked_source.contains(pattern) {
                violations.insert(SqlRuntimeOwnershipViolation {
                    file: relative_path.clone(),
                    pattern,
                });
            }
        }
    }

    violations.into_iter().collect()
}

fn current_sql2_data_sink_exec_violations() -> Vec<SqlRuntimeOwnershipViolation> {
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        if !relative_path.starts_with("sql2/") {
            continue;
        }

        let stripped = strip_test_code(&source);
        let masked_source = mask_rust_source(&stripped);
        for pattern in ["DataSinkExec", "DataSinkExec::new("] {
            if masked_source.contains(pattern) {
                violations.insert(SqlRuntimeOwnershipViolation {
                    file: relative_path.clone(),
                    pattern,
                });
            }
        }
    }

    violations.into_iter().collect()
}

fn current_session_transaction_durable_commit_boundary_violations() -> Vec<RawSqlExecutionViolation>
{
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        if !(relative_path.starts_with("session/") || relative_path.starts_with("transaction/")) {
            continue;
        }

        let masked_source = mask_rust_source(&source);
        let lines: Vec<&str> = masked_source.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if !line.contains(".commit_write_set(") {
                continue;
            }
            let start = index.saturating_sub(5);
            let window = lines[start..=index].join("\n");
            if !window.contains("commit_at_boundary(Some(")
                && !window.contains("commit_at_boundary(commit_boundary.as_ref()")
            {
                violations.insert(RawSqlExecutionViolation {
                    file: relative_path.clone(),
                    pattern: ".commit_write_set(",
                });
            }
        }
    }

    violations.into_iter().collect()
}

fn current_schema_catalog_dependency_violations() -> Vec<ImportPathViolation> {
    let module_set = top_level_module_set();
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        if !relative_path.starts_with("schema/") {
            continue;
        }

        let current_module_path = module_path_for_file(&relative_path);
        for imported_path in
            collect_module_paths_from_source(&source, &current_module_path, &module_set)
        {
            if imported_path
                .first()
                .is_some_and(|root| root == "schema_catalog")
            {
                violations.insert(ImportPathViolation {
                    importer_file: relative_path.clone(),
                    imported_path: imported_path.join("::"),
                });
            }
        }
    }

    violations.into_iter().collect()
}

fn current_schema_invalid_param_violations() -> Vec<RawSqlExecutionViolation> {
    let mut violations = BTreeSet::new();

    for (relative_path, source) in production_source_files() {
        if !relative_path.starts_with("schema/") {
            continue;
        }

        let masked_source = mask_rust_source(&source);
        for pattern in ["CODE_INVALID_PARAM", "LIX_INVALID_PARAM"] {
            if masked_source.contains(pattern) {
                violations.insert(RawSqlExecutionViolation {
                    file: relative_path.clone(),
                    pattern,
                });
            }
        }
    }

    violations.into_iter().collect()
}

#[test]
fn rust_modules_do_not_use_path_attributes() {
    let violations = source_test_and_bench_rust_files()
        .into_iter()
        .flat_map(|(relative_path, source)| {
            source
                .lines()
                .enumerate()
                .filter(|(_, line)| line.trim_start().starts_with("#[path"))
                .map(move |(index, line)| format!("{relative_path}:{}: {}", index + 1, line.trim()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "Rust modules must not use path attributes; move modules under a normal owner instead.\n\n{}",
        violations.join("\n"),
    );
}

#[test]
fn sealed_owner_violations_are_empty() {
    let violations = current_sealed_owner_violations();

    assert!(
        violations.is_empty(),
        "sealed-owner violations are present.\n\nCurrent violations:\n{}",
        render_grouped_sealed_owner_violations(&violations),
    );
}

#[test]
fn forbidden_dependency_rules_have_no_current_violations() {
    let graph = analyze_engine_dependency_graph();
    let graph_modules = module_set(&graph);
    for module in TARGET_CORE_MODULES {
        assert!(
            graph_modules.contains(*module),
            "target core graph should include `{module}`",
        );
    }

    let forbidden_lookup = forbidden_dependency_lookup();
    let violations = actual_architecture_violations(&graph, &forbidden_lookup);

    assert!(
        violations.is_empty(),
        "forbidden owner-root dependencies are present.\n\nTarget core graph:\n{}\nCurrent violations:\n{}",
        render_target_core_graph(&target_core_graph(&graph)),
        render_forbidden_dependency_violations(&violations, &forbidden_lookup),
    );
}

#[test]
fn target_core_owner_graph_has_no_cycles() {
    let graph = analyze_engine_dependency_graph();
    let core_graph = target_core_graph(&graph);
    let cycles = owner_root_cycles(&core_graph);

    assert!(
        cycles.is_empty(),
        "target core owner-root graph has cycles.\n\nTarget core graph:\n{}\nCycles:\n{}",
        render_target_core_graph(&core_graph),
        render_owner_root_cycles(&cycles),
    );
}

#[test]
fn schema_domain_does_not_depend_on_schema_catalog() {
    let violations = current_schema_catalog_dependency_violations();

    assert!(
        violations.is_empty(),
        "`schema/*` owns schema-document semantics and must not depend on `schema_catalog/*`; transaction/public boundary adapters should compose the two domains.\n\nCurrent violations:\n{}",
        render_grouped_import_path_violations(&violations),
    );
}

#[test]
fn schema_domain_does_not_emit_public_invalid_param() {
    let violations = current_schema_invalid_param_violations();

    assert!(
        violations.is_empty(),
        "`schema/*` must return schema-domain errors only. Public `INVALID_PARAM` classification belongs at transaction/API/SQL public boundaries.\n\nCurrent violations:\n{}",
        render_grouped_raw_sql_execution_violations(&violations),
    );
}

// `services` intentionally does not get a giant root facade. Outside code may
// depend on `services::child::*`, but not on deeper implementation paths.
#[test]
fn services_imports_are_limited_to_direct_child_namespaces() {
    if !top_level_module_set().contains("services") {
        return;
    }
    let violations = current_services_direct_child_import_violations();

    assert!(
        violations.is_empty(),
        "outside `services/*`, imports into `services` must target a direct child capability namespace only.\n\nCurrent violations:\n{}",
        render_grouped_import_path_violations(&violations),
    );
}

// Leaf `services/*` modules are standalone capabilities. They may depend on
// neutral foundations like `common`, but not on engine
// composition, semantic owners, or other top-level roots.
#[test]
fn services_has_no_external_root_dependencies() {
    if !top_level_module_set().contains("services") {
        return;
    }
    let violations = current_services_external_dependency_violations();

    assert!(
        violations.is_empty(),
        "`services/*` leaf modules may only import neutral foundation roots (`common`) outside `services`.\n\nCurrent violations:\n{}",
        render_grouped_import_path_violations(&violations),
    );
}

// Direct child `services/*` modules are also standalone relative to each
// other. If two services need shared pieces, that code should move to neutral
// ground or the capabilities should be merged.
#[test]
fn services_direct_children_do_not_import_sibling_services() {
    if !top_level_module_set().contains("services") {
        return;
    }
    let violations = current_services_sibling_dependency_violations();

    assert!(
        violations.is_empty(),
        "direct child `services/*` modules must not import sibling service namespaces.\n\nCurrent violations:\n{}",
        render_grouped_import_path_violations(&violations),
    );
}

// Engine-owned persistence modules should execute through owner-local adapters
// rather than calling raw backend SQL directly.
#[test]
fn engine_owned_persistence_modules_do_not_execute_raw_sql_directly() {
    let violations = current_engine_owned_persistence_raw_sql_execution_violations();

    assert!(
        violations.is_empty(),
        "engine-owned persistence modules must not execute raw SQL directly outside owner-local adapter files.\n\nCurrent violations:\n{}",
        render_grouped_raw_sql_execution_violations(&violations),
    );
}

// Engine-owned persistence modules should depend on owner-local store
// interfaces rather than raw backend handle types.
#[test]
fn engine_owned_persistence_modules_do_not_import_raw_backend_types() {
    let violations = current_engine_owned_persistence_raw_backend_type_violations();

    assert!(
        violations.is_empty(),
        "engine-owned persistence modules must not depend on raw backend types outside owner-local adapter files.\n\nCurrent violations:\n{}",
        render_grouped_raw_backend_type_violations(&violations),
    );
}

// Owner persistence code should speak in owner-local store terms, not import
// lower `backend/*` helpers directly outside SQL adapter files.
#[test]
fn owner_persistence_modules_do_not_depend_on_backend_root_outside_sql_adapters() {
    let violations = current_owner_persistence_backend_root_dependency_violations();

    assert!(
        violations.is_empty(),
        "owner persistence modules must not depend on `backend/*` outside owner-local SQL adapter files.\n\nCurrent violations:\n{}",
        render_grouped_import_path_violations(&violations),
    );
}

#[test]
fn backend_imports_are_limited_to_storage_boundary() {
    let violations = current_backend_import_outside_storage_violations();

    assert!(
        violations.is_empty(),
        "`backend/*` may only be imported by `storage/*`; other engine modules must depend on storage-facing APIs.\n\nCurrent violations:\n{}",
        render_grouped_import_path_violations(&violations),
    );
}

// SQL-backed store adapters are owner internals. Other roots may import the
// owner-facing store interfaces, but not the `store_sql` implementations.
#[test]
fn store_sql_modules_are_not_imported_outside_their_owning_root() {
    let violations = current_store_sql_import_boundary_violations();

    assert!(
        violations.is_empty(),
        "`store_sql` modules must not be imported outside their owning root.\n\nCurrent violations:\n{}",
        render_grouped_import_path_violations(&violations),
    );
}

// Owner persistence modules may perform work inside a caller-owned transaction,
// but must not decide when transactions begin or end. Transaction lifecycle
// policy belongs to session/runtime, while owner-local SQL adapters may still
// contain low-level backend transaction calls during the MVP.
#[test]
fn owner_persistence_modules_do_not_own_transaction_lifecycle() {
    let violations = current_owner_persistence_transaction_lifecycle_violations();

    assert!(
        violations.is_empty(),
        "owner persistence modules must not begin, commit, or roll back transactions outside owner-local SQL adapter files.\n\nCurrent violations:\n{}",
        render_grouped_transaction_lifecycle_violations(&violations),
    );
}

#[test]
fn raw_backend_execute_is_only_used_in_owner_storage_or_public_sql_layers() {
    let violations = current_raw_execute_outside_owner_storage_or_public_sql_boundary_violations();

    assert!(
        violations.is_empty(),
        "raw backend / transaction SQL execution may only appear in owner-local `storage.rs`, `sql/*`, `execution/*`, or backend glue.\n\nCurrent violations:\n{}",
        render_grouped_raw_sql_execution_violations(&violations),
    );
}

#[test]
fn internal_metadata_crud_is_centralized_in_owner_storage() {
    let violations = current_scattered_internal_metadata_crud_outside_owner_storage_violations();

    assert!(
        violations.is_empty(),
        "internal metadata CRUD for workspace selectors, commit idempotency, and undo/redo log should live in owner-local `storage.rs` seams, not scattered through `api/*`, `init/*`, `session/*`, or `transaction/*`.\n\nCurrent violations:\n{}",
        render_grouped_raw_sql_execution_violations(&violations),
    );
}

#[test]
fn owner_storage_modules_do_not_expose_public_sql_shaped_helpers() {
    let violations = current_owner_storage_public_sql_shaped_api_violations();

    assert!(
        violations.is_empty(),
        "owner-local `storage.rs` seams should expose operation-shaped APIs rather than public SQL-shaped helpers.\n\nCurrent violations:\n{}",
        render_grouped_raw_sql_execution_violations(&violations),
    );
}

#[test]
fn sql2_physical_execution_is_owned_by_runtime_module() {
    let violations = current_sql2_datafusion_physical_execution_owner_violations();

    assert!(
        violations.is_empty(),
        "DataFusion physical execution must be centralized in `sql2/runtime.rs`; read/write SQL paths should not collect DataFrames or execute physical plans through side doors.\n\nCurrent violations:\n{}",
        render_grouped_sql_runtime_ownership_violations(&violations),
    );
}

#[test]
fn sql2_write_providers_do_not_delegate_dml_execution_to_datafusion_sinks() {
    let violations = current_sql2_data_sink_exec_violations();

    assert!(
        violations.is_empty(),
        "SQL2 write providers must not use DataFusion `DataSinkExec`; DML source batches should be collected through the SQL runtime and staged by transaction-owned write code.\n\nCurrent violations:\n{}",
        render_grouped_sql_runtime_ownership_violations(&violations),
    );
}

#[test]
fn sql2_public_boundary_does_not_reintroduce_stringly_validation() {
    let mut violations = Vec::new();

    for (relative_path, source) in production_source_files() {
        if !relative_path.starts_with("sql2/") {
            continue;
        }
        let stripped = strip_test_code(&source);
        let masked_source = mask_rust_source(&stripped);

        for pattern in [
            "PublicPredicateSpec {",
            "public_input::expect_text_column(\"",
            "public_input::expect_bool_column(\"",
            "public_input::expect_json_object_metadata(\"",
            "public_input::expect_json_text(\"",
            "public_input::expect_file_path_public(\"",
            "public_input::expect_directory_path_public(\"",
            "public_input::expect_entity_pk_public(\"",
            "public_input::expect_non_blob_public_id(\"",
            "require_write(\"",
            "routed_surface(",
            "operation: &str",
            "table: &str",
        ] {
            if masked_source.contains(pattern) {
                violations.push(format!("{relative_path}: {pattern}"));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "SQL2 public boundary validation must flow through typed PublicBoundaryContext/PublicSurface helpers, not raw operation/table strings.\n\nCurrent violations:\n{}",
        violations.join("\n"),
    );
}

#[test]
fn sql2_read_session_does_not_register_write_surfaces() {
    let relative = "sql2/session.rs";
    let source = read_engine_source(relative);
    let read_session = source_between(
        relative,
        &source,
        "pub(crate) async fn build_read_session",
        "pub(crate) async fn build_transaction_read_session",
    );

    assert_source_contains_all(relative, read_session, &["providers::register_read"]);
    assert_source_contains_none(
        relative,
        read_session,
        &["SqlWriteContext::new", "providers::register_write"],
    );

    let relative = "sql2/providers/mod.rs";
    let source = read_engine_source(relative);
    assert_source_contains_all(
        relative,
        &source,
        &[
            "mod change;",
            "mod directory;",
            "mod directory_history;",
            "mod entity;",
            "mod entity_history;",
            "mod file;",
            "mod file_history;",
            "mod history;",
            "mod lix_state;",
            "mod branch;",
        ],
    );
    assert_source_contains_none(
        relative,
        &source,
        &["pub mod", "pub(crate) mod", "pub(super) mod", "pub(in "],
    );
    let read_registration = source_between(
        relative,
        &source,
        "pub(crate) async fn register_read",
        "pub(crate) async fn register_write",
    );
    assert_source_contains_all(
        relative,
        read_registration,
        &[
            "PublicCatalog::from_visible_schemas",
            "catalog.surfaces()",
            "PublicSurfaceKind::LixState",
            "PublicSurfaceKind::LixStateByBranch",
            "PublicSurfaceKind::Branch",
            "PublicSurfaceKind::Change",
            "PublicSurfaceKind::History",
            "PublicSurfaceKind::File",
            "PublicSurfaceKind::FileByBranch",
            "PublicSurfaceKind::FileHistory",
            "PublicSurfaceKind::Directory",
            "PublicSurfaceKind::DirectoryByBranch",
            "PublicSurfaceKind::DirectoryHistory",
            "lix_state::register_lix_state_active_provider",
            "lix_state::register_lix_state_by_branch_provider",
            "branch::register_lix_branch_read_provider",
            "change::register_lix_change_read_provider",
            "history::register_history_provider",
            "file_history::register_lix_file_history_surface",
            "directory_history::register_lix_directory_history_surface",
            "directory::register_lix_directory_active_provider",
            "directory::register_lix_directory_by_branch_provider",
            "file::register_lix_file_active_provider",
            "file::register_lix_file_by_branch_provider",
            "entity::register_entity_providers",
        ],
    );
    assert_source_contains_none(
        relative,
        read_registration,
        &[
            "register_lix_state_write_providers",
            "register_lix_branch_write_provider",
            "register_lix_directory_write_providers",
            "register_lix_file_write_providers",
            "register_entity_write_providers",
            "register_lix_state_providers",
            "register_lix_branch_provider",
            "register_lix_change_provider",
            "register_history_providers",
            "register_lix_file_history_provider",
            "register_lix_directory_history_provider",
            "register_lix_directory_providers",
            "register_lix_file_providers",
        ],
    );
}

#[test]
fn sql2_write_session_registers_writable_transaction_surfaces() {
    let relative = "sql2/session.rs";
    let source = read_engine_source(relative);
    let write_session = source_between(
        relative,
        &source,
        "pub(crate) async fn build_write_session",
        "fn new_sql_session_context",
    );

    assert_source_contains_all(
        relative,
        write_session,
        &["SqlWriteContext::new", "providers::register_write"],
    );
    assert_source_contains_none(relative, write_session, &["providers::register_read"]);

    let relative = "sql2/providers/mod.rs";
    let source = read_engine_source(relative);
    assert_source_contains_none(
        relative,
        &source,
        &["pub mod", "pub(crate) mod", "pub(super) mod", "pub(in "],
    );
    let write_registration = source_between(
        relative,
        &source,
        "pub(crate) async fn register_write",
        "#[cfg(test)]",
    );
    assert_source_contains_all(
        relative,
        write_registration,
        &[
            "PublicCatalog::from_visible_schemas",
            "catalog.surfaces()",
            "PublicSurfaceKind::LixState",
            "PublicSurfaceKind::LixStateByBranch",
            "PublicSurfaceKind::Branch",
            "PublicSurfaceKind::File",
            "PublicSurfaceKind::FileByBranch",
            "PublicSurfaceKind::Directory",
            "PublicSurfaceKind::DirectoryByBranch",
            "lix_state::register_lix_state_active_write_provider",
            "lix_state::register_lix_state_by_branch_write_provider",
            "branch::register_write_provider",
            "file::register_active_write_provider",
            "file::register_by_branch_write_provider",
            "directory::register_active_write_provider",
            "directory::register_by_branch_write_provider",
            "entity::register_entity_write_providers",
        ],
    );
    assert_source_contains_none(
        relative,
        write_registration,
        &[
            "ctx.live_state()",
            "ctx.branch_ref()",
            "register_lix_state_providers",
            "register_lix_branch_provider",
            "register_lix_change_provider",
            "register_history_providers",
            "register_lix_file_history_provider",
            "register_lix_directory_history_provider",
            "register_lix_directory_providers",
            "register_lix_file_providers",
            "register_entity_providers",
            "register_lix_state_write_providers",
            "register_lix_branch_write_provider",
            "register_lix_branch_write_surface",
            "register_lix_directory_active_write_provider",
            "register_lix_directory_by_branch_write_provider",
            "register_lix_directory_write_providers",
            "register_lix_file_active_write_provider",
            "register_lix_file_by_branch_write_provider",
            "register_lix_file_write_providers",
        ],
    );
}

#[test]
fn session_transaction_durable_commits_go_through_commit_boundary() {
    let violations = current_session_transaction_durable_commit_boundary_violations();

    assert!(
        violations.is_empty(),
        "session/transaction commits must use `commit_at_boundary` so close cannot race the final pre-commit check. Low-level storage, init, and engine maintenance writes are the MVP escape hatches.\n\nCurrent violations:\n{}",
        render_grouped_raw_sql_execution_violations(&violations),
    );
}

#[test]
fn sql2_entity_provider_registration_is_catalog_driven() {
    let relative = "sql2/providers/entity.rs";
    let source = read_engine_source(relative);
    let non_test_source = strip_test_code(&source);
    let read_registration = source_between(
        relative,
        &source,
        "pub(crate) async fn register_entity_providers",
        "pub(crate) async fn register_entity_write_providers",
    );
    let write_registration = source_between(
        relative,
        &source,
        "pub(crate) async fn register_entity_write_providers",
        "fn catalog_entity_spec",
    );

    assert_source_contains_all(
        relative,
        read_registration,
        &[
            "catalog.surfaces()",
            "PublicSurfaceKind::EntityBase",
            "PublicSurfaceKind::EntityByBranch",
            "PublicSurfaceKind::EntityHistory",
        ],
    );
    assert_source_contains_all(
        relative,
        write_registration,
        &[
            "catalog.surfaces()",
            "PublicSurfaceKind::EntityBase",
            "PublicSurfaceKind::EntityByBranch",
        ],
    );
    assert_source_contains_none(
        relative,
        read_registration,
        &[
            "schema_definitions",
            "derive_entity_surface_spec_from_schema",
            "schema_exposed_as_entity_surface",
            "schema_exposed_as_entity_history_surface",
        ],
    );
    assert_source_contains_none(
        relative,
        write_registration,
        &[
            "schema_definitions",
            "derive_entity_surface_spec_from_schema",
            "schema_exposed_as_entity_surface",
            "schema_exposed_as_entity_history_surface",
        ],
    );
    assert_source_contains_none(
        relative,
        &non_test_source,
        &[
            "schema_exposed_as_entity_surface",
            "schema_exposed_as_entity_history_surface",
            "derive_entity_surface_spec_from_schema(",
        ],
    );
}

#[test]
fn sql2_session_context_keeps_wasm_safe_physical_plan_defaults() {
    let relative = "sql2/session.rs";
    let source = read_engine_source(relative);
    let session_context = source_between(relative, &source, "fn new_sql_session_context", "\n}");

    assert_source_contains_all(
        relative,
        session_context,
        &[
            ".with_target_partitions(1)",
            "\"datafusion.optimizer.repartition_aggregations\", false",
            "\"datafusion.optimizer.repartition_joins\", false",
            "\"datafusion.optimizer.repartition_sorts\", false",
            "\"datafusion.optimizer.repartition_windows\", false",
            "\"datafusion.optimizer.repartition_file_scans\", false",
            "\"datafusion.optimizer.enable_round_robin_repartition\", false",
        ],
    );
}

#[test]
fn shared_persistence_root_is_empty_or_absent() {
    let remaining_files = current_shared_persistence_root_files();

    assert!(
        remaining_files.is_empty(),
        "the shared `persistence/*` root is transitional and should become empty or disappear as owner-local `storage.rs` seams take over.\n\nCurrent files:\n{}",
        remaining_files
            .into_iter()
            .map(|file| format!("- {file}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}
