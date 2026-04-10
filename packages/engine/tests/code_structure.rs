#![allow(dead_code)]

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
            "runtime",
            "session",
            "sql",
        ],
    },
    ForbiddenDependencyRule {
        from_scope: "backend",
        reason: "backend is a lower persistence owner; shared SQL helpers must move to neutral foundation and runtime must stay above it",
        forbidden_scopes: &["runtime", "sql"],
    },
    ForbiddenDependencyRule {
        from_scope: "contracts",
        reason: "contracts is a downward-only seam and must stay neutral relative to engine owners",
        forbidden_scopes: &[
            "backend",
            "canonical",
            "execution",
            "api",
            "live_state",
            "runtime",
            "session",
            "sql",
        ],
    },
    ForbiddenDependencyRule {
        from_scope: "runtime",
        reason: "runtime is a sidecar and must not reacquire execution, root-shell, workflow, or compiler owners; sealed live_state root APIs are allowed",
        forbidden_scopes: &["execution", "api", "session", "sql"],
    },
    ForbiddenDependencyRule {
        from_scope: "live_state",
        reason: "live_state is the generic projection engine and must not reacquire runtime sidecars or write orchestration owners",
        forbidden_scopes: &["execution", "runtime"],
    },
    ForbiddenDependencyRule {
        from_scope: "sql",
        reason: "sql is the compiler and should not depend on backend, storage, execution, workflow, or session/runtime owners directly; sealed owner-root query-contract APIs plus acyclic internal-relation inventory roots are allowed",
        forbidden_scopes: &["backend", "execution", "runtime", "session"],
    },
    ForbiddenDependencyRule {
        from_scope: "execution",
        reason: "execution should consume contracts, live_state, and backend; compiler access must come through prepared artifacts rather than direct owner imports",
        forbidden_scopes: &[
            "canonical",
            "api",
            "init",
            "runtime",
            "session",
            "sql",
        ],
    },
    ForbiddenDependencyRule {
        from_scope: "session",
        reason: "session owns orchestration and workflow code, but should not couple itself to the root API shell",
        forbidden_scopes: &["api"],
    },
];

const TARGET_CORE_MODULES: &[&str] = &[
    "backend",
    "canonical",
    "contracts",
    "execution",
    "live_state",
    "runtime",
    "session",
    "sql",
];

const SEALED_OWNER_SNAPSHOT_PATH: &str = "tests/sealed_owner_violations.txt";

#[derive(Debug, Clone, PartialEq, Eq)]
struct EngineDependencyGraph {
    module_source: String,
    modules_analyzed: Vec<String>,
    edges: Vec<DependencyEdge>,
    strongly_connected_components: Vec<StronglyConnectedComponent>,
    adjacency_by_module: BTreeMap<String, ModuleAdjacency>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DependencyEdge {
    from: String,
    to: String,
    via_files: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StronglyConnectedComponent {
    modules: Vec<String>,
    internal_edges: Vec<DependencyEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ModuleAdjacency {
    incoming: Vec<String>,
    outgoing: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SealedOwnerViolation {
    importer_file: String,
    imported_path: String,
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

fn assert_source_contains_in_order(relative: &str, source: &str, needles: &[&str]) {
    let mut previous: Option<(&str, usize)> = None;
    for needle in needles {
        let index = source
            .find(needle)
            .unwrap_or_else(|| panic!("{relative} should contain `{needle}`"));
        if let Some((previous_needle, previous_index)) = previous {
            assert!(
                previous_index < index,
                "{relative} should keep `{previous_needle}` before `{needle}`",
            );
        }
        previous = Some((needle, index));
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
            let source =
                fs::read_to_string(&absolute_path).expect("module source file should be readable");
            let relative_path = absolute_path
                .strip_prefix(src_root())
                .expect("module source file should be inside src/")
                .to_string_lossy()
                .replace('\\', "/");
            let current_module_path = module_path_for_file(&relative_path);
            let dependencies =
                collect_dependencies_from_source(&source, &current_module_path, &module_set);

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

    let strongly_connected_components = tarjan(&top_level_modules, &graph)
        .into_iter()
        .filter(|component| component.len() > 1)
        .map(|component| {
            let members: BTreeSet<String> = component.into_iter().collect();
            let mut modules: Vec<String> = members.iter().cloned().collect();
            modules.sort();

            let internal_edges: Vec<DependencyEdge> = edges
                .iter()
                .filter(|edge| members.contains(&edge.from) && members.contains(&edge.to))
                .cloned()
                .collect();

            StronglyConnectedComponent {
                modules,
                internal_edges,
            }
        })
        .collect();

    let adjacency_by_module = build_adjacency_map(&top_level_modules, &edges);

    EngineDependencyGraph {
        module_source: "src/lib.rs".to_string(),
        modules_analyzed: top_level_modules,
        edges,
        strongly_connected_components,
        adjacency_by_module,
    }
}

fn build_adjacency_map(
    modules: &[String],
    edges: &[DependencyEdge],
) -> BTreeMap<String, ModuleAdjacency> {
    let mut incoming: BTreeMap<String, BTreeSet<String>> = modules
        .iter()
        .cloned()
        .map(|module| (module, BTreeSet::new()))
        .collect();
    let mut outgoing: BTreeMap<String, BTreeSet<String>> = modules
        .iter()
        .cloned()
        .map(|module| (module, BTreeSet::new()))
        .collect();

    for edge in edges {
        incoming
            .get_mut(&edge.to)
            .expect("all destination modules should exist in adjacency map")
            .insert(edge.from.clone());
        outgoing
            .get_mut(&edge.from)
            .expect("all source modules should exist in adjacency map")
            .insert(edge.to.clone());
    }

    modules
        .iter()
        .cloned()
        .map(|module| {
            let incoming = incoming
                .remove(&module)
                .expect("all modules should have incoming adjacency entries")
                .into_iter()
                .collect();
            let outgoing = outgoing
                .remove(&module)
                .expect("all modules should have outgoing adjacency entries")
                .into_iter()
                .collect();
            (module, ModuleAdjacency { incoming, outgoing })
        })
        .collect()
}

fn parse_top_level_modules(lib_source: &str) -> Vec<String> {
    let mut modules = Vec::new();
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
    ranges.sort_by(|left, right| right.0.cmp(&left.0));
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
        && !matches!(
            tokens.get(cursor),
            Some(UseToken::Comma) | Some(UseToken::RBrace)
        )
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
    while bytes
        .get(index)
        .is_some_and(|byte| byte.is_ascii_whitespace())
    {
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
        filtered
            .get_mut(&edge.from)
            .expect("target core graph should contain every filtered source")
            .insert(edge.to.clone());
    }

    filtered
}

fn production_source_files() -> Vec<(String, String)> {
    let lib_source = fs::read_to_string(lib_path()).expect("src/lib.rs should be readable");
    let top_level_modules = parse_top_level_modules(&lib_source);
    let mut files = Vec::new();

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

fn source_and_test_rust_files() -> Vec<(String, String)> {
    let mut files = production_source_files();
    let mut test_files = Vec::new();
    let tests_root = engine_root().join("tests");
    walk_rust_files(&tests_root, &mut test_files);

    for absolute_path in test_files {
        let relative_path = absolute_path
            .strip_prefix(engine_root())
            .expect("test source file should be inside the engine root")
            .to_string_lossy()
            .replace('\\', "/");
        let source =
            fs::read_to_string(&absolute_path).expect("test source file should be readable");
        files.push((relative_path, source));
    }

    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

fn is_test_support_relative_path(relative_path: &str) -> bool {
    let parts: Vec<&str> = relative_path.split('/').collect();
    parts.iter().any(|part| {
        *part == "tests"
            || *part == "test"
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

        loop {
            let Some((segment, after_segment)) = parse_identifier(bytes, cursor) else {
                break;
            };
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
            let owner_root = &imported_path[0];
            if owner_root == current_root {
                continue;
            }

            let Some(owner_child_modules) = child_modules.get(owner_root) else {
                continue;
            };
            if !owner_child_modules.contains(&imported_path[1]) {
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

fn sealed_owner_whitelist() -> BTreeSet<&'static str> {
    ["canonical", "catalog", "live_state"].into_iter().collect()
}

fn violations_for_sealed_owners(
    violations: &[SealedOwnerViolation],
    sealed_owners: &BTreeSet<&'static str>,
) -> Vec<SealedOwnerViolation> {
    violations
        .iter()
        .filter(|violation| {
            violation
                .imported_path
                .split("::")
                .next()
                .is_some_and(|owner| sealed_owners.contains(owner))
        })
        .cloned()
        .collect()
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

#[test]
fn sealed_owner_import_rule_lists_current_violations() {
    let actual_violations = current_sealed_owner_violations();
    let actual = render_grouped_sealed_owner_violations(&actual_violations);
    let snapshot_path = engine_root().join(SEALED_OWNER_SNAPSHOT_PATH);
    let expected = fs::read_to_string(&snapshot_path).unwrap_or_default();

    if actual != expected {
        fs::write(&snapshot_path, &actual).expect("sealed-owner snapshot should be writable");
    }
}

#[test]
fn sealed_owner_whitelist_has_no_current_violations() {
    let all_violations = current_sealed_owner_violations();
    let sealed_owners = sealed_owner_whitelist();
    let violations = violations_for_sealed_owners(&all_violations, &sealed_owners);

    assert!(
        violations.is_empty(),
        "owners marked sealed still have child-module import leaks.\n\nSealed owners: {}\n\nCurrent violations:\n{}",
        sealed_owners.iter().copied().collect::<Vec<_>>().join(", "),
        render_grouped_sealed_owner_violations(&violations),
    );
}

#[test]
fn analyzer_resolves_explicit_super_dependencies_to_the_top_level_scope() {
    let current_module_path = vec![
        "sql".to_string(),
        "prepare".to_string(),
        "compile".to_string(),
    ];
    assert_eq!(
        resolve_explicit_dependency(&["super".to_string()], "contracts", &current_module_path),
        Some("sql".to_string()),
    );

    let deeper_module_path = vec![
        "execution".to_string(),
        "write".to_string(),
        "sql_adapter".to_string(),
        "runtime".to_string(),
    ];
    assert_eq!(
        resolve_explicit_dependency(
            &["super".to_string(), "super".to_string()],
            "buffered",
            &deeper_module_path
        ),
        Some("execution".to_string()),
    );
}

// Why this exists: derived-surface declarations are now catalog-owned. If a
// production declaration impl appears elsewhere, we have pushed semantic
// ownership back into execution or orchestration code.
#[test]
fn catalog_projection_definition_impls_are_owned_by_catalog() {
    let impl_paths: Vec<String> = production_source_files()
        .into_iter()
        .filter_map(|(relative_path, source)| {
            let sanitized = mask_rust_source(&source);
            sanitized
                .contains("impl CatalogProjectionDefinition for")
                .then_some(relative_path)
        })
        .collect();

    assert!(
        !impl_paths.is_empty(),
        "expected at least one production `impl CatalogProjectionDefinition for ...` under src/",
    );

    let violations: Vec<String> = impl_paths
        .iter()
        .filter(|path| !path.starts_with("catalog/"))
        .cloned()
        .collect();

    assert!(
        violations.is_empty(),
        "production `CatalogProjectionDefinition` impls must live under `src/catalog/*`; found stray impls in:\n{}",
        violations.join("\n"),
    );
}

// Why this exists: the registry seam only works if built-ins are assembled in
// the registry owner and consumed from startup-owned state. Ad hoc lookups in
// execution paths would silently reintroduce the fallback we just removed.
#[test]
fn builtin_catalog_projection_registry_is_only_used_at_registry_owners() {
    let usage_paths: BTreeSet<String> = production_source_files()
        .into_iter()
        .filter_map(|(relative_path, source)| {
            let sanitized = mask_rust_source(&source);
            sanitized
                .contains("builtin_catalog_projection_registry(")
                .then_some(relative_path)
        })
        .collect();

    let expected_paths: BTreeSet<String> = ["api/lix.rs", "catalog/declaration.rs"]
        .into_iter()
        .map(str::to_string)
        .collect();

    assert_eq!(
        usage_paths, expected_paths,
        "production `builtin_catalog_projection_registry()` usage must stay in the registry owner plus startup/bootstrap seams",
    );
}

#[test]
fn architecture_rules_list_current_failures() {
    let graph = analyze_engine_dependency_graph();
    let modules = module_set(&graph);

    assert_eq!(
        graph.adjacency_by_module.len(),
        modules.len(),
        "every analyzed top-level module should have an adjacency entry",
    );
    for module in &modules {
        assert!(
            graph.adjacency_by_module.contains_key(module),
            "missing adjacency entry for top-level module `{module}`",
        );
    }

    let forbidden_lookup = forbidden_dependency_lookup();

    for rule in FORBIDDEN_DEPENDENCY_RULES {
        assert!(
            modules.contains(rule.from_scope),
            "forbidden dependency map mentions unknown source scope `{}`",
            rule.from_scope,
        );
        for forbidden_scope in rule.forbidden_scopes {
            assert!(
                modules.contains(*forbidden_scope),
                "forbidden dependency map mentions unknown target scope `{forbidden_scope}`",
            );
        }
    }

    for edge in &graph.edges {
        assert!(
            modules.contains(&edge.from),
            "dependency graph should not report an edge from unknown module `{}`",
            edge.from,
        );
        assert!(
            modules.contains(&edge.to),
            "dependency graph should not report an edge to unknown module `{}`",
            edge.to,
        );
    }

    let actual_violations = actual_architecture_violations(&graph, &forbidden_lookup);
    let rendered: Vec<String> = actual_violations
        .iter()
        .map(|edge| {
            let rule = forbidden_lookup
                .get(edge.from.as_str())
                .expect("violation source must have a corresponding rule");
            format!(
                "{} -> {} via {:?}. reason: {}",
                edge.from, edge.to, edge.via_files, rule.reason
            )
        })
        .collect();

    assert!(
        actual_violations.is_empty(),
        "first-principles architecture violations:\n{}",
        rendered.join("\n"),
    );
}

#[test]
fn execution_forbidden_rule_covers_closed_handoff_edges() {
    let forbidden_lookup = forbidden_dependency_lookup();
    let rule = forbidden_lookup
        .get("execution")
        .expect("execution should have an explicit forbidden dependency rule");
    let forbidden_scopes: BTreeSet<&str> = rule.forbidden_scopes.iter().copied().collect();

    for forbidden_scope in ["session", "sql", "runtime", "api"] {
        assert!(
            forbidden_scopes.contains(forbidden_scope),
            "execution forbidden dependency rule should explicitly forbid `{forbidden_scope}`",
        );
    }
}

#[test]
fn target_core_graph_excludes_removed_version_state_root() {
    let graph = analyze_engine_dependency_graph();
    let filtered_graph = target_core_graph(&graph);
    let target_core_modules: Vec<String> = TARGET_CORE_MODULES
        .iter()
        .map(|module| (*module).to_string())
        .collect();

    assert!(
        !target_core_modules
            .iter()
            .any(|module| module == "version_state"),
        "target core modules should no longer include version_state: {:?}",
        target_core_modules,
    );
    assert!(
        !filtered_graph.contains_key("version_state"),
        "target core graph should not include removed version_state root: {:?}",
        filtered_graph,
    );
    assert!(
        graph
            .edges
            .iter()
            .all(|edge| edge.from != "version_state" && edge.to != "version_state"),
        "engine dependency graph should not include version_state edges: {:?}",
        graph
            .edges
            .iter()
            .filter(|edge| edge.from == "version_state" || edge.to == "version_state")
            .collect::<Vec<_>>(),
    );
}

#[test]
fn phase_c_committed_read_handoff_stays_above_read_runtime() {
    let read_runtime_prepare = src_root().join("execution/read/prepare.rs");
    assert!(
        !read_runtime_prepare.exists(),
        "Phase C regression: read_runtime-owned preparation file returned at {}",
        read_runtime_prepare.display()
    );

    let session_source = fs::read_to_string(src_root().join("session/mod.rs"))
        .expect("session/mod.rs should be readable");
    assert!(
        session_source.contains("prepare_committed_read_program_with_backend("),
        "Phase C regression: session no longer owns committed-read preparation"
    );
    assert!(
        session_source.contains("begin_read_unit(prepared_committed_read.transaction_mode)"),
        "Phase C regression: session should begin the committed read transaction after preparation"
    );
    assert!(
        session_source.contains("execute_prepared_read_program_in_committed_read_transaction("),
        "Phase C regression: session should invoke read_runtime with a prepared read program"
    );
    assert!(
        !session_source.contains("TransactionBackendAdapter"),
        "Phase C regression: session should not construct backend adapters for committed-read preparation"
    );

    let read_runtime_source = fs::read_to_string(src_root().join("execution/read/mod.rs"))
        .expect("execution/read/mod.rs should be readable");
    assert!(
        !read_runtime_source.contains("parse_sql_statements("),
        "Phase C regression: read_runtime should not parse SQL during execution"
    );
    assert!(
        !read_runtime_source.contains("compile_committed_read_program"),
        "Phase C regression: read_runtime should not own committed-read compilation"
    );
    assert!(
        !read_runtime_source.contains("TransactionBackendAdapter"),
        "Phase C regression: read_runtime should normalize prepared-read errors from neutral diagnostic contracts, not backend adapters"
    );
}

#[test]
fn plan81_surface_sql_owner_stays_removed() {
    assert!(
        !src_root().join("surface_sql/mod.rs").exists(),
        "Plan 81 regression: src/surface_sql/mod.rs should stay removed"
    );
    assert!(
        !src_root().join("surface_sql/version.rs").exists(),
        "Plan 81 regression: src/surface_sql/version.rs should stay removed"
    );
    assert!(
        !src_root().join("surface_sql/filesystem.rs").exists(),
        "Plan 81 regression: src/surface_sql/filesystem.rs should stay removed"
    );

    let lib_source = fs::read_to_string(lib_path()).expect("src/lib.rs should be readable");
    assert!(
        !lib_source.contains("mod surface_sql;"),
        "Plan 81 regression: src/lib.rs should not reintroduce a surface_sql root module"
    );

    let offenders: Vec<String> = production_source_files()
        .into_iter()
        .filter_map(|(relative_path, source)| {
            let sanitized = mask_rust_source(&source);
            (sanitized.contains("crate::surface_sql::")
                || sanitized.contains("use crate::surface_sql")
                || sanitized.contains("surface_sql::"))
            .then_some(relative_path)
        })
        .collect();

    assert!(
        offenders.is_empty(),
        "Plan 81 regression: surface_sql should stay removed, but these files still reference it:\n{}",
        offenders.join("\n"),
    );
}

#[test]
fn plan81_catalog_relation_lowering_uses_root_owned_sql_api_outside_sql() {
    let offenders: Vec<String> = production_source_files()
        .into_iter()
        .filter(|(relative_path, _)| !relative_path.starts_with("sql/"))
        .filter_map(|(relative_path, source)| {
            let sanitized = mask_rust_source(&source);
            (sanitized.contains("crate::sql::physical_plan::source_sql::")
                || sanitized.contains("use crate::sql::physical_plan::source_sql")
                || sanitized.contains("crate::sql::physical_plan::catalog_relation_sql::")
                || sanitized.contains("use crate::sql::physical_plan::catalog_relation_sql"))
            .then_some(relative_path)
        })
        .collect();

    assert!(
        offenders.is_empty(),
        "Plan 81 regression: non-sql code should reach lowering through crate::sql::* root APIs, not sql child modules:\n{}",
        offenders.join("\n"),
    );
}

#[test]
fn plan82_builtin_registry_access_moves_to_catalog_root_api() {
    let offenders: Vec<String> = source_and_test_rust_files()
        .into_iter()
        .filter_map(|(relative_path, source)| {
            let sanitized = mask_rust_source(&source);
            (sanitized.contains("crate::surfaces::build_builtin_surface_registry")
                || sanitized.contains("crate::surfaces::register_dynamic_entity_surface_spec")
                || sanitized.contains("crate::surfaces::builtin_public_surface_names")
                || sanitized.contains("crate::surfaces::builtin_public_surface_columns")
                || sanitized.contains("use crate::surfaces::{builtin_public_surface_columns")
                || sanitized.contains("use crate::surfaces::{builtin_public_surface_names"))
            .then_some(relative_path)
        })
        .collect();

    assert!(
        offenders.is_empty(),
        "Plan 82 regression: builtin registry access should go through crate::catalog::* root APIs, not crate::surfaces::*:\n{}",
        offenders.join("\n"),
    );
}

#[test]
fn plan82_catalog_root_stays_the_only_public_entrypoint_outside_owner() {
    let offenders: Vec<String> = source_and_test_rust_files()
        .into_iter()
        .filter(|(relative_path, _)| {
            !relative_path.starts_with("src/catalog/")
                && relative_path != "src/catalog.rs"
                && relative_path != "src/lib.rs"
        })
        .filter_map(|(relative_path, source)| {
            let sanitized = mask_rust_source(&source);
            (sanitized.contains("crate::catalog::binding::")
                || sanitized.contains("crate::catalog::registry::")
                || sanitized.contains("use crate::catalog::binding")
                || sanitized.contains("use crate::catalog::registry"))
            .then_some(relative_path)
        })
        .collect();

    assert!(
        offenders.is_empty(),
        "Plan 82 regression: code outside catalog should depend on crate::catalog::* root APIs, not crate::catalog::child::*:\n{}",
        offenders.join("\n"),
    );
}

#[test]
fn plan82_registry_loading_moves_to_runtime_and_session_owners() {
    let runtime_source = fs::read_to_string(src_root().join("runtime/mod.rs"))
        .expect("runtime/mod.rs should be readable");
    assert!(
        runtime_source
            .contains("load_public_surface_registry_with_backend(self.backend().as_ref())"),
        "Plan 82 regression: runtime should load committed registries through the runtime root API"
    );
    assert!(
        !runtime_source.contains("crate::surfaces::load_public_surface_registry_with_backend"),
        "Plan 82 regression: runtime should not reach committed registry loading through surfaces"
    );

    let pending_reads_source = fs::read_to_string(src_root().join("session/pending_reads.rs"))
        .expect("session/pending_reads.rs should be readable");
    assert!(
        pending_reads_source.contains("crate::runtime::load_public_surface_registry_with_backend(self.base)"),
        "Plan 82 regression: session pending reads should use the runtime root API for committed registry loading"
    );
    assert!(
        pending_reads_source.contains("crate::session::apply_registered_schema_snapshot_to_surface_registry("),
        "Plan 82 regression: session pending reads should apply registered-schema overlays through the session root API"
    );
    assert!(
        !pending_reads_source.contains("crate::surfaces::load_public_surface_registry_with_backend"),
        "Plan 82 regression: session pending reads should not load committed registries through surfaces"
    );
    assert!(
        !pending_reads_source
            .contains("crate::surfaces::apply_registered_schema_snapshot_to_surface_registry"),
        "Plan 82 regression: session pending reads should not apply overlays through surfaces"
    );

    assert!(
        !src_root().join("surfaces/mod.rs").exists(),
        "Plan 82 regression: surfaces/mod.rs should stay removed after registry loading moves to runtime and session owners"
    );
}

#[test]
fn plan82_catalog_schema_to_spec_helper_stays_free_of_runtime_sidecars_and_sql_owners() {
    let graph = analyze_engine_dependency_graph();
    for forbidden in ["runtime", "session", "sql"] {
        if let Some(edge) = graph
            .edges
            .iter()
            .find(|edge| edge.from == "catalog" && edge.to == forbidden)
        {
            panic!(
                "Plan 82 regression: catalog must not depend on {forbidden}; current edge flows via {:?}",
                edge.via_files
            );
        }
    }

    let catalog_source = fs::read_to_string(src_root().join("catalog/mod.rs"))
        .expect("catalog/mod.rs should be readable");
    let sanitized = mask_rust_source(&catalog_source);

    assert!(
        sanitized.contains("dynamic_entity_surface_spec_from_schema("),
        "Plan 82 regression: catalog should keep the schema-to-spec helper at the catalog root"
    );

    for forbidden in [
        "crate::runtime::",
        "use crate::runtime",
        "shared_runtime(",
        "crate::session::",
        "use crate::session",
        "crate::sql::",
        "use crate::sql",
    ] {
        assert!(
            !sanitized.contains(forbidden),
            "Plan 82 regression: catalog/mod.rs should stay free of runtime sidecar/session/sql ownership imports, but found `{forbidden}`"
        );
    }
}

#[test]
fn plan82_relation_policy_moves_to_sql_owner() {
    assert!(
        !src_root().join("surfaces/relation_policy.rs").exists(),
        "Plan 82 regression: surfaces/relation_policy.rs should stay removed"
    );
    assert!(
        !src_root().join("surfaces/mod.rs").exists(),
        "Plan 82 regression: surfaces/mod.rs should stay removed"
    );

    let sql_source =
        fs::read_to_string(src_root().join("sql/mod.rs")).expect("sql/mod.rs should be readable");
    assert!(
        sql_source.contains("mod relation_policy;"),
        "Plan 82 regression: sql should own the relation_policy module"
    );
    assert!(
        sql_source.contains("classify_relation_name")
            && sql_source.contains("object_name_is_protected_builtin_ddl_target"),
        "Plan 82 regression: sql root should re-export relation-policy APIs"
    );

    let offenders: Vec<String> = source_and_test_rust_files()
        .into_iter()
        .filter_map(|(relative_path, source)| {
            let sanitized = mask_rust_source(&source);
            (sanitized.contains("crate::surfaces::classify_builtin_relation_name")
                || sanitized.contains("crate::surfaces::classify_relation_name")
                || sanitized.contains("crate::surfaces::object_name_is_internal_storage_relation")
                || sanitized
                    .contains("crate::surfaces::object_name_is_protected_builtin_ddl_target")
                || sanitized.contains("crate::surfaces::builtin_relation_inventory")
                || sanitized.contains("crate::surfaces::protected_builtin_public_surface_names")
                || sanitized.contains("crate::surfaces::relation_policy_choice_summary")
                || sanitized.contains("use crate::surfaces::{classify_builtin_relation_name")
                || sanitized.contains("use crate::surfaces::{classify_relation_name")
                || sanitized.contains("use crate::surfaces::{builtin_relation_inventory")
                || sanitized
                    .contains("use crate::surfaces::{protected_builtin_public_surface_names"))
            .then_some(relative_path)
        })
        .collect();

    assert!(
        offenders.is_empty(),
        "Plan 82 regression: relation-policy helpers should be consumed from crate::sql::*, not crate::surfaces::*:\n{}",
        offenders.join("\n"),
    );
}

#[test]
fn plan82_surfaces_root_stays_removed_from_production_code() {
    let lib_source = fs::read_to_string(lib_path()).expect("src/lib.rs should be readable");
    assert!(
        !lib_source.contains("mod surfaces;"),
        "Plan 82 regression: src/lib.rs should not reintroduce a surfaces root module"
    );

    let offenders: Vec<String> = production_source_files()
        .into_iter()
        .filter_map(|(relative_path, source)| {
            let sanitized = mask_rust_source(&source);
            (sanitized.contains("crate::surfaces::")
                || sanitized.contains("use crate::surfaces")
                || sanitized.contains("surfaces::"))
            .then_some(relative_path)
        })
        .collect();

    assert!(
        offenders.is_empty(),
        "Plan 82 regression: production code should not import crate::surfaces::*:\n{}",
        offenders.join("\n"),
    );
}

#[test]
fn plan82_relation_policy_composes_internal_inventory_from_owner_roots() {
    let sql_source = fs::read_to_string(src_root().join("sql/relation_policy.rs"))
        .expect("sql/relation_policy.rs should be readable");
    let sanitized = mask_rust_source(&sql_source);

    for required in [
        "crate::canonical::internal_exact_relation_names()",
        "crate::live_state::internal_exact_relation_names()",
        "crate::binary_cas::internal_exact_relation_names()",
    ] {
        assert!(
            sanitized.contains(required),
            "Plan 82 regression: sql relation policy should compose internal-relation inventory through sealed owner roots, missing `{required}`"
        );
    }

    for forbidden in [
        "crate::common::naming::internal_exact_relation_names()",
        "crate::canonical::graph::",
        "crate::canonical::journal::",
        "crate::live_state::lifecycle::",
        "crate::live_state::storage::",
        "crate::binary_cas::schema::",
        "crate::session::internal_exact_relation_names()",
        "crate::session::observe::",
        "crate::session::workspace::",
        "crate::session::version_ops::",
        "crate::version_state::checkpoints::",
    ] {
        assert!(
            !sanitized.contains(forbidden),
            "Plan 82 regression: sql relation policy should stay on owner-root APIs, but found `{forbidden}`"
        );
    }
}

#[test]
fn phase_c_read_preparation_stops_reaching_runtime_and_version_owners() {
    let graph = analyze_engine_dependency_graph();

    if let Some(sql_runtime_edge) = graph
        .edges
        .iter()
        .find(|edge| edge.from == "sql" && edge.to == "runtime")
    {
        assert!(
            !sql_runtime_edge
                .via_files
                .contains(&"sql/prepare/prepared_read.rs".to_string()),
            "Phase C regression: sql -> runtime still flows through sql/prepare/prepared_read.rs: {:?}",
            sql_runtime_edge.via_files,
        );
    }
}

#[test]
fn phase_f_session_write_preparation_stays_as_orchestration_only() {
    let session_source = read_engine_source("session/mod.rs");
    assert_source_contains_in_order(
        "session/mod.rs",
        &session_source,
        &[
            "parse_sql_with_timing(",
            "ExecutionProgram::compile(",
            "begin_write_unit().await?",
            "execute_execution_program_with_write_transaction(",
        ],
    );

    let write_preparation_source = read_engine_source("session/write_preparation.rs");
    let write_pipeline_source = read_engine_source("session/write_pipeline.rs");

    assert!(
        !src_root().join("session/write_execution").exists(),
        "Phase F regression: session/write_execution/ should stay removed once write execution is execution-owned"
    );

    for forbidden in [
        "load_sql_compiler_metadata_with_reader(",
        "compile_execution_from_template_instance_with_context(",
        "PreparedWriteExecutionStep::build(",
        "run_public_tracked_append_txn_with_transaction(",
        "run_public_untracked_write_txn_with_transaction(",
        "run_internal_write_txn_with_transaction(",
        "execute_planned_write_delta(",
    ] {
        assert!(
            !write_preparation_source.contains(forbidden),
            "Phase F regression: session/write_preparation.rs should not reacquire lower-level write preparation or write apply via `{forbidden}`"
        );
    }

    for forbidden in [
        "run_public_tracked_append_txn_with_transaction(",
        "run_public_untracked_write_txn_with_transaction(",
        "run_internal_write_txn_with_transaction(",
        "execute_planned_write_delta(",
    ] {
        assert!(
            !write_pipeline_source.contains(forbidden),
            "Phase F regression: session/write_pipeline.rs should not reacquire execution-owned write apply via `{forbidden}`"
        );
    }

    for required in [
        "PreparedWritePreparationContext",
        "PreparedWriteContextStamp",
        "load_sql_compiler_metadata_with_reader_and_pending_view(",
        "compile_execution_from_template_instance_with_context(",
        "PreparedWriteExecutionBoundary",
        "PreparedWriteExecutionStep::build(",
    ] {
        assert!(
            write_pipeline_source.contains(required),
            "Phase F regression: session/write_pipeline.rs should own write preparation step `{required}`"
        );
    }

    assert_source_contains_in_order(
        "session/write_preparation.rs",
        &write_preparation_source,
        &[
            "bootstrap_prepared_write_preparation_context(",
            "prepare_buffered_write_execution_step(",
            "execute_prepared_write_execution_step_with_transaction(",
        ],
    );
}

#[test]
fn phase_f_session_selector_reads_stays_as_orchestration_only() {
    let selector_reads_source = read_engine_source("session/selector_reads.rs");
    let read_preparation_source = read_engine_source("session/read_preparation.rs");

    assert!(
        !src_root().join("session/read_execution.rs").exists(),
        "Phase F regression: session/read_execution.rs should stay removed once committed read execution lives under execution/read/"
    );

    for forbidden in [
        "bootstrap_public_surface_registry_with_pending_transaction_view(",
        "build_surface_registry(",
        "load_sql_compiler_metadata(",
        "load_active_history_root_commit_id_for_preparation(",
        "try_prepare_public_read_with_registry_and_internal_access(",
        "prepare_public_read_artifact(",
    ] {
        assert!(
            !selector_reads_source.contains(forbidden),
            "Phase F regression: session/selector_reads.rs should not reacquire selector-read preparation via `{forbidden}`"
        );
    }

    for required in [
        "build_surface_registry(",
        "load_sql_compiler_metadata(",
        "load_active_history_root_commit_id_for_preparation(",
        "try_prepare_public_read_with_registry_and_internal_access(",
        "prepare_public_read_artifact(",
    ] {
        assert!(
            read_preparation_source.contains(required),
            "Phase F regression: session/read_preparation.rs should own selector-read preparation step `{required}`"
        );
    }

    assert_source_contains_in_order(
        "session/selector_reads.rs",
        &selector_reads_source,
        &[
            "bootstrap_prepared_public_read_collaborators(",
            "build_public_selector_query(",
            "prepare_required_active_public_read_artifact_with_backend(",
            "execute_prepared_public_read_with_pending_view(",
        ],
    );
}

#[test]
fn phase_f_runtime_deterministic_storage_consumes_resolved_scope_only() {
    let storage_source = read_engine_source("runtime/deterministic_mode/storage.rs");
    let scope_source = read_engine_source("runtime/deterministic_mode/scope.rs");

    for forbidden in [
        "key_value_schema_key(",
        "TRACKED_LIVE_TABLE_PREFIX",
        "tracked_live_table_name(",
        "GLOBAL_VERSION_ID",
    ] {
        assert!(
            !storage_source.contains(forbidden),
            "Phase F regression: runtime/deterministic_mode/storage.rs should not reconstruct semantic storage via `{forbidden}`"
        );
    }

    for required in ["scope.table_name", "scope.version_id"] {
        assert!(
            storage_source.contains(required),
            "Phase F regression: runtime/deterministic_mode/storage.rs should consume resolved scope field `{required}`"
        );
    }

    for required in [
        "tracked_relation_name(",
        "key_value_schema_key(",
        "PersistedKeyValueStorageScope::new(",
        "\"global\"",
    ] {
        assert!(
            scope_source.contains(required),
            "Phase F regression: deterministic_settings_scope.rs should own runtime deterministic scope resolution step `{required}`"
        );
    }
}

#[test]
fn phase_g_session_mod_defers_version_convenience_to_request_collaborators() {
    let session_source = read_engine_source("session/mod.rs");

    for forbidden in [
        "crate::version::create_version_in_session(",
        "crate::version::merge_version_in_session(",
        "crate::version::undo_redo::undo_with_options_in_session(",
        "crate::version::undo_redo::redo_with_options_in_session(",
        "crate::version::context::ensure_version_exists_with_backend(",
    ] {
        assert!(
            !session_source.contains(forbidden),
            "Phase G regression: session/mod.rs should not reach version owner directly via `{forbidden}`"
        );
    }

    for required in [
        ".create_version_in_session(self, options)",
        ".merge_version_in_session(self, options)",
        ".undo_with_options_in_session(self, options)",
        ".redo_with_options_in_session(self, options)",
        ".ensure_version_exists(version_id)",
    ] {
        assert!(
            session_source.contains(required),
            "Phase G regression: session/mod.rs should route version convenience through collaborators via `{required}`"
        );
    }
}

#[test]
fn phase_g_sql_uses_neutral_dialect_seam_instead_of_backend_owner() {
    let dialect_source = read_engine_source("common/dialect.rs");
    assert!(
        dialect_source.contains("pub enum SqlDialect"),
        "common/dialect.rs should own the neutral SqlDialect foundation type"
    );

    for relative in [
        "sql/ast/lower_json_fn.rs",
        "sql/ast/lowering.rs",
        "sql/explain/mod.rs",
    ] {
        let source = read_engine_source(relative);
        assert!(
            !source.contains("crate::backend::SqlDialect"),
            "{relative} should not import SqlDialect from backend owner"
        );
        assert!(
            source.contains("SqlDialect"),
            "{relative} should consume the neutral SqlDialect seam"
        );
    }
}

#[test]
fn plan41_phase_d_keeps_relation_policy_out_of_session_sql_read_and_write_layers() {
    for module in ["execution", "session", "sql"] {
        for absolute_path in rust_files_for_top_level_module(module) {
            let relative = absolute_path
                .strip_prefix(src_root())
                .expect("module source file should be inside src/")
                .to_string_lossy()
                .replace('\\', "/");
            let source =
                fs::read_to_string(&absolute_path).expect("module source file should be readable");

            for forbidden in [
                "starts_with(\"lix_internal_\")",
                "starts_with(\"lix_\") && !relation.starts_with(\"lix_internal_\")",
            ] {
                assert!(
                    !source.contains(forbidden),
                    "Plan 41 Phase D regression: {relative} should not recreate relation-protection policy via `{forbidden}`"
                );
            }
        }
    }
}

#[test]
fn prepared_write_execution_seam_stays_closed_over_runtime_inputs() {
    let contracts_source = read_engine_source("contracts/artifacts.rs");
    assert!(
        contracts_source.contains("struct PreparedWriteProgram"),
        "contracts/artifacts.rs should keep the neutral PreparedWriteProgram handoff"
    );
    assert!(
        contracts_source.contains("struct PreparedWriteStep"),
        "contracts/artifacts.rs should keep the prepared write step contract"
    );

    for relative in [
        "execution/write/sql_adapter/runtime.rs",
        "execution/write/buffered/planned_write.rs",
        "execution/write/sql_adapter/mod.rs",
        "execution/write/sql_adapter/planned_write_runner.rs",
    ] {
        let source = read_engine_source(relative);
        for forbidden in [
            "BoundStatementTemplateInstance",
            "CompiledExecution",
            "PhysicalPlan::PublicWrite",
            "crate::sql::prepare::",
            "crate::session::",
            "ExecutionContext",
            "use crate::Lix;",
            "&Lix",
            "sqlparser::ast",
            "load_sql_compiler_metadata(",
            "compile_execution_from_template_instance_with_context(",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative} should stay free of compile-time or whole-owner reach-through via `{forbidden}`"
            );
        }
    }

    let runtime_source = read_engine_source("execution/write/sql_adapter/runtime.rs");
    for required in [
        "PreparedWriteStep",
        "PreparedWriteExecutionStep",
        "PreparedWriteExecutionRoute",
    ] {
        assert!(
            runtime_source.contains(required),
            "execution/write/sql_adapter/runtime.rs should execute from prepared seam item `{required}`"
        );
    }

    let planned_write_source = read_engine_source("execution/write/buffered/planned_write.rs");
    for required in [
        "PreparedWriteStep",
        "PreparedPublicWriteArtifact",
        "PreparedInternalWriteArtifact",
        "PreparedWriteRuntimeState",
    ] {
        assert!(
            planned_write_source.contains(required),
            "execution/write/buffered/planned_write.rs should plan from prepared artifact `{required}`"
        );
    }
}

#[test]
fn phase_d_contracts_artifacts_stays_the_only_generic_artifact_bucket() {
    let artifact_paths: BTreeSet<String> = production_source_files()
        .into_iter()
        .map(|(relative_path, _)| relative_path)
        .filter(|relative_path| relative_path.ends_with("/artifacts.rs"))
        .collect();

    let expected_paths: BTreeSet<String> = ["contracts/artifacts.rs"]
        .into_iter()
        .map(str::to_string)
        .collect();

    assert_eq!(
        artifact_paths, expected_paths,
        "Phase D regression: owner-local artifact families should use role-based file names; only contracts/artifacts.rs should remain generic",
    );
}

#[test]
fn pending_transaction_view_is_write_runtime_owned() {
    let executor_compile_source = read_engine_source("sql/prepare/compile.rs");
    assert!(
        !executor_compile_source.contains("struct PendingTransactionView"),
        "executor compile ownership should not define PendingTransactionView once write_runtime owns pending visibility"
    );

    let overlay_mod_source = read_engine_source("execution/write/overlay/mod.rs");
    assert!(
        overlay_mod_source.contains("mod pending_view;"),
        "execution/write/overlay/mod.rs should compile the pending_view module"
    );

    let pending_view_source = read_engine_source("execution/write/overlay/pending_view.rs");
    assert!(
        pending_view_source.contains("struct PendingTransactionView"),
        "execution/write/overlay/pending_view.rs should own PendingTransactionView"
    );
}

#[test]
fn plugin_install_path_uses_write_runtime_owned_write_entrypoints() {
    let init_source = read_engine_source("init/seed.rs");
    assert!(
        init_source.contains("BorrowedBufferedWriteTransaction"),
        "init/seed.rs should route its borrowed backend transaction through the buffered write-runtime wrapper"
    );
    assert!(
        init_source.contains("execute_parsed_statements_in_borrowed_write_transaction"),
        "init/seed.rs should execute writes through the session-owned write orchestration seam"
    );

    let plugin_session_source = read_engine_source("session/plugin.rs");
    assert!(
        plugin_session_source.contains("BufferedWriteTransaction::new("),
        "session/plugin.rs should use the write-runtime-owned buffered write lifecycle"
    );
    assert!(
        plugin_session_source.contains("commit_buffered_write("),
        "session/plugin.rs should finish plugin installation through the buffered write-runtime commit seam"
    );
    assert!(
        plugin_session_source.contains("install_plugin_archive_with_writer")
            && plugin_session_source.contains("PluginInstallWriteExecutor"),
        "session/plugin.rs should adapt plugin installation through the write-runtime plugin writer seam"
    );

    let plugin_source = read_engine_source("execution/write/plugin_install.rs");
    assert!(
        plugin_source.contains("trait PluginInstallWriteExecutor"),
        "execution/write/plugin_install.rs should define a narrow write executor seam"
    );
    for forbidden in [
        "execute_with_options_in_write_transaction",
        "crate::session::plugin",
        "crate::session::execution_context::ExecutionContext",
    ] {
        assert!(
            !plugin_source.contains(forbidden),
            "execution/write/plugin_install.rs should stay free of session-owned write execution through `{forbidden}`"
        );
    }
}

#[test]
fn planned_write_runner_and_filesystem_resolution_stay_split_by_owner() {
    let runner_source = read_engine_source("execution/write/sql_adapter/planned_write_runner.rs");
    assert!(
        runner_source.contains("run_public_tracked_append_txn_with_transaction("),
        "planned_write_runner.rs should delegate tracked append apply"
    );
    assert!(
        runner_source.contains("run_internal_write_txn_with_transaction("),
        "planned_write_runner.rs should delegate internal apply"
    );
    for forbidden in [
        "append_tracked_with_pending_public_session(",
        "execute_internal_execution_with_transaction(",
        "validate_commit_time_write(",
        "persist_filesystem_payload_changes_direct(",
    ] {
        assert!(
            !runner_source.contains(forbidden),
            "planned_write_runner.rs should not own `{forbidden}` after the split"
        );
    }

    let resolve_source = read_engine_source("session/write_resolution/mod.rs");
    assert!(
        resolve_source.contains("mod filesystem_writes;"),
        "session/write_resolution/mod.rs should compile the filesystem_writes owner"
    );
    assert!(
        !resolve_source.contains("mod filesystem_insert_planning;"),
        "session/write_resolution/mod.rs should not compile a sibling filesystem_insert_planning module"
    );

    let filesystem_source = read_engine_source("session/write_resolution/filesystem_writes.rs");
    assert!(
        filesystem_source.contains("mod insert_planning;"),
        "session/write_resolution/filesystem_writes.rs should compile its insert planning as an internal owner submodule"
    );
    assert!(
        src_root()
            .join("session/write_resolution/filesystem_writes/insert_planning.rs")
            .exists(),
        "insert planning should live under the filesystem_writes runtime owner area"
    );
    assert!(
        !src_root()
            .join("session/write_resolution/filesystem_insert_planning.rs")
            .exists(),
        "the standalone filesystem_insert_planning.rs owner split should stay removed"
    );
}

#[test]
fn phase_b_canonical_owner_stays_free_of_session_imports() {
    for absolute_path in rust_files_for_top_level_module("canonical") {
        let relative_path = absolute_path
            .strip_prefix(src_root())
            .expect("canonical file should live under src/")
            .to_string_lossy()
            .replace('\\', "/");
        let source = strip_test_code(
            &fs::read_to_string(&absolute_path).expect("canonical source should be readable"),
        );
        for forbidden in ["use crate::session", "crate::session::"] {
            assert!(
                !source.contains(forbidden),
                "{relative_path} should stay free of session-owner reach-through via `{forbidden}`"
            );
        }
    }
}

#[test]
fn phase_b_canonical_read_exports_commit_addressed_apis_only() {
    let canonical_read_mod = read_engine_source("canonical/read/mod.rs");
    for forbidden in [
        "load_exact_committed_state_row_at_version_head",
        "load_version_info_for_versions",
        "VersionInfo",
        "VersionSnapshot",
    ] {
        assert!(
            !canonical_read_mod.contains(forbidden),
            "canonical/read/mod.rs should not re-export version-addressed helper `{forbidden}`"
        );
    }

    let canonical_read_state = strip_test_code(&read_engine_source("canonical/read/state.rs"));
    for forbidden in ["use crate::version_state", "crate::version_state::"] {
        assert!(
            !canonical_read_state.contains(forbidden),
            "canonical/read/state.rs should stay commit-addressed without `{forbidden}`"
        );
    }
}

#[test]
fn phase_b_checkpoint_history_rebuild_uses_explicit_head_inputs() {
    let history_source = strip_test_code(&read_engine_source(
        "canonical/checkpoint_labels/history.rs",
    ));
    assert!(
        history_source.contains("struct CheckpointVersionHeadFact"),
        "canonical/checkpoint_labels/history.rs should define explicit checkpoint rebuild inputs"
    );
    assert!(
        history_source.contains("rebuild_internal_last_checkpoint_from_heads"),
        "checkpoint history rebuild should accept resolved version-head facts from callers"
    );
    for forbidden in [
        "use crate::session",
        "crate::session::",
        "use crate::version_state",
        "crate::version_state::",
    ] {
        assert!(
            !history_source.contains(forbidden),
            "canonical/checkpoint_labels/history.rs should not resolve version ownership internally via `{forbidden}`"
        );
    }
}

#[test]
fn phase_b_contracts_stay_free_of_projection_registry_coupling() {
    for absolute_path in rust_files_for_top_level_module("contracts") {
        let relative_path = absolute_path
            .strip_prefix(src_root())
            .expect("contracts file should live under src/")
            .to_string_lossy()
            .replace('\\', "/");
        let source = strip_test_code(
            &fs::read_to_string(&absolute_path).expect("contracts source should be readable"),
        );
        for forbidden in [
            "use crate::projections",
            "crate::projections::",
            "ProjectionRegistry",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative_path} should stay free of projection-registry coupling via `{forbidden}`"
            );
        }
    }
}

#[test]
fn phase_b_contracts_stay_free_of_live_state_imports() {
    for absolute_path in rust_files_for_top_level_module("contracts") {
        let relative_path = absolute_path
            .strip_prefix(src_root())
            .expect("contracts file should live under src/")
            .to_string_lossy()
            .replace('\\', "/");
        let source = strip_test_code(
            &fs::read_to_string(&absolute_path).expect("contracts source should be readable"),
        );
        for forbidden in ["use crate::live_state", "crate::live_state::"] {
            assert!(
                !source.contains(forbidden),
                "{relative_path} should stay free of live_state imports via `{forbidden}`"
            );
        }
    }
}

#[test]
fn phase_b_sql_stays_free_of_projection_owner_imports() {
    for absolute_path in rust_files_for_top_level_module("sql") {
        let relative_path = absolute_path
            .strip_prefix(src_root())
            .expect("sql file should live under src/")
            .to_string_lossy()
            .replace('\\', "/");
        let source = strip_test_code(
            &fs::read_to_string(&absolute_path).expect("sql source should be readable"),
        );
        for forbidden in ["use crate::projections", "crate::projections::"] {
            assert!(
                !source.contains(forbidden),
                "{relative_path} should stay free of projection-owner imports via `{forbidden}`"
            );
        }
    }
}
