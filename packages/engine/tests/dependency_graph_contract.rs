#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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
            "checkpoint",
            "engine",
            "live_state",
            "projections",
            "read_runtime",
            "runtime",
            "session",
            "sql",
            "version",
            "write_runtime",
        ],
    },
    ForbiddenDependencyRule {
        from_scope: "runtime",
        reason: "runtime is a sidecar and must not reacquire semantic owners or the live-state engine",
        forbidden_scopes: &[
            "engine",
            "live_state",
            "session",
            "sql",
            "version",
            "write_runtime",
        ],
    },
    ForbiddenDependencyRule {
        from_scope: "live_state",
        reason: "live_state is the generic projection engine and must not depend on projection definitions or dissolved domain owners",
        forbidden_scopes: &[
            "projections",
            "runtime",
            "version",
            "write_runtime",
        ],
    },
    ForbiddenDependencyRule {
        from_scope: "projections",
        reason: "projection definitions must stay declarative and must not reach storage or projection-engine owners directly",
        forbidden_scopes: &["backend", "canonical", "live_state"],
    },
    ForbiddenDependencyRule {
        from_scope: "sql",
        reason: "sql is the compiler and should depend on contracts and foundation only, never state owners or dissolved domains directly",
        forbidden_scopes: &[
            "backend",
            "binary_cas",
            "canonical",
            "live_schema_access",
            "live_state",
            "projections",
            "read_runtime",
            "runtime",
            "session",
            "version",
            "write_runtime",
        ],
    },
    ForbiddenDependencyRule {
        from_scope: "read_runtime",
        reason: "read_runtime should consume contracts and live_state, not compiler internals, session orchestration, or dissolved domain owners",
        forbidden_scopes: &[
            "canonical",
            "runtime",
            "session",
            "sql",
            "version",
            "write_runtime",
        ],
    },
    ForbiddenDependencyRule {
        from_scope: "write_runtime",
        reason: "write_runtime should consume contracts, live_state, and canonical; compiler access must come through artifacts rather than direct owner imports",
        forbidden_scopes: &[
            "backend",
            "binary_cas",
            "checkpoint",
            "engine",
            "init",
            "live_schema_access",
            "read_runtime",
            "runtime",
            "session",
            "sql",
            "version",
        ],
    },
    ForbiddenDependencyRule {
        from_scope: "session",
        reason: "session is orchestration; it may call sql, read_runtime, write_runtime, and checkpoint convenience APIs but should not reach lower owners directly",
        forbidden_scopes: &[
            "backend",
            "binary_cas",
            "canonical",
            "engine",
            "init",
            "live_schema_access",
            "live_state",
            "runtime",
            "schema",
            "version",
        ],
    },
];

const TARGET_CORE_MODULES: &[&str] = &[
    "backend",
    "canonical",
    "checkpoint",
    "contracts",
    "live_state",
    "projections",
    "read_runtime",
    "runtime",
    "session",
    "sql",
    "write_runtime",
];

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
            let source =
                fs::read_to_string(&absolute_path).expect("module source file should be readable");
            files.push((relative_path, strip_test_code(&source)));
        }
    }

    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
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
        "write_runtime".to_string(),
        "sql_adapter".to_string(),
        "execute".to_string(),
    ];
    assert_eq!(
        resolve_explicit_dependency(
            &["super".to_string(), "super".to_string()],
            "runtime",
            &deeper_module_path,
        ),
        Some("write_runtime".to_string()),
    );
}

// Why this exists: new projections should extend the `projections/*` owner
// boundary. If a production `ProjectionTrait` impl appears elsewhere, we have
// pushed projection definition work back into execution or orchestration code.
#[test]
fn projection_trait_impls_are_owned_by_projections() {
    let impl_paths: Vec<String> = production_source_files()
        .into_iter()
        .filter_map(|(relative_path, source)| {
            let sanitized = mask_rust_source(&source);
            sanitized
                .contains("impl ProjectionTrait for")
                .then_some(relative_path)
        })
        .collect();

    assert!(
        !impl_paths.is_empty(),
        "expected at least one production `impl ProjectionTrait for ...` under src/",
    );

    let violations: Vec<String> = impl_paths
        .iter()
        .filter(|path| !path.starts_with("projections/"))
        .cloned()
        .collect();

    assert!(
        violations.is_empty(),
        "production `ProjectionTrait` impls must live under `src/projections/*`; found stray impls in:\n{}",
        violations.join("\n"),
    );
}

// Why this exists: the registry seam only works if built-ins are assembled in
// the registry owner and consumed from startup-owned state. Ad hoc lookups in
// execution paths would silently reintroduce the fallback we just removed.
#[test]
fn builtin_projection_registry_is_only_used_at_registry_owners() {
    let usage_paths: BTreeSet<String> = production_source_files()
        .into_iter()
        .filter_map(|(relative_path, source)| {
            let sanitized = mask_rust_source(&source);
            sanitized
                .contains("builtin_projection_registry(")
                .then_some(relative_path)
        })
        .collect();

    let expected_paths: BTreeSet<String> = ["engine.rs", "projections/mod.rs"]
        .into_iter()
        .map(str::to_string)
        .collect();

    assert_eq!(
        usage_paths, expected_paths,
        "production `builtin_projection_registry()` usage must stay in the registry owner plus startup owner",
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
fn target_core_graph_lists_current_cycles() {
    let graph = analyze_engine_dependency_graph();
    let filtered_graph = target_core_graph(&graph);
    let target_core_modules: Vec<String> = TARGET_CORE_MODULES
        .iter()
        .map(|module| (*module).to_string())
        .collect();
    let cyclic_components: Vec<StronglyConnectedComponent> =
        tarjan(&target_core_modules, &filtered_graph)
            .into_iter()
            .filter(|component| component.len() > 1)
            .map(|component| {
                let members: BTreeSet<String> = component.into_iter().collect();
                let mut modules: Vec<String> = members.iter().cloned().collect();
                modules.sort();

                let internal_edges: Vec<DependencyEdge> = graph
                    .edges
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

    assert!(
        cyclic_components.is_empty(),
        "target core graph still has cycles: {:#?}",
        cyclic_components,
    );
}

#[test]
fn phase_c_committed_read_handoff_stays_above_read_runtime() {
    let read_runtime_prepare = src_root().join("read_runtime/prepare.rs");
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

    let read_runtime_source = fs::read_to_string(src_root().join("read_runtime/mod.rs"))
        .expect("read_runtime/mod.rs should be readable");
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

    if let Some(sql_version_edge) = graph
        .edges
        .iter()
        .find(|edge| edge.from == "sql" && edge.to == "version")
    {
        assert!(
            !sql_version_edge
                .via_files
                .contains(&"sql/prepare/prepared_read.rs".to_string()),
            "Phase C regression: sql -> version still flows through sql/prepare/prepared_read.rs: {:?}",
            sql_version_edge.via_files,
        );
        assert!(
            !sql_version_edge
                .via_files
                .contains(&"sql/prepare/compiler_metadata.rs".to_string()),
            "Phase C regression: sql -> version still flows through sql/prepare/compiler_metadata.rs: {:?}",
            sql_version_edge.via_files,
        );
    }
}
