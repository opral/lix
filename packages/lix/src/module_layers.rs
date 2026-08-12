//! The engine's declared module layer order, and the test that enforces it.
//!
//! Rust has no notion of intra-crate module acyclicity, so nothing in the
//! toolchain notices when a lower module starts reaching up into a higher one.
//! That matters here because one of the layout invariants is *structural*: a
//! derived view may read the canonical store, and the canonical store may not
//! read the derived view. `tracked_state` (canonical state) sitting below
//! `hot_state` (the hot plane) is what makes "derived views are caches, never
//! the correctness authority" checkable rather than merely conventional.
//!
//! [`MODULE_LAYERS`] is the declared order — the artifact itself, reviewable in
//! a diff. The test walks `$CARGO_MANIFEST_DIR/src`, attributes every
//! `crate::<module>` path in production code to the top-level module of the
//! file containing it, and fails on any reference that points at a strictly
//! higher layer.
//!
//! **The order is deliberately partial.** Only modules listed in
//! [`MODULE_LAYERS`] are constrained; every other top-level module is listed in
//! [`UNLAYERED_MODULES`] with the reason it is not ordered yet. Encoding an
//! aspirational order that fails today would only produce a guard someone
//! silences. Moving a module out of [`UNLAYERED_MODULES`] and into
//! [`MODULE_LAYERS`] is the unit of progress.

/// The declared layer order, bottom first. A module may reference modules in
/// strictly lower layers, and modules outside this list, but never a module in
/// its own layer or above.
///
/// Every entry below is verified acyclic against the current tree; there are no
/// allowances. See [`UNLAYERED_MODULES`] for what is not covered yet and why.
const MODULE_LAYERS: &[&[&str]] = &[
    // Pure encodings with no repository semantics.
    &["compression", "storage_codec"],
    // The physical key space and write-set mechanism everything above records
    // its bytes through.
    &["storage_adapter"],
    // Value types and generic containers layered directly on the key space.
    &["account", "binary_cas", "columnar_row_group", "common", "json_store"],
    &["entity_pk"],
    // The order-preserving key byte format. A pure encoding over `entity_pk`
    // with no repository semantics, and the single authority for how a key
    // encodes. Both state planes store rows in the order it defines, so it must
    // sit below both and must never read either back.
    &["order_preserving_key"],
    // The canonical facts: changes, commits, commit-change ids.
    &["changelog"],
    // The collection-replacement marker. Both state planes read it to decide
    // whether a generation can be replaced wholesale, so it must stay below
    // them and must never read either one back.
    &["collection_generation"],
    // The entity-specific contract over generic columnar row groups. Addresses
    // row groups by owning commit, so it sits above `changelog`, and is shared
    // by both state planes, so it sits below them.
    &["entity_columnar"],
    // Canonical state: commit-state manifests, tree chunks, scoped ranges,
    // commit deltas. Sole writer of every canonical-state storage space.
    &["tracked_state"],
    // Topology plus history resolution. Pure traversal needs only `changelog`,
    // but resolving *what changed* at a commit genuinely needs the commit-delta
    // plane, so this is honestly placed above `tracked_state` rather than
    // recorded as an exception. It performs no production writes.
    &["commit_graph"],
    // Branch-head control authority. It resolves heads over commit topology and
    // the hot plane reads branch heads back, which pins it to exactly this
    // position: above `commit_graph`, below `hot_state`.
    &["branch"],
    // The derived serving plane — a rebuildable cache over the canonical state.
    // Sole writer of every hot-plane storage space. Its position above
    // `tracked_state` is the structural half of the "derived views are caches"
    // invariant: the compiler now rejects a canonical-side read of the cache.
    &["hot_state"],
    // Entity-level overlays over the state planes. None of them is read by any
    // module below, and none reads another: `checkpoint` and `plugin` both use
    // `branch`, `undo_redo` only needs `changelog`.
    &["checkpoint", "plugin", "undo_redo"],
];

/// Top-level modules deliberately left out of [`MODULE_LAYERS`], with the
/// reason. This list is the to-do, not an amnesty: a guard that starts red gets
/// ignored, so an order is only declared once it holds.
///
/// The former dominant blocker — `transaction::types` living inside the
/// write-path module that consumes it — is gone: it is now
/// [`crate::transaction_types`], and `branch`, `checkpoint`,
/// `collection_generation`, `plugin` and `undo_redo` moved into
/// [`MODULE_LAYERS`] as a result — ten mutual cycles became six. What remains
/// is `session` ↔ `transaction` (the `EXECUTE_IDEMPOTENCY_RECEIPT_SPACE`
/// ownership inversion), `gc` ↔ `session` (correct by design), `filesystem` ↔
/// `hot_state`, and the two upward references `transaction_types` still makes
/// of its own — into `sql2` and `collection_generation`.
const UNLAYERED_MODULES: &[(&str, &str)] = &[
    ("catalog", "not yet analysed"),
    ("cel", "leaf utility, no layer semantics"),
    ("client_state", "entry-point plumbing, no layer semantics"),
    ("default_wasm_runtime", "feature-gated harness"),
    ("domain", "pure predicates, no owned invariant"),
    ("engine", "entry point; reaches everywhere by design"),
    (
        "filesystem",
        "genuinely spans the overlay and write-path layers",
    ),
    ("functions", "not yet analysed"),
    (
        "gc",
        "peer of `session` in the reclamation cycle, by design",
    ),
    ("handle", "entry point; reaches everywhere by design"),
    ("hot_index_aging_probe", "cfg(test) measurement probe"),
    ("hot_row_tombstone_probe", "cfg(test) measurement probe"),
    ("init", "entry point; reaches everywhere by design"),
    ("lib", "crate root"),
    ("module_layers", "this guard"),
    ("observe_coordinator", "not yet analysed"),
    ("observe_invalidation", "not yet analysed"),
    ("plugin_arena", "leaf utility, no layer semantics"),
    ("plugin_layout", "leaf utility, no layer semantics"),
    ("plugin_wire", "leaf utility, no layer semantics"),
    ("prepared_dml", "leaf utility, no layer semantics"),
    ("registered_spaces", "cfg-gated harness"),
    ("schema", "not yet analysed"),
    ("session", "cyclic with `transaction` and with `gc`"),
    (
        "sql2",
        "still cyclic with `transaction` (one reference, \
         `duplicate_insert_identity_message`) and with `transaction_types`, \
         which names `sql2::EncodedEntityRowGroups`",
    ),
    ("sql_profile", "feature-gated instrumentation"),
    ("sql_telemetry", "instrumentation, no layer semantics"),
    (
        "storage",
        "public API facade: re-exports `handle` and uses `filesystem` for the \
         in-memory backend, so it is not a layer",
    ),
    ("storage_bench", "feature-gated harness"),
    (
        "storage_spaces",
        "the space registry, which `storage::types` reads to check the value \
         semantics a space id is declared with, so it sits beside `storage` \
         rather than below it",
    ),
    ("telemetry", "instrumentation, no layer semantics"),
    ("test_support", "cfg-gated harness"),
    (
        "transaction",
        "down from six cycles to two: still cyclic with `session` and with \
         `sql2`",
    ),
    (
        "transaction_types",
        "the shared write-row vocabulary, now its own module. It cannot be \
         layered yet because it names `hot_state`'s materialized row types \
         while `hot_state` reads `branch`, which reads this module — so \
         layering it would have to place it both above and below `hot_state`. \
         Deduplicating the `Materialized*Row` DTOs downward unblocks it",
    ),
    ("wasm", "plugin ABI vocabulary, not yet analysed"),
];

/// Upward references that are intentional, as
/// `(from_module, to_module, reason)`.
///
/// Empty on purpose: every module currently in [`MODULE_LAYERS`] is acyclic
/// with respect to the declared order. An entry here documents an exception at
/// the point of enforcement instead of in a review comment — but prefer moving
/// the module to [`UNLAYERED_MODULES`] over adding one, because an exception
/// list is where a layering quietly stops meaning anything.
const ALLOWED_UPWARD_EDGES: &[(&str, &str, &str)] = &[];

/// One `crate::<module>` reference found in production source.
#[derive(Debug)]
struct Reference {
    from: String,
    to: String,
    file: String,
    line: usize,
}

fn layer_of(module: &str) -> Option<usize> {
    MODULE_LAYERS
        .iter()
        .position(|layer| layer.contains(&module))
}

/// Blanks the bytes in `range`, preserving newlines so byte offsets still map
/// to line numbers. Non-ASCII bytes are left alone: they cannot spell a path,
/// and rewriting them would break the UTF-8 encoding of the buffer.
fn blank(buffer: &mut [u8], range: std::ops::Range<usize>) {
    for byte in &mut buffer[range] {
        if byte.is_ascii() && *byte != b'\n' {
            *byte = b' ';
        }
    }
}

/// Blanks comments and string, raw-string and char literals, so a path spelled
/// inside a doc comment or a test fixture is not mistaken for a reference.
fn blank_comments_and_literals(buffer: &mut [u8]) {
    let length = buffer.len();
    let mut index = 0;
    while index < length {
        let byte = buffer[index];
        let next = buffer.get(index + 1).copied();
        if byte == b'/' && next == Some(b'/') {
            let end = buffer[index..]
                .iter()
                .position(|byte| *byte == b'\n')
                .map_or(length, |offset| index + offset);
            blank(buffer, index..end);
            index = end;
        } else if byte == b'/' && next == Some(b'*') {
            let mut depth = 1_usize;
            let mut cursor = index + 2;
            while cursor < length && depth > 0 {
                if buffer[cursor] == b'/' && buffer.get(cursor + 1) == Some(&b'*') {
                    depth += 1;
                    cursor += 2;
                } else if buffer[cursor] == b'*' && buffer.get(cursor + 1) == Some(&b'/') {
                    depth -= 1;
                    cursor += 2;
                } else {
                    cursor += 1;
                }
            }
            let cursor = cursor.min(length);
            blank(buffer, index..cursor);
            index = cursor;
        } else if byte == b'r' && matches!(next, Some(b'#') | Some(b'"')) {
            let mut cursor = index + 1;
            let mut hashes = 0_usize;
            while buffer.get(cursor) == Some(&b'#') {
                hashes += 1;
                cursor += 1;
            }
            if buffer.get(cursor) != Some(&b'"') {
                // A raw identifier such as `r#type`, not a raw string.
                index += 1;
                continue;
            }
            cursor += 1;
            let end = loop {
                let Some(offset) = buffer[cursor..].iter().position(|byte| *byte == b'"') else {
                    break length;
                };
                let close = cursor + offset + 1;
                if close + hashes <= length
                    && buffer[close..close + hashes]
                        .iter()
                        .all(|byte| *byte == b'#')
                {
                    break close + hashes;
                }
                cursor = close;
            };
            blank(buffer, index..end);
            index = end;
        } else if byte == b'"' {
            let mut cursor = index + 1;
            let end = loop {
                match buffer.get(cursor) {
                    None => break length,
                    Some(b'\\') => cursor += 2,
                    Some(b'"') => break cursor + 1,
                    Some(_) => cursor += 1,
                }
            };
            blank(buffer, index..end);
            index = end;
        } else if byte == b'\'' {
            // Distinguish a char literal from a lifetime.
            let end = if buffer.get(index + 1) == Some(&b'\\') {
                let mut cursor = index + 2;
                while cursor < length && buffer[cursor] != b'\'' {
                    cursor += 1;
                }
                cursor + 1
            } else if buffer.get(index + 2) == Some(&b'\'') {
                index + 3
            } else {
                index += 1;
                continue;
            };
            let end = end.min(length);
            blank(buffer, index..end);
            index = end;
        } else {
            index += 1;
        }
    }
}

/// Blanks every `#[cfg(test)]` item. Test-only references are out of scope:
/// the invariant is about what the shipped engine does, and test doubles
/// legitimately reach across planes.
///
/// Only the exact `#[cfg(test)]` form is treated as test-only.
/// `#[cfg(any(test, feature = "storage-benches"))]` compiles in a real build
/// and is therefore production for this purpose.
fn blank_test_items(buffer: &mut [u8]) {
    const ATTRIBUTE: &[u8] = b"#[cfg(test)]";
    let length = buffer.len();
    let mut index = 0;
    while index < length {
        if !buffer[index..].starts_with(ATTRIBUTE) {
            index += 1;
            continue;
        }
        let mut cursor = index + ATTRIBUTE.len();
        let mut nesting = 0_i32;
        let end = loop {
            let Some(byte) = buffer.get(cursor).copied() else {
                break length;
            };
            match byte {
                b'(' | b'[' => nesting += 1,
                b')' | b']' => nesting -= 1,
                // `const X: [u8; 4] = …;` puts a semicolon inside brackets, so
                // only a top-level one ends the item.
                b';' if nesting == 0 => break cursor + 1,
                b'{' if nesting == 0 => {
                    let mut depth = 1_usize;
                    let mut scan = cursor + 1;
                    while scan < length && depth > 0 {
                        match buffer[scan] {
                            b'{' => depth += 1,
                            b'}' => depth -= 1,
                            _ => {}
                        }
                        scan += 1;
                    }
                    break scan;
                }
                _ => {}
            }
            cursor += 1;
        };
        blank(buffer, index..end);
        index = end;
    }
}

/// Rewrites a source file so that only production code remains scannable.
fn production_source(source: &str) -> String {
    let mut buffer = source.as_bytes().to_vec();
    blank_comments_and_literals(&mut buffer);
    blank_test_items(&mut buffer);
    String::from_utf8(buffer).expect("blanking preserves UTF-8")
}

/// Every `.rs` file under `src`, as `(top-level module, display path, source)`.
fn engine_sources() -> Vec<(String, String, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut sources = Vec::new();
    let mut pending = vec![root.clone()];
    while let Some(directory) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&directory) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            if path.extension().is_none_or(|extension| extension != "rs") {
                continue;
            }
            let Ok(relative) = path.strip_prefix(&root) else {
                continue;
            };
            let Some(first) = relative.components().next() else {
                continue;
            };
            let module = std::path::Path::new(first.as_os_str())
                .file_stem()
                .expect("source path component has a stem")
                .to_string_lossy()
                .into_owned();
            let Ok(source) = std::fs::read_to_string(&path) else {
                continue;
            };
            sources.push((module, relative.display().to_string(), source));
        }
    }
    assert!(!sources.is_empty(), "engine sources should be readable");
    sources
}

/// Every production `crate::<module>` reference, excluding self-references.
fn production_references(known: &std::collections::BTreeSet<String>) -> Vec<Reference> {
    const PREFIX: &[u8] = b"crate";
    let mut references = Vec::new();
    for (module, file, source) in engine_sources() {
        let production = production_source(&source);
        let bytes = production.as_bytes();
        let mut index = 0;
        while index + PREFIX.len() < bytes.len() {
            if !bytes[index..].starts_with(PREFIX) {
                index += 1;
                continue;
            }
            let preceded_by_identifier =
                index > 0 && (bytes[index - 1].is_ascii_alphanumeric() || bytes[index - 1] == b'_');
            if preceded_by_identifier {
                index += PREFIX.len();
                continue;
            }
            let mut cursor = index + PREFIX.len();
            while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
                cursor += 1;
            }
            if !bytes[cursor..].starts_with(b"::") {
                index += PREFIX.len();
                continue;
            }
            cursor += 2;
            while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
                cursor += 1;
            }
            let start = cursor;
            while bytes
                .get(cursor)
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
            {
                cursor += 1;
            }
            let target = &production[start..cursor];
            if known.contains(target) && target != module {
                references.push(Reference {
                    from: module.clone(),
                    to: target.to_owned(),
                    file: file.clone(),
                    line: bytes[..start].iter().filter(|byte| **byte == b'\n').count() + 1,
                });
            }
            index = cursor.max(index + PREFIX.len());
        }
    }
    references
}

fn declared_modules() -> std::collections::BTreeSet<String> {
    MODULE_LAYERS
        .iter()
        .flat_map(|layer| layer.iter())
        .map(|module| (*module).to_owned())
        .chain(
            UNLAYERED_MODULES
                .iter()
                .map(|(module, _)| (*module).to_owned()),
        )
        .collect()
}

/// A module that is neither layered nor explicitly excused is a hole in the
/// guard, so a newly added module cannot slip past it unnoticed.
#[test]
fn every_top_level_module_is_accounted_for() {
    let declared = declared_modules();
    let mut on_disk = std::collections::BTreeSet::new();
    for (module, _, _) in engine_sources() {
        on_disk.insert(module);
    }
    let undeclared = on_disk.difference(&declared).cloned().collect::<Vec<_>>();
    assert!(
        undeclared.is_empty(),
        "top-level modules are neither in MODULE_LAYERS nor in UNLAYERED_MODULES: {}",
        undeclared.join(", "),
    );
    let stale = declared.difference(&on_disk).cloned().collect::<Vec<_>>();
    assert!(
        stale.is_empty(),
        "MODULE_LAYERS/UNLAYERED_MODULES name modules that no longer exist: {}",
        stale.join(", "),
    );
}

#[test]
fn no_module_is_declared_twice() {
    let mut seen = std::collections::BTreeSet::new();
    for module in MODULE_LAYERS
        .iter()
        .flat_map(|layer| layer.iter())
        .chain(UNLAYERED_MODULES.iter().map(|(module, _)| module))
    {
        assert!(
            seen.insert(*module),
            "`{module}` is declared more than once across MODULE_LAYERS/UNLAYERED_MODULES",
        );
    }
}

/// The guard itself: no layered module may reference its own layer or above.
///
/// The canonical case is `tracked_state` → `hot_state`. Nine such references
/// existed before the layering was declared; the cache-may-read-canonical
/// direction is only an invariant while this test is green.
#[test]
fn no_module_references_a_higher_layer() {
    let known = declared_modules();
    let mut violations = Vec::new();
    for reference in production_references(&known) {
        let (Some(from), Some(to)) = (layer_of(&reference.from), layer_of(&reference.to)) else {
            continue;
        };
        if to < from {
            continue;
        }
        if ALLOWED_UPWARD_EDGES
            .iter()
            .any(|(allowed_from, allowed_to, _)| {
                *allowed_from == reference.from && *allowed_to == reference.to
            })
        {
            continue;
        }
        violations.push(format!(
            "{}:{} — `{}` (layer {from}) references `{}` (layer {to})",
            reference.file, reference.line, reference.from, reference.to,
        ));
    }
    violations.sort();
    violations.dedup();
    assert!(
        violations.is_empty(),
        "module layering violations ({} distinct):\n  {}\n\nEither move the code down to the layer \
         that owns it, or — if the reference is genuinely correct — add it to \
         ALLOWED_UPWARD_EDGES with a reason.",
        violations.len(),
        violations.join("\n  "),
    );
}

/// Guards the guard: if the scanner silently stopped finding anything, the
/// layering test would pass for the wrong reason.
#[test]
fn the_scanner_finds_the_references_it_is_supposed_to() {
    let known = declared_modules();
    let references = production_references(&known);
    assert!(
        references.len() > 500,
        "expected the engine to contain many cross-module references, found {}",
        references.len(),
    );
    assert!(
        references
            .iter()
            .any(|reference| reference.from == "hot_state" && reference.to == "tracked_state"),
        "the hot plane is supposed to read canonical state; the scanner missed it",
    );
    assert!(
        !references
            .iter()
            .any(|reference| reference.from == "tracked_state" && reference.to == "hot_state"),
        "canonical state must not read the hot plane",
    );
}
