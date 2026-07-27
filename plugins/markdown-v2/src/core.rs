use crate::markdown_file::{
    ParsedMarkdown, parse_file, parse_file_with_literal_fast_path, parse_markdown_source,
    render_tree,
};
use crate::model::{
    InlineContent, InlineNode, NodeKind, NodeSnapshot, NodeTree, Projection, parse_inline_payload,
    replace_column_ids, semantic_payload,
};
use base64::Engine;
use lix_order_key::OrderKey;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::sync::Arc;

pub(crate) const PARSED_ROOT_ID: &str = "parsed-markdown-root";
pub const NODE_SCHEMA_KEY: &str = crate::schemas::NODE_SCHEMA_KEY;
const LEXICAL_FALLBACK_FIELD: &str = "lexical_fallback_base64";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PluginError {
    InvalidInput(String),
    Internal(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct File {
    pub filename: Option<String>,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityState {
    pub entity_pk: Vec<String>,
    pub schema_key: String,
    pub snapshot_content: String,
    pub metadata: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DetectedChange {
    pub entity_pk: Vec<String>,
    pub schema_key: String,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct IdNamespace {
    high: u64,
    low: u64,
}

impl IdNamespace {
    pub const fn from_halves(high: u64, low: u64) -> Self {
        Self { high, low }
    }

    /// Reconstructs the core's compact namespace from one canonical ID minted
    /// by the opaque public API namespace.
    pub fn from_generated_id(id: &str) -> Result<Self, String> {
        let mut decoded = [0_u8; 24];
        let decoded_len = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode_slice(id.as_bytes(), &mut decoded)
            .map_err(|_| "plugin API generated an invalid Markdown identity".to_owned())?;
        if decoded_len != decoded.len() {
            return Err("plugin API generated an invalid Markdown identity".to_owned());
        }
        let high = u64::from_be_bytes(
            decoded[..8]
                .try_into()
                .map_err(|_| "plugin API generated an invalid Markdown identity".to_owned())?,
        );
        let low = u64::from_be_bytes(
            decoded[8..16]
                .try_into()
                .map_err(|_| "plugin API generated an invalid Markdown identity".to_owned())?,
        );
        Ok(Self::from_halves(high, low))
    }
}

#[derive(Debug)]
struct IdAllocator {
    namespace: IdNamespace,
    ordinal: u64,
}

impl IdAllocator {
    fn new(namespace: IdNamespace) -> Self {
        Self {
            namespace,
            ordinal: 0,
        }
    }

    fn next(&mut self) -> String {
        let mut bytes = [0_u8; 24];
        bytes[..8].copy_from_slice(&self.namespace.high.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.namespace.low.to_be_bytes());
        bytes[16..].copy_from_slice(&self.ordinal.to_be_bytes());
        self.ordinal = self
            .ordinal
            .checked_add(1)
            .expect("one Markdown transition cannot allocate more than u64::MAX IDs");
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    }
}

type SubtreeHash = u64;

#[derive(Default)]
struct SubtreeHashes {
    by_address: HashMap<usize, SubtreeHash>,
}

impl SubtreeHashes {
    fn from_tree(tree: &NodeTree) -> Self {
        fn visit(tree: &NodeTree, output: &mut SubtreeHashes) -> SubtreeHash {
            let mut hasher = DefaultHasher::new();
            tree.node.content_signature().hash(&mut hasher);
            tree.children.len().hash(&mut hasher);
            for child in &tree.children {
                visit(child, output).hash(&mut hasher);
            }
            let hash = hasher.finish();
            output.by_address.insert(tree_address(tree), hash);
            hash
        }

        let mut output = Self::default();
        visit(tree, &mut output);
        output
    }

    fn get(&self, tree: &NodeTree) -> SubtreeHash {
        *self
            .by_address
            .get(&tree_address(tree))
            .expect("Markdown subtree hash must be precomputed")
    }
}

fn tree_address(tree: &NodeTree) -> usize {
    std::ptr::from_ref(tree).addr()
}

#[derive(Clone, Copy, Debug)]
pub struct MarkdownPlugin;

impl MarkdownPlugin {
    pub fn detect_changes(
        state: Vec<EntityState>,
        file: File,
    ) -> Result<Vec<DetectedChange>, PluginError> {
        Self::detect_changes_with_namespace(state, file, IdNamespace::default())
    }

    pub fn detect_changes_with_namespace(
        state: Vec<EntityState>,
        file: File,
        namespace: IdNamespace,
    ) -> Result<Vec<DetectedChange>, PluginError> {
        let before = Projection::from_entity_state(state.into_iter())?;
        let mut after = parse_file(&file)?;
        retain_noncanonical_source(&mut after, &file.data)?;
        detect_changes_for_markdown(&before, after, namespace)
    }

    pub fn render(state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
        let projection = Projection::from_entity_state(state.into_iter())?;
        let root = projection.to_tree()?;
        render_tree_with_lexical_fallback(&root)
    }
}

fn retain_noncanonical_source(
    parsed: &mut ParsedMarkdown,
    source: &[u8],
) -> Result<(), PluginError> {
    let canonical = parsed.canonical_render.take().ok_or_else(|| {
        PluginError::Internal(
            "stable Markdown parse is missing its canonical render for lexical fallback".into(),
        )
    })?;
    if canonical == source {
        return Ok(());
    }
    let format = parsed.root.node.format.as_object_mut().ok_or_else(|| {
        PluginError::Internal("Markdown document format must be an object".into())
    })?;
    format.insert(
        LEXICAL_FALLBACK_FIELD.to_owned(),
        serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(source)),
    );
    Ok(())
}

fn render_tree_with_lexical_fallback(root: &NodeTree) -> Result<Vec<u8>, PluginError> {
    let canonical = render_tree(root)?;
    let Some(encoded) = root
        .node
        .format
        .get(LEXICAL_FALLBACK_FIELD)
        .and_then(serde_json::Value::as_str)
    else {
        return Ok(canonical);
    };
    let Ok(raw) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
        return Ok(canonical);
    };
    let Ok(parsed_raw) = parse_file(&File {
        filename: None,
        data: raw.clone(),
    }) else {
        return Ok(canonical);
    };
    let Ok(raw_canonical) = render_tree(&parsed_raw.root) else {
        return Ok(canonical);
    };
    if raw_canonical == canonical {
        Ok(raw)
    } else {
        Ok(canonical)
    }
}

fn detect_changes_for_markdown(
    before: &Projection,
    mut after: ParsedMarkdown,
    namespace: IdNamespace,
) -> Result<Vec<DetectedChange>, PluginError> {
    let generated_ids = collect_generated_ids(&after.root);
    let before_root = if before.nodes_by_id.is_empty() {
        None
    } else {
        Some(before.to_tree()?)
    };
    let mut replacements = BTreeMap::new();
    if let Some(before_root) = &before_root {
        let old_hashes = SubtreeHashes::from_tree(before_root);
        let new_hashes = SubtreeHashes::from_tree(&after.root);
        let mut global_subtrees = HashMap::<SubtreeHash, Vec<&NodeTree>>::new();
        collect_subtrees(before_root, &old_hashes, &mut global_subtrees);
        let mut new_signature_counts = HashMap::<SubtreeHash, usize>::new();
        collect_signature_counts(&after.root, &new_hashes, &mut new_signature_counts);
        let mut used_ids = BTreeSet::from([before_root.node.id.clone()]);
        let mut has_fresh_subtrees = false;
        reconcile_node(
            before_root,
            &mut after.root,
            None,
            &mut replacements,
            &global_subtrees,
            &new_signature_counts,
            &old_hashes,
            &new_hashes,
            &mut used_ids,
            &mut has_fresh_subtrees,
        )?;
        if has_fresh_subtrees {
            adopt_unique_global_moves(
                &mut after.root,
                &global_subtrees,
                &new_signature_counts,
                &old_hashes,
                &new_hashes,
                &mut used_ids,
                &mut replacements,
            )?;
        }
    } else {
        initialize_subtree(&mut after.root, None)?;
    }

    let mut allocator = IdAllocator::new(namespace);
    allocate_generated_ids(
        &mut after.root,
        &generated_ids,
        &mut allocator,
        &mut replacements,
    );
    after.root.visit_mut(&mut |node| {
        if let Some(parent_id) = &mut node.parent_id
            && let Some(replacement) = replacements.get(parent_id)
        {
            *parent_id = replacement.clone();
        }
        replace_column_ids(&mut node.payload, &replacements);
    });
    let after_nodes = flatten_tree(&after.root);
    diff_projections(before, &after_nodes)
}

fn collect_generated_ids(root: &NodeTree) -> BTreeSet<String> {
    fn collect_value_ids(value: &serde_json::Value, output: &mut BTreeSet<String>) {
        match value {
            serde_json::Value::Object(object) => {
                if let Some(serde_json::Value::String(id)) = object.get("id") {
                    output.insert(id.clone());
                }
                for child in object.values() {
                    collect_value_ids(child, output);
                }
            }
            serde_json::Value::Array(array) => {
                for child in array {
                    collect_value_ids(child, output);
                }
            }
            _ => {}
        }
    }

    fn visit(tree: &NodeTree, output: &mut BTreeSet<String>) {
        output.insert(tree.node.id.clone());
        collect_value_ids(&tree.node.payload, output);
        for child in &tree.children {
            visit(child, output);
        }
    }

    let mut output = BTreeSet::new();
    visit(root, &mut output);
    output
}

fn allocate_generated_ids(
    root: &mut NodeTree,
    generated: &BTreeSet<String>,
    allocator: &mut IdAllocator,
    replacements: &mut BTreeMap<String, String>,
) {
    fn collect_reserved_ids(
        tree: &NodeTree,
        generated: &BTreeSet<String>,
        reserved: &mut BTreeSet<String>,
    ) {
        fn collect_value(
            value: &serde_json::Value,
            generated: &BTreeSet<String>,
            reserved: &mut BTreeSet<String>,
        ) {
            match value {
                serde_json::Value::Object(object) => {
                    if let Some(serde_json::Value::String(id)) = object.get("id")
                        && !generated.contains(id)
                    {
                        reserved.insert(id.clone());
                    }
                    for child in object.values() {
                        collect_value(child, generated, reserved);
                    }
                }
                serde_json::Value::Array(array) => {
                    for child in array {
                        collect_value(child, generated, reserved);
                    }
                }
                _ => {}
            }
        }

        if !generated.contains(&tree.node.id) {
            reserved.insert(tree.node.id.clone());
        }
        collect_value(&tree.node.payload, generated, reserved);
        for child in &tree.children {
            collect_reserved_ids(child, generated, reserved);
        }
    }

    fn allocate_value_ids(
        value: &mut serde_json::Value,
        generated: &BTreeSet<String>,
        replacements: &BTreeMap<String, String>,
    ) {
        match value {
            serde_json::Value::Object(object) => {
                if let Some(serde_json::Value::String(id)) = object.get_mut("id")
                    && generated.contains(id)
                {
                    *id = replacements
                        .get(id)
                        .expect("every generated Markdown ID must have a replacement")
                        .clone();
                }
                for child in object.values_mut() {
                    allocate_value_ids(child, generated, replacements);
                }
            }
            serde_json::Value::Array(array) => {
                for child in array {
                    allocate_value_ids(child, generated, replacements);
                }
            }
            _ => {}
        }
    }

    fn visit(
        tree: &mut NodeTree,
        generated: &BTreeSet<String>,
        replacements: &BTreeMap<String, String>,
    ) {
        if generated.contains(&tree.node.id) {
            tree.node.id = replacements
                .get(&tree.node.id)
                .expect("every generated Markdown node ID must have a replacement")
                .clone();
        }
        allocate_value_ids(&mut tree.node.payload, generated, replacements);
        for child in &mut tree.children {
            visit(child, generated, replacements);
        }
    }

    let mut reserved = replacements.values().cloned().collect::<BTreeSet<_>>();
    collect_reserved_ids(root, generated, &mut reserved);
    for generated_id in generated {
        if replacements.contains_key(generated_id) {
            continue;
        }
        let replacement = loop {
            let candidate = allocator.next();
            if reserved.insert(candidate.clone()) {
                break candidate;
            }
        };
        replacements.insert(generated_id.clone(), replacement);
    }
    visit(root, generated, replacements);
}

fn reconcile_node(
    old: &NodeTree,
    new: &mut NodeTree,
    parent_id: Option<&str>,
    replacements: &mut BTreeMap<String, String>,
    global_subtrees: &HashMap<SubtreeHash, Vec<&NodeTree>>,
    new_signature_counts: &HashMap<SubtreeHash, usize>,
    old_hashes: &SubtreeHashes,
    new_hashes: &SubtreeHashes,
    used_ids: &mut BTreeSet<String>,
    has_fresh_subtrees: &mut bool,
) -> Result<(), PluginError> {
    let generated_id = new.node.id.clone();
    new.node.id.clone_from(&old.node.id);
    if generated_id != new.node.id {
        replacements.insert(generated_id, new.node.id.clone());
    }
    new.node.parent_id = parent_id.map(str::to_string);
    new.node.order_key.clone_from(&old.node.order_key);
    reconcile_inline_payload(old, new)?;
    reconcile_children(
        old,
        new,
        replacements,
        global_subtrees,
        new_signature_counts,
        old_hashes,
        new_hashes,
        used_ids,
        has_fresh_subtrees,
    )
}

fn reconcile_children(
    old: &NodeTree,
    new: &mut NodeTree,
    replacements: &mut BTreeMap<String, String>,
    global_subtrees: &HashMap<SubtreeHash, Vec<&NodeTree>>,
    new_signature_counts: &HashMap<SubtreeHash, usize>,
    old_hashes: &SubtreeHashes,
    new_hashes: &SubtreeHashes,
    used_ids: &mut BTreeSet<String>,
    has_fresh_subtrees: &mut bool,
) -> Result<(), PluginError> {
    let mut old_for_new = vec![None; new.children.len()];
    let mut old_used = old
        .children
        .iter()
        .map(|child| used_ids.contains(&child.node.id))
        .collect::<Vec<_>>();
    if old.node.kind == NodeKind::Table && new.node.kind == NodeKind::Table {
        match_table_columns(old, new, &mut old_for_new, &mut old_used, used_ids);
    }
    for index in 0..new.children.len().min(old.children.len()) {
        if old_for_new[index].is_none()
            && !old_used[index]
            && new.children[index].subtree_signature() == old.children[index].subtree_signature()
        {
            old_for_new[index] = Some(index);
            old_used[index] = true;
            used_ids.insert(old.children[index].node.id.clone());
        }
    }
    let mut exact = HashMap::<String, Vec<usize>>::new();
    for (index, child) in old.children.iter().enumerate().rev() {
        if old_used[index] {
            continue;
        }
        exact
            .entry(child.subtree_signature())
            .or_default()
            .push(index);
    }
    for (new_index, child) in new.children.iter().enumerate() {
        if old_for_new[new_index].is_some() {
            continue;
        }
        let signature = child.subtree_signature();
        let Some(indices) = exact.get_mut(&signature) else {
            continue;
        };
        while let Some(old_index) = indices.pop() {
            if !old_used[old_index] {
                old_for_new[new_index] = Some(old_index);
                old_used[old_index] = true;
                used_ids.insert(old.children[old_index].node.id.clone());
                break;
            }
        }
    }

    let mut search_start = 0;
    for (new_index, child) in new.children.iter().enumerate() {
        if old_for_new[new_index].is_some() {
            continue;
        }
        let matching = (search_start..old.children.len())
            .chain(0..search_start)
            .find(|old_index| {
                !old_used[*old_index]
                    && node_kinds_are_identity_compatible(
                        old.children[*old_index].node.kind,
                        child.node.kind,
                    )
                    && !has_available_unique_global_match(
                        child,
                        global_subtrees,
                        new_signature_counts,
                        old_hashes,
                        new_hashes,
                        used_ids,
                    )
            });
        if let Some(old_index) = matching {
            old_for_new[new_index] = Some(old_index);
            old_used[old_index] = true;
            used_ids.insert(old.children[old_index].node.id.clone());
            search_start = old_index.saturating_add(1);
        }
    }

    let parent_id = new.node.id.clone();
    for (new_index, child) in new.children.iter_mut().enumerate() {
        if let Some(old_index) = old_for_new[new_index] {
            reconcile_node(
                &old.children[old_index],
                child,
                Some(&parent_id),
                replacements,
                global_subtrees,
                new_signature_counts,
                old_hashes,
                new_hashes,
                used_ids,
                has_fresh_subtrees,
            )?;
        }
    }
    for (new_index, child) in new.children.iter_mut().enumerate() {
        if old_for_new[new_index].is_some() {
            continue;
        }
        *has_fresh_subtrees = true;
        initialize_subtree(child, Some(&parent_id))?;
    }
    if new.node.kind == NodeKind::TableRow {
        preserve_table_cell_order_keys(&mut new.children, &old_for_new, &old.children)
    } else {
        assign_sibling_order_keys(&mut new.children, &old_for_new, &old.children)
    }
}

fn node_kinds_are_identity_compatible(old: NodeKind, new: NodeKind) -> bool {
    old == new
        || matches!(
            (old, new),
            (NodeKind::Paragraph, NodeKind::Heading) | (NodeKind::Heading, NodeKind::Paragraph)
        )
}

fn has_available_unique_global_match(
    tree: &NodeTree,
    global_subtrees: &HashMap<SubtreeHash, Vec<&NodeTree>>,
    new_signature_counts: &HashMap<SubtreeHash, usize>,
    old_hashes: &SubtreeHashes,
    new_hashes: &SubtreeHashes,
    used_ids: &BTreeSet<String>,
) -> bool {
    let signature = new_hashes.get(tree);
    global_subtrees.get(&signature).is_some_and(|candidates| {
        candidates.len() == 1
            && new_signature_counts.get(&signature) == Some(&1)
            && subtree_ids_are_available(candidates[0], used_ids)
            && old_hashes.get(candidates[0]) == signature
            && candidates[0].subtree_signature() == tree.subtree_signature()
    })
}

fn match_table_columns(
    old: &NodeTree,
    new: &NodeTree,
    old_for_new: &mut [Option<usize>],
    old_used: &mut [bool],
    used_ids: &mut BTreeSet<String>,
) {
    let mut old_by_signature = HashMap::<String, Vec<usize>>::new();
    for (index, column) in old.children.iter().enumerate() {
        if column.node.kind == NodeKind::TableColumn && !old_used[index] {
            old_by_signature
                .entry(table_column_signature(old, column))
                .or_default()
                .push(index);
        }
    }
    let mut new_counts = HashMap::<String, usize>::new();
    for column in &new.children {
        if column.node.kind == NodeKind::TableColumn {
            *new_counts
                .entry(table_column_signature(new, column))
                .or_default() += 1;
        }
    }
    for (new_index, column) in new.children.iter().enumerate() {
        if column.node.kind != NodeKind::TableColumn || old_for_new[new_index].is_some() {
            continue;
        }
        let signature = table_column_signature(new, column);
        let Some(old_indices) = old_by_signature.get(&signature) else {
            continue;
        };
        if old_indices.len() != 1 || new_counts.get(&signature) != Some(&1) {
            continue;
        }
        let old_index = old_indices[0];
        old_for_new[new_index] = Some(old_index);
        old_used[old_index] = true;
        used_ids.insert(old.children[old_index].node.id.clone());
    }
}

fn table_column_signature(table: &NodeTree, column: &NodeTree) -> String {
    let mut cells = Vec::new();
    for row in table
        .children
        .iter()
        .filter(|child| child.node.kind == NodeKind::TableRow)
    {
        let cell = row.children.iter().find(|cell| {
            cell.node
                .payload
                .get("column_id")
                .and_then(serde_json::Value::as_str)
                == Some(column.node.id.as_str())
        });
        cells.push(cell.map(NodeTree::subtree_signature));
    }
    serde_json::to_string(&(column.node.content_signature(), cells))
        .expect("table column signature must serialize")
}

fn preserve_table_cell_order_keys(
    children: &mut [NodeTree],
    old_for_new: &[Option<usize>],
    old_children: &[NodeTree],
) -> Result<(), PluginError> {
    let fresh_count = old_for_new.iter().filter(|old| old.is_none()).count();
    let mut fresh = OrderKey::evenly_between(None, None, fresh_count)
        .map_err(PluginError::Internal)?
        .into_iter();
    for (index, child) in children.iter_mut().enumerate() {
        child.node.order_key = old_for_new[index].map_or_else(
            || {
                Some(
                    fresh
                        .next()
                        .expect("fresh table cell order key must exist")
                        .to_snapshot_string(),
                )
            },
            |old_index| old_children[old_index].node.order_key.clone(),
        );
    }
    Ok(())
}

fn collect_subtrees<'a>(
    tree: &'a NodeTree,
    hashes: &SubtreeHashes,
    output: &mut HashMap<SubtreeHash, Vec<&'a NodeTree>>,
) {
    if tree.node.kind != NodeKind::Document {
        output.entry(hashes.get(tree)).or_default().push(tree);
    }
    for child in &tree.children {
        collect_subtrees(child, hashes, output);
    }
}

fn adopt_unique_global_moves(
    tree: &mut NodeTree,
    global_subtrees: &HashMap<SubtreeHash, Vec<&NodeTree>>,
    new_signature_counts: &HashMap<SubtreeHash, usize>,
    old_hashes: &SubtreeHashes,
    new_hashes: &SubtreeHashes,
    used_ids: &mut BTreeSet<String>,
    replacements: &mut BTreeMap<String, String>,
) -> Result<(), PluginError> {
    for child in &mut tree.children {
        let signature = new_hashes.get(child);
        let candidate = global_subtrees.get(&signature).and_then(|candidates| {
            (candidates.len() == 1 && new_signature_counts.get(&signature) == Some(&1))
                .then_some(candidates[0])
        });
        if let Some(candidate) = candidate.filter(|candidate| {
            subtree_ids_are_available(candidate, used_ids)
                && !used_ids.contains(&child.node.id)
                && old_hashes.get(candidate) == signature
                && candidate.subtree_signature() == child.subtree_signature()
        }) {
            adopt_exact_subtree(candidate, child, used_ids, replacements)?;
        } else {
            adopt_unique_global_moves(
                child,
                global_subtrees,
                new_signature_counts,
                old_hashes,
                new_hashes,
                used_ids,
                replacements,
            )?;
        }
    }
    Ok(())
}

fn subtree_ids_are_available(tree: &NodeTree, used_ids: &BTreeSet<String>) -> bool {
    !used_ids.contains(&tree.node.id)
        && tree
            .children
            .iter()
            .all(|child| subtree_ids_are_available(child, used_ids))
}

fn adopt_exact_subtree(
    old: &NodeTree,
    new: &mut NodeTree,
    used_ids: &mut BTreeSet<String>,
    replacements: &mut BTreeMap<String, String>,
) -> Result<(), PluginError> {
    let generated_id = new.node.id.clone();
    let parent_id = new.node.parent_id.clone();
    let order_key = new.node.order_key.clone();
    new.node.id.clone_from(&old.node.id);
    new.node.parent_id = parent_id;
    new.node.order_key = order_key;
    used_ids.insert(new.node.id.clone());
    if generated_id != new.node.id {
        replacements.insert(generated_id, new.node.id.clone());
    }
    reconcile_inline_payload(old, new)?;

    if old.children.len() != new.children.len() {
        return Err(PluginError::Internal(
            "equal Markdown subtree signatures had different child counts".to_string(),
        ));
    }
    let parent_id = new.node.id.clone();
    for (old_child, new_child) in old.children.iter().zip(&mut new.children) {
        adopt_exact_subtree_child(old_child, new_child, &parent_id, used_ids, replacements)?;
    }
    Ok(())
}

fn adopt_exact_subtree_child(
    old: &NodeTree,
    new: &mut NodeTree,
    parent_id: &str,
    used_ids: &mut BTreeSet<String>,
    replacements: &mut BTreeMap<String, String>,
) -> Result<(), PluginError> {
    let generated_id = new.node.id.clone();
    new.node.id.clone_from(&old.node.id);
    new.node.parent_id = Some(parent_id.to_string());
    new.node.order_key.clone_from(&old.node.order_key);
    used_ids.insert(new.node.id.clone());
    if generated_id != new.node.id {
        replacements.insert(generated_id, new.node.id.clone());
    }
    reconcile_inline_payload(old, new)?;
    if old.children.len() != new.children.len() {
        return Err(PluginError::Internal(
            "equal Markdown subtree signatures had different child counts".to_string(),
        ));
    }
    let parent_id = new.node.id.clone();
    for (old_child, new_child) in old.children.iter().zip(&mut new.children) {
        adopt_exact_subtree_child(old_child, new_child, &parent_id, used_ids, replacements)?;
    }
    Ok(())
}

fn collect_signature_counts(
    tree: &NodeTree,
    hashes: &SubtreeHashes,
    output: &mut HashMap<SubtreeHash, usize>,
) {
    if tree.node.kind != NodeKind::Document {
        *output.entry(hashes.get(tree)).or_default() += 1;
    }
    for child in &tree.children {
        collect_signature_counts(child, hashes, output);
    }
}

fn initialize_subtree(tree: &mut NodeTree, parent_id: Option<&str>) -> Result<(), PluginError> {
    tree.node.parent_id = parent_id.map(str::to_string);
    if tree.node.kind == NodeKind::Document {
        tree.node.order_key = None;
    }
    let parent_id = tree.node.id.clone();
    for child in &mut tree.children {
        initialize_subtree(child, Some(&parent_id))?;
    }
    assign_fresh_order_keys(&mut tree.children)
}

fn assign_fresh_order_keys(children: &mut [NodeTree]) -> Result<(), PluginError> {
    let keys =
        OrderKey::evenly_between(None, None, children.len()).map_err(PluginError::Internal)?;
    for (child, key) in children.iter_mut().zip(keys) {
        child.node.order_key = Some(key.to_snapshot_string());
    }
    Ok(())
}

fn assign_sibling_order_keys(
    children: &mut [NodeTree],
    old_for_new: &[Option<usize>],
    old_children: &[NodeTree],
) -> Result<(), PluginError> {
    if children.is_empty() {
        return Ok(());
    }
    let old_keys = old_for_new
        .iter()
        .map(|old_index| {
            old_index
                .map(|index| old_children[index].node.parsed_order_key())
                .transpose()
                .map(Option::flatten)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|message| PluginError::InvalidInput(format!("invalid order_key: {message}")))?;
    let keep = longest_increasing_key_subsequence(&old_keys, children);
    let mut previous = None::<OrderKey>;
    let mut pending = Vec::new();

    for index in 0..children.len() {
        if keep[index] {
            let next = old_keys[index]
                .as_ref()
                .expect("kept order key must belong to an existing child");
            if flush_order_keys(&mut pending, &mut previous, Some(next), children).is_err() {
                return assign_fresh_order_keys(children);
            }
            children[index].node.order_key = Some(next.to_snapshot_string());
            previous = Some(next.clone());
        } else {
            pending.push(index);
        }
    }
    if flush_order_keys(&mut pending, &mut previous, None, children).is_err() {
        return assign_fresh_order_keys(children);
    }
    Ok(())
}

fn flush_order_keys(
    pending: &mut Vec<usize>,
    previous: &mut Option<OrderKey>,
    next: Option<&OrderKey>,
    children: &mut [NodeTree],
) -> Result<(), PluginError> {
    if pending.is_empty() {
        return Ok(());
    }
    let keys = OrderKey::evenly_between(previous.as_ref(), next, pending.len())
        .map_err(PluginError::Internal)?;
    for (index, key) in pending.drain(..).zip(keys) {
        children[index].node.order_key = Some(key.to_snapshot_string());
        *previous = Some(key);
    }
    Ok(())
}

fn longest_increasing_key_subsequence(
    keys: &[Option<OrderKey>],
    children: &[NodeTree],
) -> Vec<bool> {
    let mut keep = vec![false; keys.len()];
    let mut pile_tops = Vec::<usize>::new();
    let mut predecessors = vec![None; keys.len()];
    for (index, key) in keys.iter().enumerate() {
        if key.is_none() {
            continue;
        }
        let pile = pile_tops.partition_point(|top| {
            compare_sibling_positions(*top, index, keys, children) == Ordering::Less
        });
        if pile > 0 {
            predecessors[index] = Some(pile_tops[pile - 1]);
        }
        if pile == pile_tops.len() {
            pile_tops.push(index);
        } else if compare_sibling_positions(pile_tops[pile], index, keys, children)
            == Ordering::Greater
        {
            pile_tops[pile] = index;
        }
    }
    let Some(mut current) = pile_tops.last().copied() else {
        return keep;
    };
    loop {
        keep[current] = true;
        let Some(previous) = predecessors[current] else {
            break;
        };
        current = previous;
    }
    keep
}

fn compare_sibling_positions(
    left: usize,
    right: usize,
    keys: &[Option<OrderKey>],
    children: &[NodeTree],
) -> Ordering {
    keys[left]
        .as_ref()
        .expect("left sibling position has key")
        .cmp(
            keys[right]
                .as_ref()
                .expect("right sibling position has key"),
        )
        .then_with(|| children[left].node.id.cmp(&children[right].node.id))
}

fn reconcile_inline_payload(old: &NodeTree, new: &mut NodeTree) -> Result<(), PluginError> {
    if !matches!(
        new.node.kind,
        NodeKind::Paragraph | NodeKind::Heading | NodeKind::TableCell
    ) {
        return Ok(());
    }
    let old_inlines = parse_inline_payload(&old.node.payload).map_err(PluginError::InvalidInput)?;
    let mut new_inlines =
        parse_inline_payload(&new.node.payload).map_err(PluginError::InvalidInput)?;
    reconcile_inline_sequence(&old_inlines, &mut new_inlines);
    new.node.payload["inline"] = serde_json::to_value(new_inlines).map_err(|error| {
        PluginError::Internal(format!("failed to serialize inline AST: {error}"))
    })?;
    Ok(())
}

fn reconcile_inline_sequence(old: &[InlineNode], new: &mut [InlineNode]) {
    if old.is_empty() || new.is_empty() {
        return;
    }
    if let ([old_inline], [new_inline]) = (old, &mut *new) {
        if old_inline.signature() == new_inline.signature()
            || old_inline.kind_tag() == new_inline.kind_tag()
        {
            new_inline.id.clone_from(&old_inline.id);
            if let (Some(old_children), Some(new_children)) =
                (old_inline.children(), new_inline.children_mut())
            {
                reconcile_inline_sequence(old_children, new_children);
            }
        }
        return;
    }

    let old_signatures = old.iter().map(InlineNode::signature).collect::<Vec<_>>();
    let new_signatures = new.iter().map(InlineNode::signature).collect::<Vec<_>>();
    let mut old_for_new = vec![None; new.len()];
    let mut old_used = vec![false; old.len()];
    // Unique, non-crossing atoms (including plain text) establish context before
    // repeated atoms are matched inside each gap. A fully identical run still
    // has no knowable insertion position, so it uses deterministic local order
    // while retaining every reusable old ID exactly once.
    let anchors = unique_non_crossing_inline_anchors(&old_signatures, &new_signatures);
    for &(old_index, new_index) in &anchors {
        old_for_new[new_index] = Some(old_index);
        old_used[old_index] = true;
    }

    for_each_inline_gap(
        old.len(),
        new.len(),
        &anchors,
        |old_start, old_end, new_start, new_end| {
            match_exact_inlines_in_range(
                &old_signatures,
                &new_signatures,
                old_start,
                old_end,
                new_start,
                new_end,
                &mut old_for_new,
                &mut old_used,
            );
        },
    );

    match_exact_inlines_in_range(
        &old_signatures,
        &new_signatures,
        0,
        old.len(),
        0,
        new.len(),
        &mut old_for_new,
        &mut old_used,
    );

    for_each_inline_gap(
        old.len(),
        new.len(),
        &anchors,
        |old_start, old_end, new_start, new_end| {
            match_compatible_inlines_in_range(
                old,
                new,
                old_start,
                old_end,
                new_start,
                new_end,
                &mut old_for_new,
                &mut old_used,
            );
        },
    );

    match_compatible_inlines_in_range(
        old,
        new,
        0,
        old.len(),
        0,
        new.len(),
        &mut old_for_new,
        &mut old_used,
    );

    for (new_index, inline) in new.iter_mut().enumerate() {
        let Some(old_index) = old_for_new[new_index] else {
            continue;
        };
        inline.id.clone_from(&old[old_index].id);
        if let (Some(old_children), Some(new_children)) =
            (old[old_index].children(), inline.children_mut())
        {
            reconcile_inline_sequence(old_children, new_children);
        }
    }
}

fn unique_non_crossing_inline_anchors(
    old_signatures: &[String],
    new_signatures: &[String],
) -> Vec<(usize, usize)> {
    let mut old_positions = HashMap::<&str, Vec<usize>>::new();
    let mut new_positions = HashMap::<&str, Vec<usize>>::new();
    for (index, signature) in old_signatures.iter().enumerate() {
        old_positions.entry(signature).or_default().push(index);
    }
    for (index, signature) in new_signatures.iter().enumerate() {
        new_positions.entry(signature).or_default().push(index);
    }

    let mut candidates = old_positions
        .into_iter()
        .filter_map(|(signature, old_indices)| {
            let new_indices = new_positions.get(&signature)?;
            (old_indices.len() == 1 && new_indices.len() == 1)
                .then_some((old_indices[0], new_indices[0]))
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|(old_index, _)| *old_index);
    if candidates.is_empty() {
        return Vec::new();
    }

    let mut pile_tops = Vec::<usize>::new();
    let mut predecessors = vec![None; candidates.len()];
    for (index, &(_, new_index)) in candidates.iter().enumerate() {
        let pile = pile_tops.partition_point(|top| candidates[*top].1 < new_index);
        if pile > 0 {
            predecessors[index] = Some(pile_tops[pile - 1]);
        }
        if pile == pile_tops.len() {
            pile_tops.push(index);
        } else {
            pile_tops[pile] = index;
        }
    }

    let mut anchors = Vec::with_capacity(pile_tops.len());
    let mut current = *pile_tops.last().expect("inline anchor pile must exist");
    loop {
        anchors.push(candidates[current]);
        let Some(previous) = predecessors[current] else {
            break;
        };
        current = previous;
    }
    anchors.reverse();
    anchors
}

fn for_each_inline_gap(
    old_len: usize,
    new_len: usize,
    anchors: &[(usize, usize)],
    mut visitor: impl FnMut(usize, usize, usize, usize),
) {
    let mut old_start = 0;
    let mut new_start = 0;
    for &(old_anchor, new_anchor) in anchors {
        visitor(old_start, old_anchor, new_start, new_anchor);
        old_start = old_anchor + 1;
        new_start = new_anchor + 1;
    }
    visitor(old_start, old_len, new_start, new_len);
}

#[allow(clippy::too_many_arguments)]
fn match_exact_inlines_in_range(
    old_signatures: &[String],
    new_signatures: &[String],
    old_start: usize,
    old_end: usize,
    new_start: usize,
    new_end: usize,
    old_for_new: &mut [Option<usize>],
    old_used: &mut [bool],
) {
    let paired = (old_end - old_start).min(new_end - new_start);
    for offset in 0..paired {
        let old_index = old_start + offset;
        let new_index = new_start + offset;
        if old_for_new[new_index].is_none()
            && !old_used[old_index]
            && new_signatures[new_index] == old_signatures[old_index]
        {
            old_for_new[new_index] = Some(old_index);
            old_used[old_index] = true;
        }
    }

    let mut exact = HashMap::<&str, Vec<usize>>::new();
    for old_index in (old_start..old_end).rev() {
        if old_used[old_index] {
            continue;
        }
        exact
            .entry(old_signatures[old_index].as_str())
            .or_default()
            .push(old_index);
    }
    for new_index in new_start..new_end {
        if old_for_new[new_index].is_some() {
            continue;
        }
        if let Some(indices) = exact.get_mut(new_signatures[new_index].as_str())
            && let Some(old_index) = indices.pop()
        {
            old_for_new[new_index] = Some(old_index);
            old_used[old_index] = true;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn match_compatible_inlines_in_range(
    old: &[InlineNode],
    new: &[InlineNode],
    old_start: usize,
    old_end: usize,
    new_start: usize,
    new_end: usize,
    old_for_new: &mut [Option<usize>],
    old_used: &mut [bool],
) {
    let mut available = HashMap::<&'static str, VecDeque<usize>>::new();
    for old_index in old_start..old_end {
        if !old_used[old_index] {
            available
                .entry(old[old_index].kind_tag())
                .or_default()
                .push_back(old_index);
        }
    }
    for new_index in new_start..new_end {
        if old_for_new[new_index].is_some() {
            continue;
        }
        if let Some(old_index) = available
            .get_mut(new[new_index].kind_tag())
            .and_then(VecDeque::pop_front)
        {
            old_for_new[new_index] = Some(old_index);
            old_used[old_index] = true;
        }
    }
}

fn flatten_tree(root: &NodeTree) -> BTreeMap<String, NodeSnapshot> {
    fn visit(tree: &NodeTree, output: &mut BTreeMap<String, NodeSnapshot>) {
        output.insert(tree.node.id.clone(), tree.node.clone());
        for child in &tree.children {
            visit(child, output);
        }
    }
    let mut output = BTreeMap::new();
    visit(root, &mut output);
    output
}

fn diff_projections(
    before: &Projection,
    after: &BTreeMap<String, NodeSnapshot>,
) -> Result<Vec<DetectedChange>, PluginError> {
    let mut changes = Vec::new();
    for id in before.nodes_by_id.keys() {
        if !after.contains_key(id) {
            changes.push(DetectedChange {
                entity_pk: vec![id.clone()],
                schema_key: NODE_SCHEMA_KEY.to_string(),
                snapshot_content: None,
                metadata: None,
            });
        }
    }
    for (id, node) in after {
        if before.nodes_by_id.get(id) == Some(node) {
            continue;
        }
        let snapshot_content = serde_json::to_string(node).map_err(|error| {
            PluginError::Internal(format!("failed to serialize Markdown node '{id}': {error}"))
        })?;
        changes.push(DetectedChange {
            entity_pk: vec![id.clone()],
            schema_key: NODE_SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content),
            metadata: change_metadata(before.nodes_by_id.get(id), node),
        });
    }
    Ok(changes)
}

fn change_metadata(before: Option<&NodeSnapshot>, after: &NodeSnapshot) -> Option<String> {
    let before = before?;
    if before.id == after.id
        && before.kind == after.kind
        && before.parent_id == after.parent_id
        && before.order_key == after.order_key
        && semantic_payload(&before.payload) == semantic_payload(&after.payload)
        && (before.payload != after.payload || before.format != after.format)
    {
        Some(r#"{"impact":"format"}"#.to_string())
    } else {
        None
    }
}

fn single_entity_pk(mut entity_pk: Vec<String>) -> Result<String, PluginError> {
    if entity_pk.len() != 1 {
        return Err(PluginError::InvalidInput(format!(
            "expected single-component entity_pk, got {} components",
            entity_pk.len()
        )));
    }
    Ok(entity_pk.remove(0))
}

impl Projection {
    fn from_entity_state(rows: impl Iterator<Item = EntityState>) -> Result<Self, PluginError> {
        let mut nodes_by_id = BTreeMap::new();
        for row in rows {
            if row.schema_key != NODE_SCHEMA_KEY {
                continue;
            }
            let entity_pk = single_entity_pk(row.entity_pk)?;
            let node: NodeSnapshot =
                serde_json::from_str(&row.snapshot_content).map_err(|error| {
                    PluginError::InvalidInput(format!(
                        "invalid Markdown node snapshot for entity_pk '{entity_pk}': {error}"
                    ))
                })?;
            if node.id != entity_pk {
                return Err(PluginError::InvalidInput(format!(
                    "Markdown node snapshot id '{}' does not match entity_pk '{entity_pk}'",
                    node.id
                )));
            }
            if nodes_by_id.insert(entity_pk.clone(), node).is_some() {
                return Err(PluginError::InvalidInput(format!(
                    "duplicate Markdown node entity_pk '{entity_pk}'"
                )));
            }
        }
        Ok(Self { nodes_by_id })
    }

    fn to_tree(&self) -> Result<NodeTree, PluginError> {
        let roots = self
            .nodes_by_id
            .values()
            .filter(|node| node.kind == NodeKind::Document)
            .collect::<Vec<_>>();
        let [root] = roots.as_slice() else {
            return Err(PluginError::InvalidInput(format!(
                "Markdown state must contain exactly one document root, found {}",
                roots.len()
            )));
        };
        if root.parent_id.is_some() || root.order_key.is_some() {
            return Err(PluginError::InvalidInput(
                "Markdown document root must have kind=document, parent_id=null, order_key=null"
                    .to_string(),
            ));
        }
        let mut children_by_parent = BTreeMap::<String, Vec<&NodeSnapshot>>::new();
        for node in self.nodes_by_id.values() {
            if node.id == root.id {
                continue;
            }
            let parent = node.parent_id.as_ref().ok_or_else(|| {
                PluginError::InvalidInput(format!(
                    "Markdown node '{}' is missing parent_id",
                    node.id
                ))
            })?;
            if node.order_key.is_none() {
                return Err(PluginError::InvalidInput(format!(
                    "Markdown node '{}' is missing order_key",
                    node.id
                )));
            }
            if let Err(message) = node.parsed_order_key() {
                return Err(PluginError::InvalidInput(format!(
                    "Markdown node '{}' has invalid order_key: {message}",
                    node.id
                )));
            }
            if !self.nodes_by_id.contains_key(parent) {
                return Err(PluginError::InvalidInput(format!(
                    "Markdown node '{}' references missing parent '{parent}'",
                    node.id
                )));
            }
            children_by_parent
                .entry(parent.clone())
                .or_default()
                .push(node);
        }
        for children in children_by_parent.values_mut() {
            children.sort_by(|left, right| {
                let left_key = left.parsed_order_key();
                let right_key = right.parsed_order_key();
                match (left_key, right_key) {
                    (Ok(Some(left_key)), Ok(Some(right_key))) => left_key
                        .cmp(&right_key)
                        .then_with(|| left.id.cmp(&right.id)),
                    _ => left.id.cmp(&right.id),
                }
            });
        }
        let mut visiting = BTreeSet::new();
        let mut visited = BTreeSet::new();
        let tree = build_tree(root, &children_by_parent, &mut visiting, &mut visited)?;
        if visited.len() != self.nodes_by_id.len() {
            let unreachable = self
                .nodes_by_id
                .keys()
                .find(|id| !visited.contains(*id))
                .expect("unreachable node must exist");
            return Err(PluginError::InvalidInput(format!(
                "Markdown node '{unreachable}' is not reachable from the document root"
            )));
        }
        Ok(tree)
    }
}

fn build_tree(
    node: &NodeSnapshot,
    children_by_parent: &BTreeMap<String, Vec<&NodeSnapshot>>,
    visiting: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
) -> Result<NodeTree, PluginError> {
    if !visiting.insert(node.id.clone()) {
        return Err(PluginError::InvalidInput(format!(
            "Markdown graph contains a cycle at node '{}'",
            node.id
        )));
    }
    let children = children_by_parent
        .get(&node.id)
        .into_iter()
        .flatten()
        .map(|child| build_tree(child, children_by_parent, visiting, visited))
        .collect::<Result<Vec<_>, _>>()?;
    visiting.remove(&node.id);
    visited.insert(node.id.clone());
    Ok(NodeTree {
        node: node.clone(),
        children,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangeEffect {
    Content,
    FormatOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityRecord {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
    pub snapshot: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityChange {
    pub schema_key: String,
    pub entity_pk: Vec<String>,
    pub snapshot: Option<Vec<u8>>,
    pub effect: ChangeEffect,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InputSplice<'a> {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: &'a [u8],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ByteEdit {
    pub offset: u64,
    pub delete_len: u64,
    pub insert: Arc<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct Document {
    bytes: PersistentBytes,
    tree: PersistentTree,
    top_level_ranges: Arc<Vec<Range<usize>>>,
}

/// Persistent top-level Markdown tree. The common paragraph-edit path changes
/// one top-level node, so retaining the parsed base avoids cloning thousands
/// of unrelated `NodeSnapshot` JSON values on every keystroke.
#[derive(Clone, Debug)]
struct PersistentTree {
    base: Arc<NodeTree>,
    top_level_overrides: Arc<BTreeMap<usize, NodeSnapshot>>,
}

impl PersistentTree {
    fn new(root: NodeTree) -> Self {
        Self {
            base: Arc::new(root),
            top_level_overrides: Arc::new(BTreeMap::new()),
        }
    }

    fn root_node(&self) -> &NodeSnapshot {
        &self.base.node
    }

    fn top_level_node(&self, index: usize) -> Option<&NodeSnapshot> {
        self.top_level_overrides
            .get(&index)
            .or_else(|| self.base.children.get(index).map(|child| &child.node))
    }

    fn replace_top_level(&self, index: usize, node: NodeSnapshot) -> Self {
        let mut overrides = self.top_level_overrides.as_ref().clone();
        overrides.insert(index, node);
        Self {
            base: Arc::clone(&self.base),
            top_level_overrides: Arc::new(overrides),
        }
    }

    fn materialize(&self) -> NodeTree {
        let mut root = self.base.as_ref().clone();
        for (&index, node) in self.top_level_overrides.iter() {
            root.children[index].node.clone_from(node);
        }
        root
    }
}

/// Immutable piece table for accepted Markdown bytes. Sparse successors share
/// all unchanged allocations and own only their inserted bytes.
#[derive(Clone, Debug)]
struct PersistentBytes {
    pieces: Arc<Vec<BytePiece>>,
    len: usize,
}

#[derive(Clone, Debug)]
struct BytePiece {
    bytes: Arc<Vec<u8>>,
    start: usize,
    len: usize,
}

impl PersistentBytes {
    fn from_vec(bytes: Vec<u8>) -> Self {
        let len = bytes.len();
        let pieces = if bytes.is_empty() {
            Vec::new()
        } else {
            vec![BytePiece {
                bytes: Arc::new(bytes),
                start: 0,
                len,
            }]
        };
        Self {
            pieces: Arc::new(pieces),
            len,
        }
    }

    fn materialize(&self) -> Vec<u8> {
        self.range(0..self.len)
            .expect("complete Markdown byte range is valid")
    }

    fn byte(&self, offset: usize) -> Option<u8> {
        if offset >= self.len {
            return None;
        }
        let mut logical_start = 0usize;
        for piece in self.pieces.iter() {
            let logical_end = logical_start + piece.len;
            if offset < logical_end {
                return Some(piece.bytes[piece.start + offset - logical_start]);
            }
            logical_start = logical_end;
        }
        None
    }

    fn range(&self, range: Range<usize>) -> Result<Vec<u8>, PluginError> {
        if !(range.start..=self.len).contains(&range.end) {
            return Err(PluginError::InvalidInput(
                "Markdown byte range is out of bounds".to_owned(),
            ));
        }
        let mut output = Vec::with_capacity(range.end - range.start);
        let mut logical_start = 0usize;
        for piece in self.pieces.iter() {
            let logical_end = logical_start + piece.len;
            let selected_start = range.start.max(logical_start);
            let selected_end = range.end.min(logical_end);
            if selected_start < selected_end {
                let piece_start = piece.start + selected_start - logical_start;
                let piece_end = piece.start + selected_end - logical_start;
                output.extend_from_slice(&piece.bytes[piece_start..piece_end]);
            }
            if logical_end >= range.end {
                break;
            }
            logical_start = logical_end;
        }
        Ok(output)
    }

    fn append_piece_range(
        &self,
        range: Range<usize>,
        output: &mut Vec<BytePiece>,
    ) -> Result<(), PluginError> {
        if !(range.start..=self.len).contains(&range.end) {
            return Err(PluginError::InvalidInput(
                "Markdown byte range is out of bounds".to_owned(),
            ));
        }
        let mut logical_start = 0usize;
        for piece in self.pieces.iter() {
            let logical_end = logical_start + piece.len;
            let selected_start = range.start.max(logical_start);
            let selected_end = range.end.min(logical_end);
            if selected_start < selected_end {
                push_byte_piece(
                    output,
                    BytePiece {
                        bytes: Arc::clone(&piece.bytes),
                        start: piece.start + selected_start - logical_start,
                        len: selected_end - selected_start,
                    },
                );
            }
            if logical_end >= range.end {
                break;
            }
            logical_start = logical_end;
        }
        Ok(())
    }

    fn splice(&self, splices: &[InputSplice<'_>]) -> Result<Self, PluginError> {
        let mut pieces = Vec::with_capacity(self.pieces.len() + splices.len() * 2);
        let mut cursor = 0usize;
        let mut result_len = self.len;
        let mut previous_offset = None;
        for splice in splices {
            let offset = usize::try_from(splice.offset).map_err(|_| {
                PluginError::InvalidInput("Markdown splice offset is too large".into())
            })?;
            let delete_len = usize::try_from(splice.delete_len).map_err(|_| {
                PluginError::InvalidInput("Markdown splice delete length is too large".into())
            })?;
            let end = offset.checked_add(delete_len).ok_or_else(|| {
                PluginError::InvalidInput("Markdown splice range overflow".to_owned())
            })?;
            if previous_offset.is_some_and(|previous| offset <= previous)
                || offset < cursor
                || end > self.len
            {
                return Err(PluginError::InvalidInput(
                    "Markdown splice starts must be strictly increasing, non-overlapping, and within the base"
                        .to_owned(),
                ));
            }
            self.append_piece_range(cursor..offset, &mut pieces)?;
            if !splice.insert.is_empty() {
                push_byte_piece(
                    &mut pieces,
                    BytePiece {
                        bytes: Arc::new(splice.insert.to_vec()),
                        start: 0,
                        len: splice.insert.len(),
                    },
                );
            }
            result_len = result_len
                .checked_sub(delete_len)
                .and_then(|len| len.checked_add(splice.insert.len()))
                .ok_or_else(|| {
                    PluginError::InvalidInput("Markdown splice size overflow".to_owned())
                })?;
            cursor = end;
            previous_offset = Some(offset);
        }
        self.append_piece_range(cursor..self.len, &mut pieces)?;
        Ok(Self {
            pieces: Arc::new(pieces),
            len: result_len,
        })
    }
}

fn push_byte_piece(output: &mut Vec<BytePiece>, piece: BytePiece) {
    if piece.len == 0 {
        return;
    }
    if let Some(previous) = output.last_mut()
        && Arc::ptr_eq(&previous.bytes, &piece.bytes)
        && previous.start + previous.len == piece.start
    {
        previous.len += piece.len;
        return;
    }
    output.push(piece);
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WireNodeSnapshot {
    id: String,
    kind: NodeKind,
    parent_id: Option<String>,
    order_key: Option<String>,
    payload_json: String,
    format_json: String,
}

impl WireNodeSnapshot {
    fn from_logical(node: NodeSnapshot) -> Result<Self, PluginError> {
        let payload_json = serde_json::to_string(&node.payload).map_err(|error| {
            PluginError::Internal(format!("failed to encode Markdown node payload: {error}"))
        })?;
        let format_json = serde_json::to_string(&node.format).map_err(|error| {
            PluginError::Internal(format!("failed to encode Markdown node format: {error}"))
        })?;
        Ok(Self {
            id: node.id,
            kind: node.kind,
            parent_id: node.parent_id,
            order_key: node.order_key,
            payload_json,
            format_json,
        })
    }

    fn into_logical(self) -> Result<NodeSnapshot, PluginError> {
        let payload: serde_json::Value =
            serde_json::from_str(&self.payload_json).map_err(|error| {
                PluginError::InvalidInput(format!(
                    "Markdown node payload_json is not valid JSON: {error}"
                ))
            })?;
        let format: serde_json::Value =
            serde_json::from_str(&self.format_json).map_err(|error| {
                PluginError::InvalidInput(format!(
                    "Markdown node format_json is not valid JSON: {error}"
                ))
            })?;
        if !payload.is_object() || !format.is_object() {
            return Err(PluginError::InvalidInput(
                "Markdown payload_json and format_json must encode JSON objects".to_owned(),
            ));
        }
        Ok(NodeSnapshot {
            id: self.id,
            kind: self.kind,
            parent_id: self.parent_id,
            order_key: self.order_key,
            payload,
            format,
        })
    }
}

fn logical_to_wire(snapshot: &str) -> Result<Vec<u8>, PluginError> {
    let logical = serde_json::from_str(snapshot).map_err(|error| {
        PluginError::Internal(format!(
            "generated Markdown snapshot is not valid JSON: {error}"
        ))
    })?;
    let wire = WireNodeSnapshot::from_logical(logical)?;
    serde_json::to_vec(&wire).map_err(|error| {
        PluginError::Internal(format!("failed to encode Markdown wire snapshot: {error}"))
    })
}

fn wire_to_logical(snapshot: &[u8]) -> Result<String, PluginError> {
    let wire: WireNodeSnapshot = serde_json::from_slice(snapshot).map_err(|error| {
        PluginError::InvalidInput(format!("invalid Markdown wire snapshot: {error}"))
    })?;
    serde_json::to_string(&wire.into_logical()?).map_err(|error| {
        PluginError::Internal(format!(
            "failed to encode logical Markdown snapshot: {error}"
        ))
    })
}

fn detected_to_entity_change(change: DetectedChange) -> Result<EntityChange, PluginError> {
    let effect = if change.metadata.as_deref() == Some(r#"{"impact":"format"}"#) {
        ChangeEffect::FormatOnly
    } else {
        ChangeEffect::Content
    };
    Ok(EntityChange {
        schema_key: change.schema_key,
        entity_pk: change.entity_pk,
        snapshot: change
            .snapshot_content
            .as_deref()
            .map(logical_to_wire)
            .transpose()?,
        effect,
    })
}

fn apply_detected_changes(state: &[EntityState], changes: &[DetectedChange]) -> Vec<EntityState> {
    let mut rows = state
        .iter()
        .cloned()
        .map(|row| ((row.schema_key.clone(), row.entity_pk.clone()), row))
        .collect::<BTreeMap<_, _>>();
    for change in changes {
        let key = (change.schema_key.clone(), change.entity_pk.clone());
        if let Some(snapshot_content) = &change.snapshot_content {
            rows.insert(
                key,
                EntityState {
                    entity_pk: change.entity_pk.clone(),
                    schema_key: change.schema_key.clone(),
                    snapshot_content: snapshot_content.clone(),
                    metadata: change.metadata.clone(),
                },
            );
        } else {
            rows.remove(&key);
        }
    }
    rows.into_values().collect()
}

fn entity_change_to_detected(change: EntityChange) -> Result<DetectedChange, PluginError> {
    if change.schema_key != NODE_SCHEMA_KEY {
        return Err(PluginError::InvalidInput(format!(
            "Markdown transition received foreign schema '{}'",
            change.schema_key
        )));
    }
    Ok(DetectedChange {
        schema_key: change.schema_key,
        entity_pk: change.entity_pk,
        snapshot_content: change
            .snapshot
            .as_deref()
            .map(wire_to_logical)
            .transpose()?,
        metadata: (change.effect == ChangeEffect::FormatOnly)
            .then(|| r#"{"impact":"format"}"#.to_owned()),
    })
}

fn minimal_byte_edit(before: &[u8], after: Vec<u8>) -> Vec<ByteEdit> {
    if before == after {
        return Vec::new();
    }
    let prefix = before
        .iter()
        .zip(&after)
        .take_while(|(left, right)| left == right)
        .count();
    let suffix_cap = before.len().min(after.len()).saturating_sub(prefix);
    let suffix = before
        .iter()
        .rev()
        .zip(after.iter().rev())
        .take(suffix_cap)
        .take_while(|(left, right)| left == right)
        .count();
    vec![ByteEdit {
        offset: u64::try_from(prefix).expect("usize fits u64"),
        delete_len: u64::try_from(before.len() - prefix - suffix).expect("usize fits u64"),
        insert: Arc::new(after[prefix..after.len() - suffix].to_vec()),
    }]
}

fn simple_top_level_ranges(root: &NodeTree, bytes: &[u8]) -> Vec<Range<usize>> {
    if root.node.format.get(LEXICAL_FALLBACK_FIELD).is_some() {
        return Vec::new();
    }

    let mut ranges = Vec::new();
    let mut start = 0usize;
    while start < bytes.len() {
        let separator = bytes[start..]
            .windows(2)
            .position(|window| window == b"\n\n")
            .map(|relative| start + relative);
        let mut end = separator.unwrap_or(bytes.len());
        if separator.is_none() && end > start && bytes[end - 1] == b'\n' {
            end -= 1;
        }
        if end == start {
            return Vec::new();
        }
        ranges.push(start..end);
        let Some(separator) = separator else {
            break;
        };
        start = separator + 2;
    }
    if ranges.len() != root.children.len() {
        return Vec::new();
    }
    ranges
}

fn projection_after_detected_changes(
    root: &NodeTree,
    changes: &[DetectedChange],
) -> Result<Projection, PluginError> {
    let mut nodes_by_id = flatten_tree(root);
    for change in changes {
        let id = change.entity_pk.first().ok_or_else(|| {
            PluginError::InvalidInput("Markdown entity_pk must contain one id".into())
        })?;
        if let Some(snapshot_content) = &change.snapshot_content {
            let node: NodeSnapshot = serde_json::from_str(snapshot_content).map_err(|error| {
                PluginError::InvalidInput(format!(
                    "invalid Markdown node snapshot for '{id}': {error}"
                ))
            })?;
            nodes_by_id.insert(id.clone(), node);
        } else {
            nodes_by_id.remove(id);
        }
    }
    Ok(Projection { nodes_by_id })
}

impl Document {
    /// Resolves one concurrent Markdown entity change without depending on a
    /// hydrated document.
    ///
    /// `a` and `b` are already in the engine's stable merge order.
    /// The default is therefore an exact canonical `b` fallback. For the common case of
    /// one plain-text paragraph changed by two disjoint single-span edits, we
    /// can safely retain both edits instead. Everything structural, formatted,
    /// deleted, malformed, or overlapping deliberately takes the b
    /// snapshot unchanged.
    pub fn resolve_entity_conflict(
        base: Option<Vec<u8>>,
        a: Option<Vec<u8>>,
        b: Option<Vec<u8>>,
    ) -> Option<Vec<u8>> {
        let b = b?;
        let (Some(base), Some(a)) = (base, a) else {
            return Some(b);
        };

        Some(merge_plain_paragraph_snapshots(&base, &a, &b).unwrap_or(b))
    }

    pub fn open_file(
        bytes: Vec<u8>,
        path: Option<&str>,
        namespace: IdNamespace,
    ) -> Result<(Self, Vec<EntityChange>), PluginError> {
        Self::open_file_with_literal_fast_path(bytes, path, namespace, true)
    }

    #[cfg(test)]
    pub(crate) fn open_file_forced_canonical_fallback(
        bytes: Vec<u8>,
        path: Option<&str>,
        namespace: IdNamespace,
    ) -> Result<(Self, Vec<EntityChange>), PluginError> {
        Self::open_file_with_literal_fast_path(bytes, path, namespace, false)
    }

    fn open_file_with_literal_fast_path(
        bytes: Vec<u8>,
        path: Option<&str>,
        namespace: IdNamespace,
        allow_literal_fast_path: bool,
    ) -> Result<(Self, Vec<EntityChange>), PluginError> {
        let file = File {
            filename: path.map(ToOwned::to_owned),
            data: bytes.clone(),
        };
        let mut parsed = parse_file_with_literal_fast_path(&file, allow_literal_fast_path)?;
        retain_noncanonical_source(&mut parsed, &file.data)?;
        let top_level_ranges = parsed.top_level_ranges.clone();
        let detected = detect_changes_for_markdown(&Projection::default(), parsed, namespace)?;
        let state = apply_detected_changes(&[], &detected);
        let root = Projection::from_entity_state(state.iter().cloned())?.to_tree()?;
        let changes = detected
            .into_iter()
            .map(detected_to_entity_change)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((
            Self {
                bytes: PersistentBytes::from_vec(bytes),
                tree: PersistentTree::new(root),
                top_level_ranges: Arc::new(top_level_ranges),
            },
            changes,
        ))
    }

    pub fn open_entities(
        records: Vec<EntityRecord>,
        accepted: Option<Vec<u8>>,
    ) -> Result<(Self, Vec<ByteEdit>), PluginError> {
        let state = records
            .into_iter()
            .map(|record| {
                if record.schema_key != NODE_SCHEMA_KEY {
                    return Err(PluginError::InvalidInput(format!(
                        "Markdown import received foreign schema '{}'",
                        record.schema_key
                    )));
                }
                Ok(EntityState {
                    entity_pk: record.entity_pk,
                    schema_key: record.schema_key,
                    snapshot_content: wire_to_logical(&record.snapshot)?,
                    metadata: None,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let projection = Projection::from_entity_state(state.iter().cloned())?;
        let root = projection.to_tree()?;
        let restored = accepted.is_some();
        let bytes = match accepted {
            Some(bytes) => bytes,
            None => render_tree_with_lexical_fallback(&root)?,
        };
        let top_level_ranges = simple_top_level_ranges(&root, &bytes);
        let edits = if restored || bytes.is_empty() {
            Vec::new()
        } else {
            vec![ByteEdit {
                offset: 0,
                delete_len: 0,
                insert: Arc::new(bytes.clone()),
            }]
        };
        Ok((
            Self {
                bytes: PersistentBytes::from_vec(bytes),
                tree: PersistentTree::new(root),
                top_level_ranges: Arc::new(top_level_ranges),
            },
            edits,
        ))
    }

    pub fn fork(&self) -> Self {
        self.clone()
    }

    pub fn file_changed(
        &self,
        splices: &[InputSplice<'_>],
        namespace: IdNamespace,
    ) -> Result<(Self, Vec<EntityChange>), PluginError> {
        let successor_bytes = self.bytes.splice(splices)?;
        let (detected, top_level_ranges, tree) = if let Some(incremental) =
            self.try_paragraph_replacement(splices, &successor_bytes, namespace)?
        {
            incremental
        } else {
            let bytes = successor_bytes.materialize();
            let file = File {
                filename: None,
                data: bytes,
            };
            let current_root = self.tree.materialize();
            let before = Projection {
                nodes_by_id: flatten_tree(&current_root),
            };
            let mut parsed = parse_file(&file)?;
            retain_noncanonical_source(&mut parsed, &file.data)?;
            let top_level_ranges = Arc::new(parsed.top_level_ranges.clone());
            let detected = detect_changes_for_markdown(&before, parsed, namespace)?;
            let root = projection_after_detected_changes(&current_root, &detected)?.to_tree()?;
            (detected, top_level_ranges, PersistentTree::new(root))
        };
        let changes = detected
            .into_iter()
            .map(detected_to_entity_change)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((
            Self {
                bytes: successor_bytes,
                tree,
                top_level_ranges,
            },
            changes,
        ))
    }

    pub fn entities_changed(
        &self,
        changes: Vec<EntityChange>,
    ) -> Result<(Self, Vec<ByteEdit>), PluginError> {
        let detected = changes
            .into_iter()
            .map(entity_change_to_detected)
            .collect::<Result<Vec<_>, _>>()?;
        if let Some((bytes, top_level_ranges, edits, tree)) =
            self.try_paragraph_entity_change(&detected)?
        {
            return Ok((
                Self {
                    bytes,
                    tree,
                    top_level_ranges,
                },
                edits,
            ));
        }
        let current_root = self.tree.materialize();
        let projection = projection_after_detected_changes(&current_root, &detected)?;
        let root = projection.to_tree()?;
        let bytes = render_tree_with_lexical_fallback(&root)?;
        let top_level_ranges = simple_top_level_ranges(&root, &bytes);
        let before = self.bytes.materialize();
        let edits = minimal_byte_edit(&before, bytes.clone());
        Ok((
            Self {
                bytes: PersistentBytes::from_vec(bytes),
                tree: PersistentTree::new(root),
                top_level_ranges: Arc::new(top_level_ranges),
            },
            edits,
        ))
    }

    fn try_paragraph_entity_change(
        &self,
        changes: &[DetectedChange],
    ) -> Result<
        Option<(
            PersistentBytes,
            Arc<Vec<Range<usize>>>,
            Vec<ByteEdit>,
            PersistentTree,
        )>,
        PluginError,
    > {
        let [change] = changes else {
            return Ok(None);
        };
        let Some(snapshot_content) = &change.snapshot_content else {
            return Ok(None);
        };
        if self
            .tree
            .root_node()
            .format
            .get(LEXICAL_FALLBACK_FIELD)
            .is_some()
        {
            return Ok(None);
        }
        let Some(block_index) = self
            .tree
            .base
            .children
            .iter()
            .position(|child| change.entity_pk == [child.node.id.clone()])
        else {
            return Ok(None);
        };
        let old = self
            .tree
            .top_level_node(block_index)
            .expect("base top-level Markdown node exists");
        let Some(range) = self.top_level_ranges.get(block_index) else {
            return Ok(None);
        };
        let new: NodeSnapshot = serde_json::from_str(snapshot_content).map_err(|error| {
            PluginError::InvalidInput(format!(
                "invalid Markdown paragraph snapshot for incremental rendering: {error}"
            ))
        })?;
        if old.kind != NodeKind::Paragraph
            || new.kind != NodeKind::Paragraph
            || new.id != old.id
            || new.parent_id != old.parent_id
            || new.order_key != old.order_key
        {
            return Ok(None);
        }

        let mut fragment_root = self.tree.root_node().clone();
        let format = fragment_root.format.as_object_mut().ok_or_else(|| {
            PluginError::Internal("Markdown document format must be an object".into())
        })?;
        format.insert("final_newline".to_owned(), serde_json::Value::Bool(false));
        let fragment = render_tree(&NodeTree {
            node: fragment_root,
            children: vec![NodeTree {
                node: new.clone(),
                children: Vec::new(),
            }],
        })?;
        let bytes = self.bytes.splice(&[InputSplice {
            offset: u64::try_from(range.start).expect("usize fits u64"),
            delete_len: u64::try_from(range.end - range.start).expect("usize fits u64"),
            insert: &fragment,
        }])?;

        let delta = isize::try_from(fragment.len()).expect("usize fits isize")
            - isize::try_from(range.end - range.start).expect("usize fits isize");
        let mut top_level_ranges = self.top_level_ranges.as_ref().clone();
        top_level_ranges[block_index].end = top_level_ranges[block_index].start + fragment.len();
        if delta != 0 {
            for following in &mut top_level_ranges[block_index + 1..] {
                following.start = following.start.checked_add_signed(delta).ok_or_else(|| {
                    PluginError::Internal("Markdown block range shift overflow".into())
                })?;
                following.end = following.end.checked_add_signed(delta).ok_or_else(|| {
                    PluginError::Internal("Markdown block range shift overflow".into())
                })?;
            }
        }
        let edits = vec![ByteEdit {
            offset: u64::try_from(range.start).expect("usize fits u64"),
            delete_len: u64::try_from(range.end - range.start).expect("usize fits u64"),
            insert: Arc::new(fragment),
        }];
        let successor_tree = self.tree.replace_top_level(block_index, new);
        Ok(Some((
            bytes,
            Arc::new(top_level_ranges),
            edits,
            successor_tree,
        )))
    }

    fn try_paragraph_replacement(
        &self,
        splices: &[InputSplice<'_>],
        bytes: &PersistentBytes,
        namespace: IdNamespace,
    ) -> Result<Option<(Vec<DetectedChange>, Arc<Vec<Range<usize>>>, PersistentTree)>, PluginError>
    {
        let [splice] = splices else {
            return Ok(None);
        };
        if splice.delete_len != 1
            || splice.insert.len() != 1
            || !splice.insert[0].is_ascii_alphanumeric()
        {
            return Ok(None);
        }
        let offset = usize::try_from(splice.offset)
            .map_err(|_| PluginError::InvalidInput("Markdown splice offset is too large".into()))?;
        if !self
            .bytes
            .byte(offset)
            .is_some_and(|byte| byte.is_ascii_alphanumeric())
        {
            return Ok(None);
        }
        let Some((block_index, range)) = self
            .top_level_ranges
            .iter()
            .enumerate()
            .find(|(_, range)| range.start <= offset && offset < range.end)
        else {
            return Ok(None);
        };

        if self
            .tree
            .root_node()
            .format
            .get(LEXICAL_FALLBACK_FIELD)
            .is_some()
            || self
                .tree
                .top_level_node(block_index)
                .is_none_or(|node| node.kind != NodeKind::Paragraph)
        {
            return Ok(None);
        }

        let fragment_bytes = bytes.range(range.clone())?;
        let fragment = std::str::from_utf8(&fragment_bytes).map_err(|error| {
            PluginError::InvalidInput(format!(
                "file.data must be valid UTF-8 for an incremental Markdown edit: {error}"
            ))
        })?;
        let mut replacement = parse_markdown_source(fragment)?;
        if replacement.root.children.len() != 1
            || replacement.root.children[0].node.kind != NodeKind::Paragraph
            || render_tree(&replacement.root)? != fragment.as_bytes()
        {
            return Ok(None);
        }

        let mut old = self.tree.base.children[block_index].clone();
        old.node.clone_from(
            self.tree
                .top_level_node(block_index)
                .expect("validated top-level paragraph exists"),
        );
        let mut new = replacement.root.children.remove(0);
        let generated_ids = collect_generated_ids(&new);
        let generated_node_id = new.node.id.clone();
        new.node.id.clone_from(&old.node.id);
        new.node.parent_id.clone_from(&old.node.parent_id);
        new.node.order_key.clone_from(&old.node.order_key);
        reconcile_inline_payload(&old, &mut new)?;

        let mut replacements = BTreeMap::from([(generated_node_id, new.node.id.clone())]);
        let mut allocator = IdAllocator::new(namespace);
        allocate_generated_ids(&mut new, &generated_ids, &mut allocator, &mut replacements);
        replace_column_ids(&mut new.node.payload, &replacements);
        let detected = if old.node == new.node {
            Vec::new()
        } else {
            let snapshot_content = serde_json::to_string(&new.node).map_err(|error| {
                PluginError::Internal(format!(
                    "failed to serialize Markdown node '{}': {error}",
                    new.node.id
                ))
            })?;
            vec![DetectedChange {
                entity_pk: vec![new.node.id.clone()],
                schema_key: NODE_SCHEMA_KEY.to_owned(),
                snapshot_content: Some(snapshot_content),
                metadata: change_metadata(Some(&old.node), &new.node),
            }]
        };
        let successor_tree = self.tree.replace_top_level(block_index, new.node.clone());
        Ok(Some((
            detected,
            Arc::clone(&self.top_level_ranges),
            successor_tree,
        )))
    }

    #[cfg(test)]
    pub(crate) fn accepted_bytes(&self) -> Vec<u8> {
        self.bytes.materialize()
    }
}

/// A single base-relative text replacement. Keeping this deliberately narrow
/// gives merge an easily auditable happy path: two insertions at opposite ends
/// of a word are independent, while a pair of arbitrary edit scripts is not.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TextReplacement {
    start: usize,
    end: usize,
    insert: Vec<char>,
}

fn merge_plain_paragraph_snapshots(
    base_snapshot: &[u8],
    a_snapshot: &[u8],
    b_snapshot: &[u8],
) -> Option<Vec<u8>> {
    let base = node_from_wire_snapshot(base_snapshot)?;
    let a = node_from_wire_snapshot(a_snapshot)?;
    let b = node_from_wire_snapshot(b_snapshot)?;

    if !same_plain_paragraph_shape(&base, &a) || !same_plain_paragraph_shape(&base, &b) {
        return None;
    }

    let base_text = single_text_value(&base)?;
    let a_text = single_text_value(&a)?;
    let b_text = single_text_value(&b)?;
    let merged = merge_disjoint_text_replacements(&base_text, &a_text, &b_text)?;

    let merged_node = replace_single_text(b, merged)?;
    node_to_wire_snapshot(&merged_node)
}

fn node_from_wire_snapshot(snapshot: &[u8]) -> Option<NodeSnapshot> {
    let logical = wire_to_logical(snapshot).ok()?;
    serde_json::from_str(&logical).ok()
}

fn node_to_wire_snapshot(node: &NodeSnapshot) -> Option<Vec<u8>> {
    let logical = serde_json::to_string(node).ok()?;
    logical_to_wire(&logical).ok()
}

fn same_plain_paragraph_shape(base: &NodeSnapshot, side: &NodeSnapshot) -> bool {
    base.kind == NodeKind::Paragraph
        && side.kind == NodeKind::Paragraph
        && base.id == side.id
        && base.parent_id == side.parent_id
        && base.order_key == side.order_key
        && base.format == side.format
}

fn single_text_value(node: &NodeSnapshot) -> Option<String> {
    let inlines = parse_inline_payload(&node.payload).ok()?;
    let [
        InlineNode {
            content: InlineContent::Text { value },
            ..
        },
    ] = inlines.as_slice()
    else {
        return None;
    };
    Some(value.clone())
}

fn replace_single_text(mut node: NodeSnapshot, value: String) -> Option<NodeSnapshot> {
    let mut inlines = parse_inline_payload(&node.payload).ok()?;
    let [
        InlineNode {
            content: InlineContent::Text { value: current },
            ..
        },
    ] = inlines.as_mut_slice()
    else {
        return None;
    };
    *current = value;
    node.payload["inline"] = serde_json::to_value(inlines).ok()?;
    Some(node)
}

fn merge_disjoint_text_replacements(base: &str, a: &str, b: &str) -> Option<String> {
    if a == b {
        return Some(b.to_owned());
    }
    if a == base {
        return Some(b.to_owned());
    }
    if b == base {
        return Some(a.to_owned());
    }

    let base = base.chars().collect::<Vec<_>>();
    let a = a.chars().collect::<Vec<_>>();
    let b = b.chars().collect::<Vec<_>>();
    let a_edit = single_text_replacement(&base, &a);
    let b_edit = single_text_replacement(&base, &b);

    if a_edit == b_edit {
        return chars_to_string(&base, &[a_edit]);
    }

    if a_edit.start == b_edit.start && a_edit.end == b_edit.end && a_edit.start == a_edit.end {
        // Concurrent inserts at one logical position have no destructive
        // overlap. Preserve both in the same canonical a-then-b order
        // regardless of which branch happened to initiate the merge.
        return chars_to_string(
            &base,
            &[TextReplacement {
                start: a_edit.start,
                end: a_edit.end,
                insert: a_edit
                    .insert
                    .iter()
                    .chain(&b_edit.insert)
                    .copied()
                    .collect(),
            }],
        );
    }

    let mut edits = [a_edit, b_edit];
    edits.sort_by_key(|edit| (edit.start, edit.end));
    if edits[0].end > edits[1].start {
        return None;
    }
    chars_to_string(&base, &edits)
}

fn single_text_replacement(base: &[char], side: &[char]) -> TextReplacement {
    let start = base
        .iter()
        .zip(side)
        .take_while(|(left, right)| left == right)
        .count();
    let suffix_cap = base.len().min(side.len()).saturating_sub(start);
    let suffix = base
        .iter()
        .rev()
        .zip(side.iter().rev())
        .take(suffix_cap)
        .take_while(|(left, right)| left == right)
        .count();
    TextReplacement {
        start,
        end: base.len() - suffix,
        insert: side[start..side.len() - suffix].to_vec(),
    }
}

fn chars_to_string(base: &[char], edits: &[TextReplacement]) -> Option<String> {
    let mut output = String::new();
    let mut cursor = 0;
    for edit in edits {
        if edit.start < cursor || edit.end < edit.start || edit.end > base.len() {
            return None;
        }
        output.extend(base[cursor..edit.start].iter());
        output.extend(edit.insert.iter());
        cursor = edit.end;
    }
    output.extend(base[cursor..].iter());
    Some(output)
}
