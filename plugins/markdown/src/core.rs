use crate::markdown_file::{
    ParsedMarkdown, parse_file, parse_file_with_literal_fast_path, parse_markdown_source,
    render_tree,
};
use crate::model::{
    InlineContent, InlineNode, NodeKind, NodeSnapshot, NodeTree, Projection, parse_inline_payload,
    replace_column_ids, semantic_payload,
};
use crate::order_key::OrderKey;
use base64::Engine;
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
const LEXICAL_SOURCE_REQUIRED_FIELD: &str = "lexical_source_required";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PluginError {
    InvalidInput(String),
    Internal(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct File {
    pub filename: Option<String>,
    pub content: Vec<u8>,
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
    low: u32,
}

impl IdNamespace {
    pub fn from_namespace_bytes(namespace: [u8; 12]) -> Self {
        Self {
            high: u64::from_be_bytes(namespace[..8].try_into().expect("eight bytes")),
            low: u32::from_be_bytes(namespace[8..].try_into().expect("four bytes")),
        }
    }

    pub const fn from_halves(high: u64, low: u32) -> Self {
        Self { high, low }
    }

    /// Reconstructs the core's compact namespace from one canonical ID minted
    /// by the opaque public API namespace.
    pub fn from_generated_id(id: &str) -> Result<Self, String> {
        let decoded = uuid::Uuid::parse_str(id)
            .map_err(|_| "plugin API generated an invalid Markdown identity".to_owned())?;
        let high = u64::from_be_bytes(
            decoded.as_bytes()[..8]
                .try_into()
                .map_err(|_| "plugin API generated an invalid Markdown identity".to_owned())?,
        );
        let low = u32::from_be_bytes(
            decoded.as_bytes()[8..12]
                .try_into()
                .map_err(|_| "plugin API generated an invalid Markdown identity".to_owned())?,
        );
        Ok(Self { high, low })
    }
}

#[derive(Debug)]
struct IdAllocator {
    namespace: IdNamespace,
    ordinal: u64,
}

impl IdAllocator {
    fn new(namespace: IdNamespace) -> Self {
        Self::with_ordinal(namespace, 0)
    }

    fn with_ordinal(namespace: IdNamespace, ordinal: u32) -> Self {
        Self {
            namespace,
            ordinal: u64::from(ordinal),
        }
    }

    fn next(&mut self) -> String {
        let ordinal = u32::try_from(self.ordinal)
            .expect("one Markdown transition cannot allocate more than u32::MAX nodes");
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&self.namespace.high.to_be_bytes());
        bytes[8..12].copy_from_slice(&self.namespace.low.to_be_bytes());
        bytes[12..].copy_from_slice(&ordinal.to_be_bytes());
        self.ordinal = self
            .ordinal
            .checked_add(1)
            .expect("Markdown allocation counter overflowed");
        uuid::Uuid::from_bytes(bytes).to_string()
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
        let before_root = if before.nodes_by_id.is_empty() {
            None
        } else {
            Some(before.to_tree()?)
        };
        let before_view = ProjectionView::from_projection(&before);
        let mut after = parse_file(&file)?;
        retain_noncanonical_source(&mut after, &file.content)?;
        let (changes, _) =
            detect_changes_for_markdown(&before_view, before_root.as_ref(), after, namespace, 0)?;
        Ok(changes)
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
    let format = parsed.root.node.format.as_object_mut().ok_or_else(|| {
        PluginError::Internal("Markdown document format must be an object".into())
    })?;
    if canonical == source {
        format.remove(LEXICAL_SOURCE_REQUIRED_FIELD);
        return Ok(());
    }
    format.insert(
        LEXICAL_FALLBACK_FIELD.to_owned(),
        serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(source)),
    );
    format.insert(
        LEXICAL_SOURCE_REQUIRED_FIELD.to_owned(),
        serde_json::Value::Bool(true),
    );
    Ok(())
}

fn render_tree_with_lexical_fallback(root: &NodeTree) -> Result<Vec<u8>, PluginError> {
    if let Some(encoded) = root
        .node
        .format
        .get(LEXICAL_FALLBACK_FIELD)
        .and_then(serde_json::Value::as_str)
    {
        return base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|error| {
                PluginError::InvalidInput(format!(
                    "Markdown lexical source fallback is not valid base64: {error}"
                ))
            });
    }
    if root
        .node
        .format
        .get(LEXICAL_SOURCE_REQUIRED_FIELD)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return Err(PluginError::InvalidInput(
            "Markdown restore requires accepted file bytes for this noncanonical source".into(),
        ));
    }
    render_tree(root)
}

fn detect_changes_for_markdown(
    before: &ProjectionView<'_>,
    before_root: Option<&NodeTree>,
    mut after: ParsedMarkdown,
    namespace: IdNamespace,
    minimum_ordinal: u32,
) -> Result<(Vec<DetectedChange>, NodeTree), PluginError> {
    let generated_ids = collect_generated_ids(&after.root);
    let mut replacements = BTreeMap::new();
    if let Some(before_root) = before_root {
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

    let mut allocator = IdAllocator::with_ordinal(namespace, minimum_ordinal);
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
    let changes = diff_tree(before, &after.root)?;
    Ok((changes, after.root))
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

fn diff_tree(
    before: &ProjectionView<'_>,
    after: &NodeTree,
) -> Result<Vec<DetectedChange>, PluginError> {
    let after = ProjectionView::from_tree(after);
    let mut changes = Vec::new();
    for id in before.nodes_by_id.keys().copied() {
        if !after.nodes_by_id.contains_key(id) {
            changes.push(DetectedChange {
                entity_pk: vec![id.to_owned()],
                schema_key: NODE_SCHEMA_KEY.to_string(),
                snapshot_content: None,
                metadata: None,
            });
        }
    }
    for (id, node) in &after.nodes_by_id {
        if before.nodes_by_id.get(id).copied() == Some(*node) {
            continue;
        }
        let snapshot_content = serde_json::to_string(node).map_err(|error| {
            PluginError::Internal(format!("failed to serialize Markdown node '{id}': {error}"))
        })?;
        changes.push(DetectedChange {
            entity_pk: vec![(*id).to_owned()],
            schema_key: NODE_SCHEMA_KEY.to_string(),
            snapshot_content: Some(snapshot_content),
            metadata: change_metadata(before.nodes_by_id.get(id).copied(), node),
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

/// Read-only identity index over an existing tree or projection. Reconciliation
/// needs keyed lookup and deletion detection, not another owned copy of every
/// snapshot. The index owns only references and is discarded with the
/// transition.
#[derive(Default)]
struct ProjectionView<'a> {
    nodes_by_id: BTreeMap<&'a str, &'a NodeSnapshot>,
}

impl<'a> ProjectionView<'a> {
    fn from_projection(projection: &'a Projection) -> Self {
        Self {
            nodes_by_id: projection
                .nodes_by_id
                .values()
                .map(|node| (node.id.as_str(), node))
                .collect(),
        }
    }

    fn from_tree(root: &'a NodeTree) -> Self {
        fn visit<'a>(tree: &'a NodeTree, output: &mut BTreeMap<&'a str, &'a NodeSnapshot>) {
            output.insert(tree.node.id.as_str(), &tree.node);
            for child in &tree.children {
                visit(child, output);
            }
        }

        let mut nodes_by_id = BTreeMap::new();
        visit(root, &mut nodes_by_id);
        Self { nodes_by_id }
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
pub struct FileEdit<'a> {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArenaMarkdownBlock {
    pub start: u64,
    pub end: u64,
    pub tree_json: Vec<u8>,
}

/// Persistent top-level Markdown tree. The common paragraph-edit path changes
/// one top-level node, so retaining the parsed base avoids cloning thousands
/// of unrelated `NodeSnapshot` JSON values on every keystroke.
#[derive(Clone, Debug)]
struct PersistentTree {
    base: Arc<NodeTree>,
    root_override: Option<Arc<NodeSnapshot>>,
    top_level_overrides: Arc<BTreeMap<usize, Arc<NodeTree>>>,
}

impl PersistentTree {
    fn new(root: NodeTree) -> Self {
        Self {
            base: Arc::new(root),
            root_override: None,
            top_level_overrides: Arc::new(BTreeMap::new()),
        }
    }

    fn root_node(&self) -> &NodeSnapshot {
        self.root_override.as_deref().unwrap_or(&self.base.node)
    }

    fn top_level_node(&self, index: usize) -> Option<&NodeSnapshot> {
        self.top_level_tree(index).map(|tree| &tree.node)
    }

    fn top_level_tree(&self, index: usize) -> Option<&NodeTree> {
        self.top_level_overrides
            .get(&index)
            .map(Arc::as_ref)
            .or_else(|| self.base.children.get(index))
    }

    fn top_level_len(&self) -> usize {
        self.base.children.len()
    }

    fn replace_top_level_node(&self, index: usize, node: NodeSnapshot) -> Self {
        let mut tree = self
            .top_level_tree(index)
            .expect("base top-level Markdown tree exists")
            .clone();
        tree.node = node;
        self.replace_top_level_tree(index, tree, None)
    }

    fn replace_top_level_tree(
        &self,
        index: usize,
        tree: NodeTree,
        root: Option<NodeSnapshot>,
    ) -> Self {
        let mut overrides = self.top_level_overrides.as_ref().clone();
        overrides.insert(index, Arc::new(tree));
        Self {
            base: Arc::clone(&self.base),
            root_override: root.map(Arc::new).or_else(|| self.root_override.clone()),
            top_level_overrides: Arc::new(overrides),
        }
    }

    fn materialize(&self) -> NodeTree {
        let mut root = self.base.as_ref().clone();
        if let Some(node) = &self.root_override {
            root.node.clone_from(node);
        }
        for (&index, tree) in self.top_level_overrides.iter() {
            root.children[index].clone_from(tree);
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
            return Err(PluginError::InvalidInput(format!(
                "Markdown byte range {}..{} is out of bounds for {} bytes",
                range.start, range.end, self.len
            )));
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
            return Err(PluginError::InvalidInput(format!(
                "Markdown byte range {}..{} is out of bounds for {} bytes",
                range.start, range.end, self.len
            )));
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

    fn splice(&self, splices: &[FileEdit<'_>]) -> Result<Self, PluginError> {
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
    format_json: String,
    id: String,
    kind: NodeKind,
    order_key: Option<String>,
    parent_id: Option<String>,
    payload_json: String,
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
            format_json,
            id: node.id,
            kind: node.kind,
            order_key: node.order_key,
            parent_id: node.parent_id,
            payload_json,
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
    if !changes.is_empty()
        && let Some(document) = nodes_by_id
            .values_mut()
            .find(|node| node.kind == NodeKind::Document)
    {
        let format = document.format.as_object_mut().ok_or_else(|| {
            PluginError::Internal("Markdown document format must be an object".into())
        })?;
        // The accepted source is valid only for the unchanged semantic tree.
        // A semantic successor must render from its derived state instead of
        // accidentally reusing the predecessor's raw bytes.
        format.remove(LEXICAL_FALLBACK_FIELD);
        format.remove(LEXICAL_SOURCE_REQUIRED_FIELD);
    }
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
            content: bytes.clone(),
        };
        let mut parsed = parse_file_with_literal_fast_path(&file, allow_literal_fast_path)?;
        retain_noncanonical_source(&mut parsed, &file.content)?;
        let top_level_ranges = parsed.top_level_ranges.clone();
        let (detected, root) =
            detect_changes_for_markdown(&ProjectionView::default(), None, parsed, namespace, 0)?;
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
        let mut root = projection.to_tree()?;
        let source_required = root
            .node
            .format
            .get(LEXICAL_SOURCE_REQUIRED_FIELD)
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let (bytes, restored) = match accepted {
            Some(bytes) => {
                let format = root.node.format.as_object_mut().ok_or_else(|| {
                    PluginError::InvalidInput("Markdown document format must be an object".into())
                })?;
                format.remove(LEXICAL_SOURCE_REQUIRED_FIELD);
                // The file bytes are the durable authority. This in-memory
                // copy is only a source-preservation cache for the actor;
                // arena_state strips it before semantic state is persisted.
                format.insert(
                    LEXICAL_FALLBACK_FIELD.to_owned(),
                    serde_json::Value::String(
                        base64::engine::general_purpose::STANDARD.encode(&bytes),
                    ),
                );
                (bytes, true)
            }
            None => {
                if source_required {
                    return Err(PluginError::InvalidInput(
                        "Markdown restore requires accepted file bytes for this noncanonical source"
                            .into(),
                    ));
                }
                (render_tree_with_lexical_fallback(&root)?, false)
            }
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

    pub fn open_arena(
        bytes: Vec<u8>,
        root_json: &[u8],
        blocks: Vec<ArenaMarkdownBlock>,
    ) -> Result<Self, PluginError> {
        let (&fallback_flag, root_json) = root_json
            .split_first()
            .ok_or_else(|| PluginError::InvalidInput("Markdown arena root is empty".to_owned()))?;
        let mut root: NodeSnapshot = serde_json::from_slice(root_json).map_err(|error| {
            PluginError::InvalidInput(format!("invalid Markdown arena root: {error}"))
        })?;
        let format = root.format.as_object_mut().ok_or_else(|| {
            PluginError::InvalidInput("Markdown arena root format must be an object".to_owned())
        })?;
        format.remove(LEXICAL_SOURCE_REQUIRED_FIELD);
        if fallback_flag != 0 {
            format.insert(
                LEXICAL_FALLBACK_FIELD.to_owned(),
                serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(&bytes)),
            );
        }
        let mut children = Vec::with_capacity(blocks.len());
        let mut ranges = Vec::with_capacity(blocks.len());
        for (ordinal, block) in blocks.into_iter().enumerate() {
            let child = serde_json::from_slice(&block.tree_json).map_err(|error| {
                PluginError::InvalidInput(format!("invalid Markdown arena block: {error}"))
            })?;
            let start = usize::try_from(block.start).map_err(|_| {
                PluginError::InvalidInput("Markdown arena block start exceeds usize".to_owned())
            })?;
            let end = usize::try_from(block.end).map_err(|_| {
                PluginError::InvalidInput("Markdown arena block end exceeds usize".to_owned())
            })?;
            if start > end || end > bytes.len() {
                return Err(PluginError::InvalidInput(format!(
                    "Markdown arena block {ordinal} range {start}..{end} is outside accepted length {}",
                    bytes.len()
                )));
            }
            children.push(child);
            ranges.push(start..end);
        }
        Ok(Self {
            bytes: PersistentBytes::from_vec(bytes),
            tree: PersistentTree::new(NodeTree {
                node: root,
                children,
            }),
            top_level_ranges: Arc::new(ranges),
        })
    }

    pub fn fork(&self) -> Self {
        self.clone()
    }

    pub fn arena_state(&self) -> Result<(Vec<u8>, Vec<ArenaMarkdownBlock>), PluginError> {
        let mut root = self.tree.root_node().clone();
        let format = root.format.as_object_mut().ok_or_else(|| {
            PluginError::Internal("Markdown document format must be an object".to_owned())
        })?;
        let had_lexical_fallback = format.remove(LEXICAL_FALLBACK_FIELD).is_some();
        format.remove(LEXICAL_SOURCE_REQUIRED_FIELD);
        let mut root_json = vec![u8::from(had_lexical_fallback)];
        root_json.extend(serde_json::to_vec(&root).map_err(|error| {
            PluginError::Internal(format!("serialize Markdown arena root: {error}"))
        })?);
        let top_level_len = self.tree.top_level_len();
        let ranges_are_addressable = self.top_level_ranges.len() == top_level_len;
        let mut blocks = Vec::with_capacity(top_level_len);
        let bytes_len = self.bytes.len;
        for index in 0..top_level_len {
            // Accepted noncanonical bytes are the only lexical authority, so
            // restore cannot derive source ranges by parsing or rendering.
            // Keep every semantic child with an empty, non-addressable range;
            // sparse edit lookup will decline it and use the full correctness
            // path with the accepted bytes and complete semantic tree.
            let range = if ranges_are_addressable {
                self.top_level_ranges[index].clone()
            } else {
                0..0
            };
            let tree = self.tree.top_level_tree(index).ok_or_else(|| {
                PluginError::Internal("Markdown arena range has no matching block".to_owned())
            })?;
            blocks.push(ArenaMarkdownBlock {
                start: u64::try_from(range.start.min(bytes_len))
                    .map_err(|_| PluginError::Internal("Markdown range exceeds u64".to_owned()))?,
                end: u64::try_from(range.end.min(bytes_len))
                    .map_err(|_| PluginError::Internal("Markdown range exceeds u64".to_owned()))?,
                tree_json: serde_json::to_vec(tree).map_err(|error| {
                    PluginError::Internal(format!("serialize Markdown arena block: {error}"))
                })?,
            });
        }
        Ok((root_json, blocks))
    }

    pub fn file_changed_from_arena_block(
        before: Vec<u8>,
        root_json: &[u8],
        block_json: &[u8],
        block_start: u64,
        block_end: u64,
        splice: FileEdit<'_>,
        namespace: IdNamespace,
        minimum_ordinal: u32,
    ) -> Result<Option<(Vec<EntityChange>, Vec<u8>, Vec<u8>)>, PluginError> {
        let (&fallback_flag, root_json) = root_json
            .split_first()
            .ok_or_else(|| PluginError::InvalidInput("Markdown arena root is empty".to_owned()))?;
        let root: NodeSnapshot = serde_json::from_slice(root_json).map_err(|error| {
            PluginError::InvalidInput(format!("invalid Markdown arena root: {error}"))
        })?;
        let block = serde_json::from_slice(block_json).map_err(|error| {
            PluginError::InvalidInput(format!("invalid Markdown arena block: {error}"))
        })?;
        let start = usize::try_from(block_start).map_err(|_| {
            PluginError::InvalidInput("Markdown arena block start exceeds usize".to_owned())
        })?;
        let end = usize::try_from(block_end).map_err(|_| {
            PluginError::InvalidInput("Markdown arena block end exceeds usize".to_owned())
        })?;
        let document = Self {
            bytes: PersistentBytes::from_vec(before),
            tree: PersistentTree::new(NodeTree {
                node: root,
                children: vec![block],
            }),
            top_level_ranges: Arc::new(std::iter::once(start..end).collect()),
        };
        let successor_bytes = document.bytes.splice(&[splice])?;
        let Some((detected, top_level_ranges, tree)) = document.try_top_level_replacement(
            &[splice],
            &successor_bytes,
            namespace,
            minimum_ordinal,
        )?
        else {
            return Ok(None);
        };
        let changes = detected
            .into_iter()
            .map(detected_to_entity_change)
            .collect::<Result<Vec<_>, _>>()?;
        let successor = Self {
            bytes: successor_bytes,
            tree,
            top_level_ranges,
        };
        let root_json = {
            let mut bytes = vec![fallback_flag];
            bytes.extend_from_slice(root_json);
            bytes
        };
        let block_json = serde_json::to_vec(successor.tree.top_level_tree(0).ok_or_else(|| {
            PluginError::Internal("Markdown arena successor lost its changed block".to_owned())
        })?)
        .map_err(|error| {
            PluginError::Internal(format!("serialize successor Markdown arena block: {error}"))
        })?;
        Ok(Some((changes, root_json, block_json)))
    }

    pub fn file_changed(
        &self,
        splices: &[FileEdit<'_>],
        namespace: IdNamespace,
    ) -> Result<(Self, Vec<EntityChange>), PluginError> {
        let successor_bytes = self.bytes.splice(splices)?;
        let (detected, top_level_ranges, tree) = if let Some(incremental) =
            self.try_paragraph_replacement(splices, &successor_bytes, namespace)?
        {
            incremental
        } else if let Some(incremental) =
            self.try_top_level_replacement(splices, &successor_bytes, namespace, 0)?
        {
            incremental
        } else {
            let bytes = successor_bytes.materialize();
            let file = File {
                filename: None,
                content: bytes,
            };
            let current_root = self.tree.materialize();
            let before = ProjectionView::from_tree(&current_root);
            let mut parsed = parse_file(&file)?;
            retain_noncanonical_source(&mut parsed, &file.content)?;
            let top_level_ranges = Arc::new(parsed.top_level_ranges.clone());
            let (detected, root) =
                detect_changes_for_markdown(&before, Some(&current_root), parsed, namespace, 0)?;
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
        if range.end > self.bytes.len {
            return Ok(None);
        }
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
        let bytes = self.bytes.splice(&[FileEdit {
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
        let successor_tree = self.tree.replace_top_level_node(block_index, new);
        Ok(Some((
            bytes,
            Arc::new(top_level_ranges),
            edits,
            successor_tree,
        )))
    }

    fn try_paragraph_replacement(
        &self,
        splices: &[FileEdit<'_>],
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
        if range.end > bytes.len {
            return Ok(None);
        }

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
                "file.content must be valid UTF-8 for an incremental Markdown edit: {error}"
            ))
        })?;
        let mut replacement = parse_markdown_source(fragment)?;
        if replacement.root.children.len() != 1
            || replacement.root.children[0].node.kind != NodeKind::Paragraph
            || render_tree(&replacement.root)? != fragment.as_bytes()
        {
            return Ok(None);
        }

        let old = self
            .tree
            .top_level_tree(block_index)
            .expect("validated top-level paragraph exists")
            .clone();
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
        let successor_tree = self.tree.replace_top_level_tree(block_index, new, None);
        Ok(Some((
            detected,
            Arc::clone(&self.top_level_ranges),
            successor_tree,
        )))
    }

    /// Reparse one complete top-level block when a single edit is fully
    /// contained by that block.
    ///
    /// The full parser remains the correctness fallback for edits that cross
    /// block boundaries or alter line structure. A successful local parse is
    /// accepted only when it produces exactly one block and renders byte-for-
    /// byte to the successor fragment. Reconciliation then runs over a
    /// document containing only the changed block, preserving stable subtree
    /// identities without cloning or parsing unrelated siblings.
    fn try_top_level_replacement(
        &self,
        splices: &[FileEdit<'_>],
        bytes: &PersistentBytes,
        namespace: IdNamespace,
        minimum_ordinal: u32,
    ) -> Result<Option<(Vec<DetectedChange>, Arc<Vec<Range<usize>>>, PersistentTree)>, PluginError>
    {
        let [splice] = splices else {
            return Ok(None);
        };
        let offset = usize::try_from(splice.offset)
            .map_err(|_| PluginError::InvalidInput("Markdown splice offset is too large".into()))?;
        let delete_len = usize::try_from(splice.delete_len).map_err(|_| {
            PluginError::InvalidInput("Markdown splice delete length is too large".into())
        })?;
        let delete_end = offset.checked_add(delete_len).ok_or_else(|| {
            PluginError::InvalidInput("Markdown splice delete range overflowed".into())
        })?;
        if splice.insert.contains(&b'\n')
            || splice.insert.contains(&b'\r')
            || self
                .bytes
                .range(offset..delete_end)?
                .iter()
                .any(|byte| matches!(byte, b'\n' | b'\r'))
        {
            return Ok(None);
        }
        let Some((block_index, range)) =
            self.top_level_ranges.iter().enumerate().find(|(_, range)| {
                range.start <= offset && offset < range.end && delete_end <= range.end
            })
        else {
            return Ok(None);
        };
        let delta = isize::try_from(splice.insert.len()).expect("usize fits isize")
            - isize::try_from(delete_len).expect("usize fits isize");
        let successor_end = range
            .end
            .checked_add_signed(delta)
            .ok_or_else(|| PluginError::Internal("Markdown block range shift overflow".into()))?;
        if successor_end > bytes.len {
            return Ok(None);
        }
        let fragment_bytes = bytes.range(range.start..successor_end)?;
        let fragment = std::str::from_utf8(&fragment_bytes).map_err(|error| {
            PluginError::InvalidInput(format!(
                "file.content must be valid UTF-8 for an incremental Markdown edit: {error}"
            ))
        })?;
        let mut replacement = parse_markdown_source(fragment)?;
        if replacement.root.children.len() != 1
            || render_tree(&replacement.root)? != fragment.as_bytes()
        {
            return Ok(None);
        }

        let old_block = self
            .tree
            .top_level_tree(block_index)
            .expect("base top-level Markdown tree exists")
            .clone();
        if !node_kinds_are_identity_compatible(
            old_block.node.kind,
            replacement.root.children[0].node.kind,
        ) {
            return Ok(None);
        }
        let old_root = NodeTree {
            node: self.tree.root_node().clone(),
            children: vec![old_block],
        };
        let mut new_root_node = self.tree.root_node().clone();
        if new_root_node.format.get(LEXICAL_FALLBACK_FIELD).is_some() {
            let format = new_root_node.format.as_object_mut().ok_or_else(|| {
                PluginError::Internal("Markdown document format must be an object".into())
            })?;
            format.insert(
                LEXICAL_FALLBACK_FIELD.to_owned(),
                serde_json::Value::String(
                    base64::engine::general_purpose::STANDARD.encode(bytes.materialize()),
                ),
            );
        }
        replacement.root.node.payload = new_root_node.payload;
        replacement.root.node.format = new_root_node.format;
        let before = ProjectionView::from_tree(&old_root);
        let (detected, mut reconciled) = detect_changes_for_markdown(
            &before,
            Some(&old_root),
            replacement,
            namespace,
            minimum_ordinal,
        )?;
        let reconciled_block = reconciled.children.pop().ok_or_else(|| {
            PluginError::Internal("incremental Markdown parse lost its changed block".into())
        })?;
        if !reconciled.children.is_empty() {
            return Err(PluginError::Internal(
                "incremental Markdown parse produced multiple changed blocks".into(),
            ));
        }

        let mut top_level_ranges = self.top_level_ranges.as_ref().clone();
        top_level_ranges[block_index].end = successor_end;
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
        let successor_tree =
            self.tree
                .replace_top_level_tree(block_index, reconciled_block, Some(reconciled.node));
        Ok(Some((detected, Arc::new(top_level_ranges), successor_tree)))
    }

    pub(crate) fn bytes(&self) -> Vec<u8> {
        self.bytes.materialize()
    }

    #[cfg(test)]
    pub(crate) fn accepted_bytes(&self) -> Vec<u8> {
        self.bytes()
    }

    #[cfg(test)]
    pub(crate) fn shares_base_tree_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.tree.base, &other.tree.base)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn semantic_records_without_raw_source(source: Vec<u8>) -> Vec<EntityRecord> {
        let (_, mut changes) = Document::open_file(
            source,
            Some("competitors.md"),
            IdNamespace::from_halves(7, 11),
        )
        .expect("parse Markdown source");
        for change in &mut changes {
            let Some(snapshot) = &change.snapshot else {
                continue;
            };
            let mut node: NodeSnapshot = serde_json::from_str(
                &wire_to_logical(snapshot).expect("decode logical Markdown snapshot"),
            )
            .expect("logical Markdown snapshot");
            if node.kind != NodeKind::Document {
                continue;
            }
            let format = node.format.as_object_mut().expect("document format object");
            format.remove(LEXICAL_FALLBACK_FIELD);
            format.insert(
                LEXICAL_SOURCE_REQUIRED_FIELD.to_owned(),
                serde_json::Value::Bool(true),
            );
            change.snapshot = Some(
                logical_to_wire(&serde_json::to_string(&node).expect("encode logical snapshot"))
                    .expect("encode wire Markdown snapshot"),
            );
        }
        changes
            .into_iter()
            .filter_map(|change| {
                change.snapshot.map(|snapshot| EntityRecord {
                    schema_key: change.schema_key,
                    entity_pk: change.entity_pk,
                    snapshot,
                })
            })
            .collect()
    }

    #[test]
    fn stripping_fallback_from_semantic_snapshots_requires_accepted_bytes() {
        let source = br#"# Competitors

*Counter:

(~26 users)

A paragraph directly followed by
- list item

**knowledge base / shared workspace agents read and
write to.**

The remaining document keeps the same ordinary prose shape while making
the source large enough to exercise the full-document serializer.

Another paragraph with *single-asterisk emphasis* and `inline code`.
"#
        .to_vec();
        let (document, _) = Document::open_file(
            source.clone(),
            Some("competitors.md"),
            IdNamespace::from_halves(7, 11),
        )
        .expect("parse reported Markdown constructs");
        let rendered = render_tree(&document.tree.materialize()).expect("render document tree");
        assert_ne!(
            rendered, source,
            "the fixture must take the noncanonical source path"
        );
        let records = semantic_records_without_raw_source(source.clone());
        let error = Document::open_entities(records.clone(), None)
            .expect_err("a noncanonical restore without accepted bytes must fail closed");
        assert!(
            matches!(error, PluginError::InvalidInput(message) if message.contains("accepted file bytes"))
        );
        let (restored, edits) =
            Document::open_entities(records, Some(source.clone())).expect("accepted bytes restore");
        assert!(edits.is_empty());
        assert_eq!(restored.bytes.materialize(), source);
    }

    #[test]
    fn accepted_restore_is_byte_idempotent_across_markdown_corpus() {
        let corpus = [
            "*Counter:\n\n(~26 users)\n\nA paragraph directly followed by\n- list item\n\n**wrapped strong / shared workspace agents read and\nwrite to.**\n",
            "---\nDateApproved: 6/10/2020\n---\n\n# Unicode 😀\n\nRésumé — naïve café\n",
            "```rust\nlet value = *Counter;\n```\n\nAn unmatched *marker and [unfinished link\n",
            "# CRLF\r\n\r\nA paragraph with *emphasis* and `code`.\r\n",
        ];
        for source in corpus {
            let source = source.as_bytes().to_vec();
            let records = semantic_records_without_raw_source(source.clone());
            let (first, first_edits) =
                Document::open_entities(records.clone(), Some(source.clone()))
                    .expect("first accepted restore");
            assert!(first_edits.is_empty());
            let (first_root, first_blocks) = first.arena_state().expect("first arena state");
            let second =
                Document::open_arena(source.clone(), &first_root, first_blocks).expect("reopen");
            assert_eq!(second.bytes.materialize(), source);
            let (second_root, second_blocks) = second.arena_state().expect("second arena state");
            let third = Document::open_arena(source.clone(), &second_root, second_blocks)
                .expect("second reopen");
            assert_eq!(third.bytes.materialize(), source);
        }
    }

    #[test]
    fn accepted_restore_preserves_semantic_children_for_reopen_and_edit() {
        let source = br#"# Competitors

*Counter:

(~26 users)

The remaining document keeps every semantic block.

Another paragraph must survive an unrelated semantic edit.
"#
        .to_vec();
        let records = semantic_records_without_raw_source(source.clone());
        let (restored, edits) =
            Document::open_entities(records, Some(source.clone())).expect("accepted bytes restore");
        assert!(edits.is_empty());
        let expected_tree = restored.tree.materialize();
        let expected_child_count = expected_tree.children.len();
        assert!(expected_child_count > 1, "fixture needs unrelated blocks");

        let (root, blocks) = restored.arena_state().expect("restored arena state");
        assert_eq!(blocks.len(), expected_child_count);
        assert!(
            blocks
                .iter()
                .all(|block| block.start == 0 && block.end == 0),
            "restored lexical ranges must be explicitly non-addressable"
        );
        let reopened = Document::open_arena(source, &root, blocks).expect("arena reopen");
        assert_eq!(reopened.tree.materialize(), expected_tree);

        let mut edited = expected_tree
            .children
            .iter()
            .find(|child| {
                serde_json::to_string(&child.node.payload)
                    .is_ok_and(|payload| payload.contains("remaining document"))
            })
            .expect("editable paragraph")
            .node
            .clone();
        let payload = serde_json::to_string(&edited.payload).expect("paragraph payload");
        let successor_payload = payload.replacen("remaining document", "retained document", 1);
        assert_ne!(successor_payload, payload);
        edited.payload = serde_json::from_str(&successor_payload).expect("edited payload");
        let edited_id = edited.id.clone();
        let snapshot = logical_to_wire(
            &serde_json::to_string(&edited).expect("edited logical Markdown snapshot"),
        )
        .expect("edited wire Markdown snapshot");
        let (successor, _) = reopened
            .entities_changed(vec![EntityChange {
                schema_key: NODE_SCHEMA_KEY.to_owned(),
                entity_pk: vec![edited_id.clone()],
                snapshot: Some(snapshot),
                effect: ChangeEffect::Content,
            }])
            .expect("semantic child edit after restore");
        let successor_tree = successor.tree.materialize();
        assert_eq!(successor_tree.children.len(), expected_child_count);
        for (before, after) in expected_tree.children.iter().zip(&successor_tree.children) {
            if before.node.id != edited_id {
                assert_eq!(after, before, "unrelated semantic block changed");
            }
        }
        assert!(
            successor_tree
                .node
                .format
                .get(LEXICAL_FALLBACK_FIELD)
                .is_none(),
            "semantic successor must clear the lexical fallback"
        );
        let rendered = String::from_utf8(successor.bytes()).expect("rendered Markdown is UTF-8");
        assert!(rendered.contains("retained document"));
        assert!(rendered.contains("Another paragraph must survive"));
    }
}
