// TODO: workaround wit_bindgen
#![expect(clippy::same_length_and_capacity)]

wit_bindgen::generate!({
    path: "../../packages/engine/wit",
    world: "plugin",
});

mod markdown_file;
mod model;
pub mod schemas;

pub use crate::exports::lix::plugin::api::{DetectedChange, File, PluginError};
use crate::exports::lix::plugin::api::{EntityState, Guest as Plugin};
use crate::markdown_file::{ParsedMarkdown, parse_file, render_tree};
use crate::model::{
    InlineNode, NodeKind, NodeSnapshot, NodeTree, Projection, parse_inline_payload,
    replace_column_ids, semantic_payload,
};
use lix_order_key::OrderKey;
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};

pub const ROOT_ENTITY_PK: &str = "root";
pub const NODE_SCHEMA_KEY: &str = schemas::NODE_SCHEMA_KEY;
pub const MANIFEST_JSON: &str = include_str!("../manifest.json");

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
#[cfg(target_family = "wasm")]
export!(MarkdownPlugin);

impl Plugin for MarkdownPlugin {
    fn detect_changes(
        state: Vec<EntityState>,
        file: File,
    ) -> Result<Vec<DetectedChange>, PluginError> {
        let before = Projection::from_entity_state(state.into_iter())?;
        let after = parse_file(&file)?;
        detect_changes_for_markdown(&before, after)
    }

    fn render(state: Vec<EntityState>) -> Result<Vec<u8>, PluginError> {
        let projection = Projection::from_entity_state(state.into_iter())?;
        let root = projection.to_tree()?;
        render_tree(&root)
    }
}

fn detect_changes_for_markdown(
    before: &Projection,
    mut after: ParsedMarkdown,
) -> Result<Vec<DetectedChange>, PluginError> {
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
        let mut used_ids = BTreeSet::from([ROOT_ENTITY_PK.to_string()]);
        let mut has_fresh_subtrees = false;
        after.root.node.id = ROOT_ENTITY_PK.to_string();
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

    after.root.visit_mut(&mut |node| {
        replace_column_ids(&mut node.payload, &replacements);
    });
    let after_nodes = flatten_tree(&after.root);
    diff_projections(before, &after_nodes)
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
                    && old.children[*old_index].node.kind == child.node.kind
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
    if tree.node.id != ROOT_ENTITY_PK {
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
    if tree.node.id != ROOT_ENTITY_PK {
        *output.entry(hashes.get(tree)).or_default() += 1;
    }
    for child in &tree.children {
        collect_signature_counts(child, hashes, output);
    }
}

fn initialize_subtree(tree: &mut NodeTree, parent_id: Option<&str>) -> Result<(), PluginError> {
    tree.node.parent_id = parent_id.map(str::to_string);
    if tree.node.kind == NodeKind::Document {
        tree.node.id = ROOT_ENTITY_PK.to_string();
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
    let mut old_for_new = vec![None; new.len()];
    let mut old_used = vec![false; old.len()];
    for index in 0..new.len().min(old.len()) {
        if new[index].signature() == old[index].signature() {
            old_for_new[index] = Some(index);
            old_used[index] = true;
        }
    }
    let mut exact = HashMap::<String, Vec<usize>>::new();
    for (index, inline) in old.iter().enumerate().rev() {
        if old_used[index] {
            continue;
        }
        exact.entry(inline.signature()).or_default().push(index);
    }
    for (new_index, inline) in new.iter().enumerate() {
        if old_for_new[new_index].is_some() {
            continue;
        }
        if let Some(indices) = exact.get_mut(&inline.signature())
            && let Some(old_index) = indices.pop()
        {
            old_for_new[new_index] = Some(old_index);
            old_used[old_index] = true;
        }
    }
    for (new_index, inline) in new.iter().enumerate() {
        if old_for_new[new_index].is_some() {
            continue;
        }
        if let Some(old_index) = old.iter().enumerate().position(|(index, candidate)| {
            !old_used[index] && candidate.kind_tag() == inline.kind_tag()
        }) {
            old_for_new[new_index] = Some(old_index);
            old_used[old_index] = true;
        }
    }
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
        let root = self.nodes_by_id.get(ROOT_ENTITY_PK).ok_or_else(|| {
            PluginError::InvalidInput("Markdown state is missing document root 'root'".to_string())
        })?;
        if root.kind != NodeKind::Document || root.parent_id.is_some() || root.order_key.is_some() {
            return Err(PluginError::InvalidInput(
                "Markdown document root must have kind=document, parent_id=null, order_key=null"
                    .to_string(),
            ));
        }
        let mut children_by_parent = BTreeMap::<String, Vec<&NodeSnapshot>>::new();
        for node in self.nodes_by_id.values() {
            if node.id == ROOT_ENTITY_PK {
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
