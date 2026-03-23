use super::codec::{
    boundary_trigger, child_summary_from_node, compare_encoded_key_to_key, encode_internal_node,
    encode_leaf_node, ChildSummary, DecodedLeafNode, DecodedNode, EncodedLeafEntry,
    PendingChunkWrite,
};
use super::{
    child_summary_from_internal_child, clone_prepared_entry, estimate_internal_chunk_size,
    estimate_leaf_chunk_size, internal_child_index, BuiltTree, InternalChunkAccumulator,
    LeafChunkAccumulator, LiveTrackedEntityKey, LiveTrackedRootId, LiveTrackedState,
    LiveTrackedStateOptions, PreparedMutations,
};
use crate::LixError;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Clone)]
struct CursorFrame {
    hash: [u8; 32],
    node: Arc<DecodedNode>,
    idx: usize,
}

#[derive(Debug, Clone)]
struct TreeCursor {
    frames: Vec<CursorFrame>,
}

#[derive(Debug, Clone)]
enum TreeItem {
    Leaf(EncodedLeafEntry),
    Summary(ChildSummary),
}

#[derive(Debug)]
enum ChunkBuilder {
    Leaf(LeafChunkAccumulator),
    Internal(InternalChunkAccumulator),
}

#[derive(Debug)]
struct ChunkerRoot {
    summary: ChildSummary,
    tree_height: usize,
}

#[derive(Debug)]
struct CursorChunker {
    level: usize,
    cursor: Option<TreeCursor>,
    parent: Option<Box<CursorChunker>>,
    builder: ChunkBuilder,
}

impl TreeCursor {
    fn current(&self) -> &CursorFrame {
        self.frames.last().expect("cursor has a current frame")
    }

    fn current_mut(&mut self) -> &mut CursorFrame {
        self.frames.last_mut().expect("cursor has a current frame")
    }

    fn count(&self) -> usize {
        match self.current().node.as_ref() {
            DecodedNode::Leaf(leaf) => leaf.entry_count(),
            DecodedNode::Internal(internal) => internal.child_count(),
        }
    }

    fn valid(&self) -> bool {
        self.count() > 0 && self.current().idx < self.count()
    }

    fn at_node_end(&self) -> bool {
        self.valid() && (self.current().idx + 1 == self.count())
    }

    fn compare(&self, other: &Self) -> isize {
        for (left, right) in self.frames.iter().zip(other.frames.iter()) {
            let diff = left.idx as isize - right.idx as isize;
            if diff != 0 {
                return diff;
            }
        }
        0
    }

    fn copy_from(&mut self, other: &Self) {
        self.frames.clone_from(&other.frames);
    }

    fn invalidate_at_end(&mut self) {
        let count = self.count();
        self.current_mut().idx = count;
    }

    fn parent(&self) -> Option<Self> {
        if self.frames.len() <= 1 {
            None
        } else {
            Some(Self {
                frames: self.frames[..self.frames.len() - 1].to_vec(),
            })
        }
    }

    fn current_item(&self) -> Result<TreeItem, LixError> {
        if !self.valid() {
            return Err(LixError::unknown(
                "dolt rewrite cursor item requested while out of bounds",
            ));
        }
        Ok(match self.current().node.as_ref() {
            DecodedNode::Leaf(leaf) => TreeItem::Leaf(EncodedLeafEntry {
                key: leaf.key_at(self.current().idx).to_vec(),
                value: leaf.value_at(self.current().idx).to_vec(),
            }),
            DecodedNode::Internal(internal) => TreeItem::Summary(
                child_summary_from_internal_child(internal, self.current().idx),
            ),
        })
    }

    fn key_equals(&self, key: &LiveTrackedEntityKey) -> bool {
        match self.current().node.as_ref() {
            DecodedNode::Leaf(leaf) if self.valid() => {
                compare_encoded_key_to_key(leaf.key_at(self.current().idx), key) == Ordering::Equal
            }
            _ => false,
        }
    }
}

impl ChunkBuilder {
    fn new(level: usize) -> Self {
        if level == 0 {
            Self::Leaf(LeafChunkAccumulator::default())
        } else {
            Self::Internal(InternalChunkAccumulator::default())
        }
    }

    fn count(&self) -> usize {
        match self {
            Self::Leaf(acc) => acc.entries.len(),
            Self::Internal(acc) => acc.children.len(),
        }
    }

    fn projected_size(&self, item: &TreeItem) -> Result<usize, LixError> {
        match (self, item) {
            (Self::Leaf(acc), TreeItem::Leaf(entry)) => Ok(estimate_leaf_chunk_size(
                acc.entries.len() + 1,
                acc.key_bytes + entry.key.len(),
                acc.value_bytes + entry.value.len(),
            )),
            (Self::Internal(acc), TreeItem::Summary(child)) => Ok(estimate_internal_chunk_size(
                acc.children.len() + 1,
                acc.first_key_bytes + child.first_key.len(),
                acc.last_key_bytes + child.last_key.len(),
            )),
            _ => Err(LixError::unknown("dolt rewrite builder/item kind mismatch")),
        }
    }

    fn current_size(&self) -> usize {
        match self {
            Self::Leaf(acc) => {
                estimate_leaf_chunk_size(acc.entries.len(), acc.key_bytes, acc.value_bytes)
            }
            Self::Internal(acc) => estimate_internal_chunk_size(
                acc.children.len(),
                acc.first_key_bytes,
                acc.last_key_bytes,
            ),
        }
    }

    fn append(&mut self, item: TreeItem) -> Result<(), LixError> {
        match (self, item) {
            (Self::Leaf(acc), TreeItem::Leaf(entry)) => {
                acc.key_bytes += entry.key.len();
                acc.value_bytes += entry.value.len();
                acc.entries.push(entry);
                Ok(())
            }
            (Self::Internal(acc), TreeItem::Summary(child)) => {
                acc.first_key_bytes += child.first_key.len();
                acc.last_key_bytes += child.last_key.len();
                acc.children.push(child);
                Ok(())
            }
            _ => Err(LixError::unknown("dolt rewrite append kind mismatch")),
        }
    }

    fn boundary_key(&self) -> Option<&[u8]> {
        match self {
            Self::Leaf(acc) => acc.entries.last().map(|entry| entry.key.as_slice()),
            Self::Internal(acc) => acc.children.last().map(|child| child.first_key.as_slice()),
        }
    }

    fn take_node(&mut self) -> (PendingChunkWrite, ChildSummary) {
        match self {
            Self::Leaf(acc) => {
                let entries = std::mem::take(&mut acc.entries);
                acc.key_bytes = 0;
                acc.value_bytes = 0;
                let subtree_count = entries.len() as u64;
                let first_key = entries
                    .first()
                    .map(|entry| entry.key.clone())
                    .unwrap_or_default();
                let last_key = entries
                    .last()
                    .map(|entry| entry.key.clone())
                    .unwrap_or_default();
                child_summary_from_node(
                    encode_leaf_node(&entries),
                    first_key,
                    last_key,
                    subtree_count,
                )
            }
            Self::Internal(acc) => {
                let children = std::mem::take(&mut acc.children);
                acc.first_key_bytes = 0;
                acc.last_key_bytes = 0;
                let subtree_count = children.iter().map(|child| child.subtree_count).sum();
                let first_key = children
                    .first()
                    .map(|child| child.first_key.clone())
                    .unwrap_or_default();
                let last_key = children
                    .last()
                    .map(|child| child.last_key.clone())
                    .unwrap_or_default();
                child_summary_from_node(
                    encode_internal_node(&children),
                    first_key,
                    last_key,
                    subtree_count,
                )
            }
        }
    }

    fn single_child_summary(&self) -> Option<ChildSummary> {
        match self {
            Self::Internal(acc) if acc.children.len() == 1 => acc.children.first().cloned(),
            _ => None,
        }
    }
}

impl CursorChunker {
    fn new(
        level: usize,
        cursor: Option<TreeCursor>,
        options: &LiveTrackedStateOptions,
        chunk_map: &mut BTreeMap<[u8; 32], PendingChunkWrite>,
    ) -> Result<Self, LixError> {
        let mut chunker = Self {
            level,
            cursor,
            parent: None,
            builder: ChunkBuilder::new(level),
        };
        if chunker.cursor.is_some() {
            chunker.process_prefix(options, chunk_map)?;
        }
        Ok(chunker)
    }

    fn create_parent(
        &mut self,
        options: &LiveTrackedStateOptions,
        chunk_map: &mut BTreeMap<[u8; 32], PendingChunkWrite>,
    ) -> Result<(), LixError> {
        if self.parent.is_none() {
            self.parent = Some(Box::new(Self::new(
                self.level + 1,
                self.cursor.as_ref().and_then(TreeCursor::parent),
                options,
                chunk_map,
            )?));
        }
        Ok(())
    }

    fn process_prefix(
        &mut self,
        options: &LiveTrackedStateOptions,
        chunk_map: &mut BTreeMap<[u8; 32], PendingChunkWrite>,
    ) -> Result<(), LixError> {
        if self.cursor.as_ref().and_then(TreeCursor::parent).is_some() {
            self.create_parent(options, chunk_map)?;
        }

        let original_idx = self
            .cursor
            .as_ref()
            .map(|cursor| cursor.current().idx)
            .unwrap_or(0);
        for index in 0..original_idx {
            self.cursor.as_mut().expect("cursor").current_mut().idx = index;
            let item = self.cursor.as_ref().expect("cursor").current_item()?;
            let _ = self.append_item(item, options, chunk_map)?;
        }
        if let Some(cursor) = self.cursor.as_mut() {
            cursor.current_mut().idx = original_idx;
        }
        Ok(())
    }

    fn append_item(
        &mut self,
        item: TreeItem,
        options: &LiveTrackedStateOptions,
        chunk_map: &mut BTreeMap<[u8; 32], PendingChunkWrite>,
    ) -> Result<bool, LixError> {
        let previous_size = self.builder.current_size();
        let projected = self.builder.projected_size(&item)?;
        if self.builder.count() > 0 && projected > options.max_chunk_bytes {
            self.handle_chunk_boundary(options, chunk_map)?;
        }

        let previous_size = if self.builder.count() == 0 {
            self.builder.current_size()
        } else {
            previous_size
        };
        self.builder.append(item)?;
        let degenerate_internal = self.level > 0 && self.builder.count() == 1;
        let current_size = self.builder.current_size();
        let should_split = current_size >= options.min_chunk_bytes
            && (current_size >= options.max_chunk_bytes
                || self
                    .builder
                    .boundary_key()
                    .map(|key| {
                        boundary_trigger(
                            key,
                            self.level,
                            current_size,
                            current_size.saturating_sub(previous_size),
                            options.target_chunk_bytes,
                        )
                    })
                    .unwrap_or(false));
        if should_split && !degenerate_internal {
            self.handle_chunk_boundary(options, chunk_map)?;
            return Ok(true);
        }
        Ok(false)
    }

    fn handle_chunk_boundary(
        &mut self,
        options: &LiveTrackedStateOptions,
        chunk_map: &mut BTreeMap<[u8; 32], PendingChunkWrite>,
    ) -> Result<(), LixError> {
        if self.builder.count() == 0 {
            return Ok(());
        }
        let (chunk, summary) = self.builder.take_node();
        let current_hash = self.cursor.as_ref().map(|cursor| cursor.current().hash);
        if current_hash != Some(chunk.hash) {
            chunk_map.entry(chunk.hash).or_insert(chunk);
        }
        self.create_parent(options, chunk_map)?;
        self.parent.as_mut().expect("parent chunker").append_item(
            TreeItem::Summary(summary),
            options,
            chunk_map,
        )?;
        Ok(())
    }

    fn any_pending(&self) -> bool {
        self.builder.count() > 0
            || self
                .parent
                .as_ref()
                .map(|parent| parent.any_pending())
                .unwrap_or(false)
    }

    fn sync_parent_cursors_from_current(&mut self) {
        if let Some(parent) = self.parent.as_mut() {
            parent.cursor = self.cursor.as_ref().and_then(TreeCursor::parent);
            parent.sync_parent_cursors_from_current();
        }
    }

    fn skip_current<'s>(
        &'s mut self,
        state: &'s LiveTrackedState<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), LixError>> + 's>> {
        Box::pin(async move {
            if let Some(cursor) = self.cursor.as_mut() {
                state.advance_dolt_cursor(cursor).await?;
                self.sync_parent_cursors_from_current();
            }
            Ok(())
        })
    }

    fn advance_to<'s>(
        &'s mut self,
        state: &'s LiveTrackedState<'_>,
        next: TreeCursor,
        options: &'s LiveTrackedStateOptions,
        chunk_map: &'s mut BTreeMap<[u8; 32], PendingChunkWrite>,
    ) -> Pin<Box<dyn Future<Output = Result<(), LixError>> + 's>> {
        Box::pin(async move {
            if self
                .cursor
                .as_ref()
                .ok_or_else(|| LixError::unknown("dolt rewrite chunker cursor missing"))?
                .compare(&next)
                == 0
            {
                return Ok(());
            }

            if !self.cursor.as_ref().expect("cursor").valid() {
                let next_parent = next.parent();
                if next_parent.is_none() {
                    self.cursor.as_mut().expect("cursor").copy_from(&next);
                    return Ok(());
                }

                self.create_parent(options, chunk_map)?;
                if let Some(parent_cursor) = self
                    .parent
                    .as_mut()
                    .and_then(|parent| parent.cursor.as_mut())
                {
                    state.advance_dolt_cursor(parent_cursor).await?;
                }
                self.parent
                    .as_mut()
                    .expect("parent chunker")
                    .advance_to(state, next_parent.expect("next parent"), options, chunk_map)
                    .await?;
                self.cursor.as_mut().expect("cursor").copy_from(&next);
                self.process_prefix(options, chunk_map)?;
                return Ok(());
            }

            let mut split = {
                let item = self.cursor.as_ref().expect("cursor").current_item()?;
                self.append_item(item, options, chunk_map)?
            };
            while {
                let cursor = self.cursor.as_ref().expect("cursor");
                !(split && cursor.at_node_end())
            } {
                state
                    .advance_dolt_cursor(self.cursor.as_mut().expect("cursor"))
                    .await?;
                self.sync_parent_cursors_from_current();
                if self.cursor.as_ref().expect("cursor").compare(&next) >= 0 {
                    return Ok(());
                }
                let item = self.cursor.as_ref().expect("cursor").current_item()?;
                split = self.append_item(item, options, chunk_map)?;
            }

            if let (Some(current_parent), Some(next_parent)) = (
                self.cursor.as_ref().and_then(TreeCursor::parent),
                next.parent(),
            ) {
                if current_parent.compare(&next_parent) == 0 {
                    self.cursor.as_mut().expect("cursor").copy_from(&next);
                    return Ok(());
                }
            }

            let next_parent = next.parent();
            if next_parent.is_none() {
                self.cursor.as_mut().expect("cursor").copy_from(&next);
                return Ok(());
            }

            self.create_parent(options, chunk_map)?;
            if let Some(parent_cursor) = self
                .parent
                .as_mut()
                .and_then(|parent| parent.cursor.as_mut())
            {
                state.advance_dolt_cursor(parent_cursor).await?;
            }
            self.cursor.as_mut().expect("cursor").invalidate_at_end();
            self.parent
                .as_mut()
                .expect("parent chunker")
                .advance_to(state, next_parent.expect("next parent"), options, chunk_map)
                .await?;
            self.cursor.as_mut().expect("cursor").copy_from(&next);
            self.process_prefix(options, chunk_map)?;
            Ok(())
        })
    }

    fn finalize_cursor<'s>(
        &'s mut self,
        state: &'s LiveTrackedState<'_>,
        options: &'s LiveTrackedStateOptions,
        chunk_map: &'s mut BTreeMap<[u8; 32], PendingChunkWrite>,
    ) -> Pin<Box<dyn Future<Output = Result<(), LixError>> + 's>> {
        Box::pin(async move {
            while self
                .cursor
                .as_ref()
                .map(|cursor| cursor.valid())
                .unwrap_or(false)
            {
                let split = {
                    let item = self.cursor.as_ref().expect("cursor").current_item()?;
                    self.append_item(item, options, chunk_map)?
                };
                if split && self.cursor.as_ref().expect("cursor").at_node_end() {
                    break;
                }
                state
                    .advance_dolt_cursor(self.cursor.as_mut().expect("cursor"))
                    .await?;
                self.sync_parent_cursors_from_current();
            }

            if let Some(parent_cursor) = self
                .parent
                .as_mut()
                .and_then(|parent| parent.cursor.as_mut())
            {
                state.advance_dolt_cursor(parent_cursor).await?;
            }
            if let Some(cursor) = self.cursor.as_mut() {
                cursor.invalidate_at_end();
            }
            Ok(())
        })
    }

    fn done<'s>(
        &'s mut self,
        state: &'s LiveTrackedState<'_>,
        options: &'s LiveTrackedStateOptions,
        chunk_map: &'s mut BTreeMap<[u8; 32], PendingChunkWrite>,
    ) -> Pin<Box<dyn Future<Output = Result<ChunkerRoot, LixError>> + 's>> {
        Box::pin(async move {
            if self.cursor.is_some() {
                self.finalize_cursor(state, options, chunk_map).await?;
            }

            if self
                .parent
                .as_ref()
                .map(|parent| parent.any_pending())
                .unwrap_or(false)
            {
                if self.builder.count() > 0 {
                    self.handle_chunk_boundary(options, chunk_map)?;
                }
                return self
                    .parent
                    .as_mut()
                    .expect("parent chunker")
                    .done(state, options, chunk_map)
                    .await;
            }

            let (chunk, summary) = self.builder.take_node();
            let current_hash = self.cursor.as_ref().map(|cursor| cursor.current().hash);
            if current_hash != Some(chunk.hash) {
                chunk_map.entry(chunk.hash).or_insert(chunk);
            }
            Ok(ChunkerRoot {
                summary,
                tree_height: self.level + 1,
            })
        })
    }
}

impl<'a> LiveTrackedState<'a> {
    pub(super) async fn build_tree_from_base_with_dolt_chunker(
        &self,
        base_root: &LiveTrackedRootId,
        prepared: PreparedMutations,
    ) -> Result<BuiltTree, LixError> {
        let PreparedMutations {
            mutations,
            values,
            value_ref_bytes,
        } = prepared;
        let root_hash = *base_root.as_bytes();
        let mut chunk_map = BTreeMap::<[u8; 32], PendingChunkWrite>::new();
        let mut chunker = CursorChunker::new(
            0,
            Some(self.cursor_at_key(root_hash, &mutations[0].key).await?),
            &self.options,
            &mut chunk_map,
        )?;

        for mutation in &mutations {
            let target = self.cursor_at_key(root_hash, &mutation.key).await?;
            chunker
                .advance_to(self, target.clone(), &self.options, &mut chunk_map)
                .await?;
            if target.key_equals(&mutation.key) {
                chunker.skip_current(self).await?;
            }
            chunker.append_item(
                TreeItem::Leaf(clone_prepared_entry(mutation)),
                &self.options,
                &mut chunk_map,
            )?;
        }

        let root = chunker.done(self, &self.options, &mut chunk_map).await?;
        let provisional_chunks = chunk_map.values().cloned().collect::<Vec<_>>();
        self.populate_cache(&provisional_chunks)?;
        let topology = self
            .load_root_leaf_topology(&LiveTrackedRootId::new(root.summary.child_hash))
            .await?;
        self.build_tree_from_level_summaries(
            topology.leaf_summaries,
            values,
            value_ref_bytes,
            chunk_map,
            1,
        )
    }

    async fn cursor_at_key(
        &self,
        root_hash: [u8; 32],
        key: &LiveTrackedEntityKey,
    ) -> Result<TreeCursor, LixError> {
        let mut frames = Vec::new();
        let mut current_hash = root_hash;
        loop {
            let node = self.load_node(&current_hash).await?;
            let idx = match node.as_ref() {
                DecodedNode::Leaf(leaf) => leaf_lower_bound(leaf, key),
                DecodedNode::Internal(internal) => internal_child_index(internal, key),
            };
            frames.push(CursorFrame {
                hash: current_hash,
                node: Arc::clone(&node),
                idx,
            });
            let DecodedNode::Internal(internal) = node.as_ref() else {
                break;
            };
            if internal.child_count() == 0 {
                break;
            }
            current_hash = *internal.child_hash_at(idx.min(internal.child_count() - 1));
        }
        Ok(TreeCursor { frames })
    }

    async fn advance_dolt_cursor(&self, cursor: &mut TreeCursor) -> Result<(), LixError> {
        let target_depth = cursor.frames.len();
        let mut pivot = None;
        for index in (0..cursor.frames.len()).rev() {
            let count = frame_count(&cursor.frames[index]);
            if cursor.frames[index].idx + 1 < count {
                pivot = Some(index);
                break;
            }
        }

        let Some(pivot) = pivot else {
            cursor.invalidate_at_end();
            return Ok(());
        };

        cursor.frames[pivot].idx += 1;
        cursor.frames.truncate(pivot + 1);
        while cursor.frames.len() < target_depth {
            let frame = cursor.frames.last().expect("cursor frame");
            let DecodedNode::Internal(internal) = frame.node.as_ref() else {
                break;
            };
            if internal.child_count() == 0 {
                break;
            }
            let child_hash = *internal.child_hash_at(frame.idx);
            let child_node = self.load_node(&child_hash).await?;
            cursor.frames.push(CursorFrame {
                hash: child_hash,
                node: child_node,
                idx: 0,
            });
        }
        Ok(())
    }
}

fn frame_count(frame: &CursorFrame) -> usize {
    match frame.node.as_ref() {
        DecodedNode::Leaf(leaf) => leaf.entry_count(),
        DecodedNode::Internal(internal) => internal.child_count(),
    }
}

fn leaf_lower_bound(leaf: &DecodedLeafNode, key: &LiveTrackedEntityKey) -> usize {
    let mut low = 0;
    let mut high = leaf.entry_count();
    while low < high {
        let mid = (low + high) / 2;
        if compare_encoded_key_to_key(leaf.key_at(mid), key) == Ordering::Less {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}
