use std::borrow::Borrow;
use std::cmp::Ordering;
use std::sync::Arc;

/// Small immutable AVL map used by revisioned in-memory indexes.
///
/// Each mutation copies only the search path. Older readers retain their root,
/// while a committed generation shares every untouched subtree with it.
#[derive(Debug, Clone)]
pub(crate) struct PersistentMap<K, V> {
    root: Option<Arc<Node<K, V>>>,
    len: usize,
}

pub(crate) struct PersistentMapRangeCursor<'a, K, V> {
    stack: Vec<&'a Node<K, V>>,
    upper: std::ops::Bound<K>,
}

#[derive(Debug)]
struct Node<K, V> {
    key: K,
    value: V,
    height: u8,
    left: Option<Arc<Self>>,
    right: Option<Arc<Self>>,
}

impl<K, V> Default for PersistentMap<K, V> {
    fn default() -> Self {
        Self { root: None, len: 0 }
    }
}

impl<K, V> PersistentMap<K, V>
where
    K: Clone + Ord,
    V: Clone,
{
    pub(crate) fn from_sorted(entries: Vec<(K, V)>) -> Self {
        fn build<K: Clone, V: Clone>(entries: &[(K, V)]) -> Option<Arc<Node<K, V>>> {
            let (middle, rest) = entries.split_at(entries.len() / 2);
            let ((key, value), right) = rest.split_first()?;
            Some(node(
                key.clone(),
                value.clone(),
                build(middle),
                build(right),
            ))
        }

        Self {
            root: build(&entries),
            len: entries.len(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut cursor = self.root.as_deref();
        while let Some(current) = cursor {
            match key.cmp(current.key.borrow()) {
                Ordering::Less => cursor = current.left.as_deref(),
                Ordering::Greater => cursor = current.right.as_deref(),
                Ordering::Equal => return Some(&current.value),
            }
        }
        None
    }

    pub(crate) fn insert(&self, key: K, value: V) -> Self {
        let (root, replaced) = insert(self.root.as_ref(), key, value);
        Self {
            root: Some(root),
            len: self.len + usize::from(!replaced),
        }
    }

    pub(crate) fn remove<Q>(&self, key: &Q) -> Self
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let (root, removed) = remove(self.root.as_ref(), key);
        Self {
            root,
            len: self.len.saturating_sub(usize::from(removed)),
        }
    }

    pub(crate) fn values(&self) -> Vec<V> {
        let mut values = Vec::with_capacity(self.len);
        collect_values(self.root.as_deref(), &mut values);
        values
    }

    /// Collects values whose key projection equals `needle` without scanning
    /// either side of the matching key range.
    pub(crate) fn values_equal_by<Q, F>(&self, needle: &Q, project: F) -> Vec<V>
    where
        Q: Ord + ?Sized,
        F: Fn(&K) -> &Q,
    {
        let mut values = Vec::new();
        collect_equal(self.root.as_deref(), needle, &project, &mut values);
        values
    }

    pub(crate) fn values_range_by<Q, F>(
        &self,
        lower: std::ops::Bound<&Q>,
        upper: std::ops::Bound<&Q>,
        project: F,
    ) -> Vec<V>
    where
        Q: Ord + ?Sized,
        F: Fn(&K) -> &Q,
    {
        let mut values = Vec::new();
        collect_range(self.root.as_deref(), lower, upper, &project, &mut values);
        values
    }

    pub(crate) fn entries_range(
        &self,
        lower: std::ops::Bound<&K>,
        upper: std::ops::Bound<&K>,
        limit: usize,
    ) -> Vec<(K, V)> {
        let mut entries = Vec::with_capacity(limit.min(self.len));
        collect_entries_range(self.root.as_deref(), lower, upper, limit, &mut entries);
        entries
    }

    pub(crate) fn range_cursor(
        &self,
        lower: std::ops::Bound<K>,
        upper: std::ops::Bound<K>,
    ) -> PersistentMapRangeCursor<'_, K, V> {
        let mut stack = Vec::new();
        let mut current = self.root.as_deref();
        while let Some(node) = current {
            let before_lower = match &lower {
                std::ops::Bound::Included(lower) => &node.key < lower,
                std::ops::Bound::Excluded(lower) => &node.key <= lower,
                std::ops::Bound::Unbounded => false,
            };
            if before_lower {
                current = node.right.as_deref();
            } else {
                stack.push(node);
                current = node.left.as_deref();
            }
        }
        PersistentMapRangeCursor { stack, upper }
    }
}

impl<K, V> Iterator for PersistentMapRangeCursor<'_, K, V>
where
    K: Clone + Ord,
    V: Clone,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;
        let before_upper = match &self.upper {
            std::ops::Bound::Included(upper) => &node.key <= upper,
            std::ops::Bound::Excluded(upper) => &node.key < upper,
            std::ops::Bound::Unbounded => true,
        };
        if !before_upper {
            self.stack.clear();
            return None;
        }
        let mut current = node.right.as_deref();
        while let Some(next) = current {
            self.stack.push(next);
            current = next.left.as_deref();
        }
        Some((node.key.clone(), node.value.clone()))
    }
}

fn height<K, V>(node: Option<&Arc<Node<K, V>>>) -> u8 {
    node.map_or(0, |node| node.height)
}

fn node<K, V>(
    key: K,
    value: V,
    left: Option<Arc<Node<K, V>>>,
    right: Option<Arc<Node<K, V>>>,
) -> Arc<Node<K, V>> {
    Arc::new(Node {
        key,
        value,
        height: 1 + height(left.as_ref()).max(height(right.as_ref())),
        left,
        right,
    })
}

fn balance<K, V>(
    key: K,
    value: V,
    mut left: Option<Arc<Node<K, V>>>,
    mut right: Option<Arc<Node<K, V>>>,
) -> Arc<Node<K, V>>
where
    K: Clone,
    V: Clone,
{
    let balance = i16::from(height(left.as_ref())) - i16::from(height(right.as_ref()));
    if balance > 1 {
        let left_root = left.as_ref().expect("left-heavy AVL node has a left child");
        if height(left_root.left.as_ref()) < height(left_root.right.as_ref()) {
            left = Some(rotate_left(Arc::clone(left_root)));
        }
        return rotate_right(node(key, value, left, right));
    }
    if balance < -1 {
        let right_root = right
            .as_ref()
            .expect("right-heavy AVL node has a right child");
        if height(right_root.right.as_ref()) < height(right_root.left.as_ref()) {
            right = Some(rotate_right(Arc::clone(right_root)));
        }
        return rotate_left(node(key, value, left, right));
    }
    node(key, value, left, right)
}

fn rotate_left<K: Clone, V: Clone>(root: Arc<Node<K, V>>) -> Arc<Node<K, V>> {
    let pivot = root
        .right
        .as_ref()
        .expect("left rotation requires right child");
    let left = node(
        root.key.clone(),
        root.value.clone(),
        root.left.clone(),
        pivot.left.clone(),
    );
    node(
        pivot.key.clone(),
        pivot.value.clone(),
        Some(left),
        pivot.right.clone(),
    )
}

fn rotate_right<K: Clone, V: Clone>(root: Arc<Node<K, V>>) -> Arc<Node<K, V>> {
    let pivot = root
        .left
        .as_ref()
        .expect("right rotation requires left child");
    let right = node(
        root.key.clone(),
        root.value.clone(),
        pivot.right.clone(),
        root.right.clone(),
    );
    node(
        pivot.key.clone(),
        pivot.value.clone(),
        pivot.left.clone(),
        Some(right),
    )
}

fn insert<K, V>(root: Option<&Arc<Node<K, V>>>, key: K, value: V) -> (Arc<Node<K, V>>, bool)
where
    K: Clone + Ord,
    V: Clone,
{
    let Some(root) = root else {
        return (node(key, value, None, None), false);
    };
    match key.cmp(&root.key) {
        Ordering::Less => {
            let (left, replaced) = insert(root.left.as_ref(), key, value);
            (
                balance(
                    root.key.clone(),
                    root.value.clone(),
                    Some(left),
                    root.right.clone(),
                ),
                replaced,
            )
        }
        Ordering::Greater => {
            let (right, replaced) = insert(root.right.as_ref(), key, value);
            (
                balance(
                    root.key.clone(),
                    root.value.clone(),
                    root.left.clone(),
                    Some(right),
                ),
                replaced,
            )
        }
        Ordering::Equal => (
            node(key, value, root.left.clone(), root.right.clone()),
            true,
        ),
    }
}

fn remove<K, V, Q>(root: Option<&Arc<Node<K, V>>>, key: &Q) -> (Option<Arc<Node<K, V>>>, bool)
where
    K: Borrow<Q> + Clone + Ord,
    V: Clone,
    Q: Ord + ?Sized,
{
    let Some(root) = root else {
        return (None, false);
    };
    match key.cmp(root.key.borrow()) {
        Ordering::Less => {
            let (left, removed) = remove(root.left.as_ref(), key);
            if !removed {
                return (Some(Arc::clone(root)), false);
            }
            (
                Some(balance(
                    root.key.clone(),
                    root.value.clone(),
                    left,
                    root.right.clone(),
                )),
                true,
            )
        }
        Ordering::Greater => {
            let (right, removed) = remove(root.right.as_ref(), key);
            if !removed {
                return (Some(Arc::clone(root)), false);
            }
            (
                Some(balance(
                    root.key.clone(),
                    root.value.clone(),
                    root.left.clone(),
                    right,
                )),
                true,
            )
        }
        Ordering::Equal => match (&root.left, &root.right) {
            (None, _) => (root.right.clone(), true),
            (_, None) => (root.left.clone(), true),
            (Some(_), Some(right)) => {
                let successor = leftmost(right);
                let (new_right, removed) = remove::<K, V, K>(root.right.as_ref(), &successor.key);
                debug_assert!(removed);
                (
                    Some(balance(
                        successor.key.clone(),
                        successor.value.clone(),
                        root.left.clone(),
                        new_right,
                    )),
                    true,
                )
            }
        },
    }
}

fn leftmost<K, V>(mut node: &Arc<Node<K, V>>) -> &Arc<Node<K, V>> {
    while let Some(left) = &node.left {
        node = left;
    }
    node
}

fn collect_values<K, V: Clone>(node: Option<&Node<K, V>>, values: &mut Vec<V>) {
    let Some(node) = node else { return };
    collect_values(node.left.as_deref(), values);
    values.push(node.value.clone());
    collect_values(node.right.as_deref(), values);
}

fn collect_equal<K, V, Q, F>(
    node: Option<&Node<K, V>>,
    needle: &Q,
    project: &F,
    values: &mut Vec<V>,
) where
    V: Clone,
    Q: Ord + ?Sized,
    F: Fn(&K) -> &Q,
{
    let Some(node) = node else { return };
    match needle.cmp(project(&node.key)) {
        Ordering::Less => collect_equal(node.left.as_deref(), needle, project, values),
        Ordering::Greater => collect_equal(node.right.as_deref(), needle, project, values),
        Ordering::Equal => {
            collect_equal(node.left.as_deref(), needle, project, values);
            values.push(node.value.clone());
            collect_equal(node.right.as_deref(), needle, project, values);
        }
    }
}

fn collect_range<K, V, Q, F>(
    node: Option<&Node<K, V>>,
    lower: std::ops::Bound<&Q>,
    upper: std::ops::Bound<&Q>,
    project: &F,
    values: &mut Vec<V>,
) where
    V: Clone,
    Q: Ord + ?Sized,
    F: Fn(&K) -> &Q,
{
    let Some(node) = node else { return };
    let projected = project(&node.key);
    let below_lower = match lower {
        std::ops::Bound::Unbounded => false,
        std::ops::Bound::Included(value) => projected < value,
        std::ops::Bound::Excluded(value) => projected <= value,
    };
    let above_upper = match upper {
        std::ops::Bound::Unbounded => false,
        std::ops::Bound::Included(value) => projected > value,
        std::ops::Bound::Excluded(value) => projected >= value,
    };
    if !below_lower {
        collect_range(node.left.as_deref(), lower, upper, project, values);
    }
    if !below_lower && !above_upper {
        values.push(node.value.clone());
    }
    if !above_upper {
        collect_range(node.right.as_deref(), lower, upper, project, values);
    }
}

fn collect_entries_range<K, V>(
    node: Option<&Node<K, V>>,
    lower: std::ops::Bound<&K>,
    upper: std::ops::Bound<&K>,
    limit: usize,
    entries: &mut Vec<(K, V)>,
) where
    K: Clone + Ord,
    V: Clone,
{
    if entries.len() == limit {
        return;
    }
    let Some(node) = node else { return };
    let below_lower = match lower {
        std::ops::Bound::Unbounded => false,
        std::ops::Bound::Included(value) => &node.key < value,
        std::ops::Bound::Excluded(value) => &node.key <= value,
    };
    let above_upper = match upper {
        std::ops::Bound::Unbounded => false,
        std::ops::Bound::Included(value) => &node.key > value,
        std::ops::Bound::Excluded(value) => &node.key >= value,
    };
    if !below_lower {
        collect_entries_range(node.left.as_deref(), lower, upper, limit, entries);
    }
    if entries.len() < limit && !below_lower && !above_upper {
        entries.push((node.key.clone(), node.value.clone()));
    }
    if entries.len() < limit && !above_upper {
        collect_entries_range(node.right.as_deref(), lower, upper, limit, entries);
    }
}

#[cfg(test)]
mod tests {
    use super::PersistentMap;
    use std::ops::Bound;

    #[test]
    fn mutations_share_snapshots_and_keep_sorted_ranges() {
        let base = PersistentMap::from_sorted((0..100).map(|n| (n, n * 10)).collect());
        let changed = base.insert(50, 999).remove(&75).insert(101, 1_010);

        assert_eq!(base.get(&50), Some(&500));
        assert_eq!(base.get(&75), Some(&750));
        assert_eq!(changed.get(&50), Some(&999));
        assert_eq!(changed.get(&75), None);
        assert_eq!(changed.len(), 100);
        assert_eq!(
            changed.values_range_by(Bound::Included(&98), Bound::Unbounded, |key| key,),
            vec![980, 990, 1_010]
        );
        assert_eq!(
            changed.entries_range(Bound::Included(&48), Bound::Excluded(&53), 3),
            vec![(48, 480), (49, 490), (50, 999)]
        );
        assert!(
            changed
                .entries_range(Bound::Unbounded, Bound::Unbounded, 0)
                .is_empty()
        );
        assert_eq!(
            changed
                .range_cursor(Bound::Included(48), Bound::Excluded(53))
                .collect::<Vec<_>>(),
            vec![(48, 480), (49, 490), (50, 999), (51, 510), (52, 520)]
        );
    }
}
