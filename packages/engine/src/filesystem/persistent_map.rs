use std::borrow::Borrow;
use std::cmp::Ordering;
use std::sync::Arc;

/// Small immutable AVL map used by revisioned in-memory indexes.
///
/// Each mutation copies only the search path. Older readers retain their root,
/// while a committed generation shares every untouched subtree with it.
#[derive(Debug, Clone)]
pub(super) struct PersistentMap<K, V> {
    root: Option<Arc<Node<K, V>>>,
    len: usize,
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
    pub(super) fn from_sorted(entries: Vec<(K, V)>) -> Self {
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

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn get<Q>(&self, key: &Q) -> Option<&V>
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

    pub(super) fn insert(&self, key: K, value: V) -> Self {
        let (root, replaced) = insert(self.root.as_ref(), key, value);
        Self {
            root: Some(root),
            len: self.len + usize::from(!replaced),
        }
    }

    pub(super) fn remove<Q>(&self, key: &Q) -> Self
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

    pub(super) fn values(&self) -> Vec<V> {
        let mut values = Vec::with_capacity(self.len);
        collect_values(self.root.as_deref(), &mut values);
        values
    }

    /// Collects values whose key projection equals `needle` without scanning
    /// either side of the matching key range.
    pub(super) fn values_equal_by<Q, F>(&self, needle: &Q, project: F) -> Vec<V>
    where
        Q: Ord + ?Sized,
        F: Fn(&K) -> &Q,
    {
        let mut values = Vec::new();
        collect_equal(self.root.as_deref(), needle, &project, &mut values);
        values
    }

    pub(super) fn values_range_by<Q, F>(
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

#[cfg(test)]
mod tests {
    use super::PersistentMap;

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
            changed.values_range_by(
                std::ops::Bound::Included(&98),
                std::ops::Bound::Unbounded,
                |key| key,
            ),
            vec![980, 990, 1_010]
        );
    }
}
