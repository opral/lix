use std::collections::BTreeMap;
use std::ops::Bound;

use bytes::Bytes;

use crate::storage::{Key, KeyRange};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReferenceModel {
    entries: BTreeMap<Key, Bytes>,
}

impl ReferenceModel {
    pub fn get(&self, key: &Key) -> Option<&Bytes> {
        self.entries.get(key)
    }

    pub fn put(&mut self, key: Key, value: Bytes) {
        self.entries.insert(key, value);
    }

    pub fn delete(&mut self, key: &Key) {
        self.entries.remove(key);
    }

    pub fn delete_range(&mut self, range: &KeyRange) {
        self.entries.retain(|key, _| !range_contains(range, key));
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Key, &Bytes)> {
        self.entries.iter()
    }
}

fn range_contains(range: &KeyRange, key: &Key) -> bool {
    let lower_matches = match &range.lower {
        Bound::Included(lower) => key >= lower,
        Bound::Excluded(lower) => key > lower,
        Bound::Unbounded => true,
    };
    let upper_matches = match &range.upper {
        Bound::Included(upper) => key <= upper,
        Bound::Excluded(upper) => key < upper,
        Bound::Unbounded => true,
    };
    lower_matches && upper_matches
}
