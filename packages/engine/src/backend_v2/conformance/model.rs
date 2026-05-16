use std::collections::BTreeMap;

use bytes::Bytes;

use crate::backend_v2::Key;

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

    pub fn iter(&self) -> impl Iterator<Item = (&Key, &Bytes)> {
        self.entries.iter()
    }
}
