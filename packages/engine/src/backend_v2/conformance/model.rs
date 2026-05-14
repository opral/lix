use std::collections::BTreeMap;

use bytes::Bytes;

use crate::backend_v2::{Key, SpaceId};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReferenceModel {
    entries: BTreeMap<(SpaceId, Key), Bytes>,
}

impl ReferenceModel {
    pub fn get(&self, space: SpaceId, key: &Key) -> Option<&Bytes> {
        self.entries.get(&(space, key.clone()))
    }

    pub fn put(&mut self, space: SpaceId, key: Key, value: Bytes) {
        self.entries.insert((space, key), value);
    }

    pub fn delete(&mut self, space: SpaceId, key: &Key) {
        self.entries.remove(&(space, key.clone()));
    }

    pub fn iter_space(
        &self,
        space: SpaceId,
    ) -> impl Iterator<Item = (&Key, &Bytes)> {
        self.entries
            .iter()
            .filter_map(move |((entry_space, key), value)| {
                (*entry_space == space).then_some((key, value))
            })
    }
}
