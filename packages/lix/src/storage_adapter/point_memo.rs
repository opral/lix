//! Per-pinned-read memo for singleton control keys.
//!
//! A read scope is one coherent, immutable view: two reads of the same key
//! through the same scope must return the same bytes. That makes a memo
//! *inside* the scope sound without any freshness key — it cannot describe a
//! view other than the one it lives in, and it dies with that view. The commit
//! opens its own newer snapshot and therefore starts cold, which is exactly
//! what keeps a commit reading current state.
//!
//! It is deliberately narrow rather than a blanket read cache: only spaces in
//! [`MEMOIZED_SPACE_IDS`], only full-value single-request batches. Everything
//! else passes through untouched.
//!
//! This is not an authority. Callers that publish against a control still
//! retain the observed bytes and still fence the write with a precondition;
//! the memo only removes a second identical read of one immutable view.

use std::sync::Mutex;

use bytes::Bytes;

use crate::storage::{
    CoreProjection, GetManyRequest, GetManyResult, GetOptions, Key, ProjectedValue, SpaceId,
    StorageSpace,
};

/// Enough for the branches one transaction touches; a wider fan-out stops
/// memoizing rather than growing.
const MAX_ENTRIES: usize = 32;

/// The spaces whose point reads this memo serves.
///
/// `branch::control`'s `branch_head_control_space_is_memoized` pins this id to
/// the space constant, so renumbering the space cannot silently turn the memo
/// off.
pub(crate) const MEMOIZED_SPACE_IDS: &[SpaceId] = &[
    // `crate::branch::BRANCH_HEAD_CONTROL_SPACE`, named by id because
    // `storage_adapter` sits below `branch` in the module layer order.
    SpaceId(0x0004_0020),
];

pub(crate) fn is_memoized(space: SpaceId) -> bool {
    MEMOIZED_SPACE_IDS.contains(&space)
}

#[derive(Debug, Default)]
pub(crate) struct PointValueMemo {
    entries: Mutex<Vec<(SpaceId, Bytes, Option<Bytes>)>>,
}

/// What a memoized batch still has to fetch, and where the answers go.
pub(crate) struct MemoPlan {
    space: StorageSpace,
    opts: GetOptions,
    keys: Vec<Key>,
    /// One slot per requested key in request order, duplicates included.
    /// `Some` is already answered; `None` is answered from `fetch_keys`.
    slots: Vec<Option<Option<Bytes>>>,
    fetch_keys: Vec<Key>,
}

impl MemoPlan {
    pub(crate) fn forwarded(&self) -> Option<GetManyRequest<'_>> {
        (!self.fetch_keys.is_empty()).then(|| GetManyRequest {
            space: self.space,
            keys: &self.fetch_keys,
            opts: self.opts,
        })
    }
}

impl PointValueMemo {
    fn get(&self, space: SpaceId, key: &Bytes) -> Option<Option<Bytes>> {
        let entries = self.entries.lock().expect("point memo lock poisoned");
        entries
            .iter()
            .find(|(entry_space, entry_key, _)| *entry_space == space && entry_key == key)
            .map(|(_, _, value)| value.clone())
    }

    fn insert(&self, space: SpaceId, key: Bytes, value: Option<Bytes>) {
        let mut entries = self.entries.lock().expect("point memo lock poisoned");
        if entries.len() >= MAX_ENTRIES
            || entries
                .iter()
                .any(|(entry_space, entry_key, _)| *entry_space == space && *entry_key == key)
        {
            return;
        }
        entries.push((space, key, value));
    }

    /// Plans a batch against this view's memo. `None` means "not ours" and the
    /// batch goes to storage exactly as it arrived.
    pub(crate) fn plan(&self, requests: &[GetManyRequest<'_>]) -> Option<MemoPlan> {
        let [request] = requests else {
            return None;
        };
        if !is_memoized(request.space.id) || request.opts.projection != CoreProjection::FullValue {
            return None;
        }
        let mut slots = Vec::with_capacity(request.keys.len());
        let mut fetch_keys: Vec<Key> = Vec::new();
        for key in request.keys {
            match self.get(request.space.id, &key.0) {
                Some(value) => slots.push(Some(value)),
                None => {
                    slots.push(None);
                    // A key repeated inside one batch is fetched once.
                    if !fetch_keys.iter().any(|entry| entry.0 == key.0) {
                        fetch_keys.push(key.clone());
                    }
                }
            }
        }
        Some(MemoPlan {
            space: request.space,
            opts: request.opts,
            keys: request.keys.to_vec(),
            slots,
            fetch_keys,
        })
    }

    /// Splices fetched values back into request order, memoizing what they
    /// prove. Request cardinality and order are preserved, duplicates included.
    pub(crate) fn finish(&self, plan: MemoPlan, fetched: GetManyResult) -> GetManyResult {
        for (key, value) in plan.fetch_keys.iter().zip(&fetched.values) {
            match value {
                Some(ProjectedValue::FullValue(bytes)) => {
                    self.insert(plan.space.id, key.0.clone(), Some(bytes.clone()));
                }
                None => self.insert(plan.space.id, key.0.clone(), None),
                // Neither a value nor a proven absence: left unmemoized so the
                // caller sees exactly what storage returned.
                Some(ProjectedValue::KeyOnly) => {}
            }
        }
        let values = plan
            .slots
            .iter()
            .zip(&plan.keys)
            .map(|(slot, key)| match slot {
                Some(value) => value.clone().map(ProjectedValue::FullValue),
                None => plan
                    .fetch_keys
                    .iter()
                    .position(|entry| entry.0 == key.0)
                    .and_then(|index| fetched.values.get(index).cloned().flatten()),
            })
            .collect::<Vec<_>>();
        GetManyResult { values }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request<'a>(space: StorageSpace, keys: &'a [Key]) -> GetManyRequest<'a> {
        GetManyRequest {
            space,
            keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }
    }

    fn memoized_space() -> StorageSpace {
        crate::branch::BRANCH_HEAD_CONTROL_SPACE
    }

    #[test]
    fn a_cold_memo_forwards_every_key_and_then_answers_from_itself() {
        let memo = PointValueMemo::default();
        let keys = vec![
            Key(Bytes::from_static(b"main")),
            Key(Bytes::from_static(b"global")),
        ];
        let plan = memo
            .plan(&[request(memoized_space(), &keys)])
            .expect("memoized space plans");
        assert_eq!(plan.fetch_keys.len(), 2, "a cold memo must fetch both keys");
        let result = memo.finish(
            plan,
            GetManyResult {
                values: vec![
                    Some(ProjectedValue::FullValue(Bytes::from_static(b"a"))),
                    None,
                ],
            },
        );
        assert_eq!(result.values.len(), 2);
        assert_eq!(
            result.values[0],
            Some(ProjectedValue::FullValue(Bytes::from_static(b"a")))
        );
        assert_eq!(result.values[1], None, "an absent key stays absent");

        let warm = memo
            .plan(&[request(memoized_space(), &keys)])
            .expect("memoized space plans");
        assert!(
            warm.fetch_keys.is_empty(),
            "the second read of one pinned view must reach storage for nothing"
        );
        let warm_result = memo.finish(warm, GetManyResult { values: Vec::new() });
        assert_eq!(
            warm_result.values[0],
            Some(ProjectedValue::FullValue(Bytes::from_static(b"a")))
        );
        assert_eq!(
            warm_result.values[1], None,
            "a proven absence is answered as absent, not refetched"
        );
    }

    #[test]
    fn duplicate_keys_in_one_batch_are_fetched_once_and_answered_twice() {
        let memo = PointValueMemo::default();
        let keys = vec![
            Key(Bytes::from_static(b"main")),
            Key(Bytes::from_static(b"main")),
        ];
        let plan = memo
            .plan(&[request(memoized_space(), &keys)])
            .expect("memoized space plans");
        assert_eq!(plan.fetch_keys.len(), 1);
        let result = memo.finish(
            plan,
            GetManyResult {
                values: vec![Some(ProjectedValue::FullValue(Bytes::from_static(b"a")))],
            },
        );
        assert_eq!(
            result.values.len(),
            2,
            "request cardinality is preserved including duplicates"
        );
        assert_eq!(result.values[0], result.values[1]);
    }

    #[test]
    fn other_spaces_and_key_only_projections_are_not_ours() {
        let memo = PointValueMemo::default();
        let keys = vec![Key(Bytes::from_static(b"k"))];
        assert!(
            memo.plan(&[request(crate::changelog::COMMIT_SPACE, &keys)])
                .is_none(),
            "an unlisted space passes through"
        );
        let key_only = GetManyRequest {
            space: memoized_space(),
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::KeyOnly,
            },
        };
        assert!(
            memo.plan(&[key_only]).is_none(),
            "a key-only projection carries no value to memoize"
        );
        assert!(
            memo.plan(&[
                request(memoized_space(), &keys),
                request(memoized_space(), &keys)
            ])
            .is_none(),
            "multi-request batches pass through rather than being spliced"
        );
    }

    #[test]
    fn the_memo_is_bounded() {
        let memo = PointValueMemo::default();
        let space = memoized_space().id;
        for index in 0..(MAX_ENTRIES + 16) {
            memo.insert(space, Bytes::from(index.to_be_bytes().to_vec()), None);
        }
        let len = memo.entries.lock().expect("point memo lock poisoned").len();
        assert_eq!(len, MAX_ENTRIES);
    }
}
