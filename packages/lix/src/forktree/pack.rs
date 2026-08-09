use bytes::Bytes;

use crate::storage::StorageError;

use super::codec::corruption;
use super::object::{ObjectDomain, ObjectId, decode_id, decode_object, encode_id, encode_object};
use super::state::{StateKey, StateValue, decode_state_key, decode_state_value};

/// The authenticated current-state manifest. The manifest binds the exact
/// state root selected by the repository/branch snapshot and authenticates an
/// ordered set of immutable row pages. Pages are deliberately separate so a
/// one-row state edit can reuse every untouched page without retaining a
/// second current-state authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EntityStatePackV1 {
    pub(crate) state_root: ObjectId,
    pub(crate) pages: Vec<EntityStatePackPageRef>,
    pub(crate) row_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EntityStatePackPageRef {
    pub(crate) object_id: ObjectId,
    pub(crate) first_key: StateKey,
    pub(crate) last_key: StateKey,
    pub(crate) row_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct EntityStatePackPageV1 {
    pub(crate) rows: Vec<(StateKey, StateValue)>,
}

pub(crate) struct EncodedEntityStatePack {
    pub(crate) root_id: ObjectId,
    pub(crate) root_bytes: Bytes,
    pub(crate) page_objects: Vec<(ObjectId, Bytes)>,
}

impl EntityStatePackV1 {
    pub(crate) const MAX_ROWS: usize = 1_000_000;
    pub(crate) const PAGE_ROWS: usize = 128;

    pub(crate) fn from_encoded_rows(
        state_root: ObjectId,
        rows: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<EncodedEntityStatePack, StorageError> {
        let rows = rows
            .iter()
            .map(|(key, value)| {
                Ok((
                    decode_state_key(key).map_err(|error| corruption(error.to_string()))?,
                    decode_state_value(value).map_err(|error| corruption(error.to_string()))?,
                ))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        Self::from_rows(state_root, rows)
    }

    pub(crate) fn from_rows(
        state_root: ObjectId,
        mut rows: Vec<(StateKey, StateValue)>,
    ) -> Result<EncodedEntityStatePack, StorageError> {
        if rows.len() > Self::MAX_ROWS {
            return Err(corruption(
                "entity state pack exceeds its bounded row contract",
            ));
        }
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        for pair in rows.windows(2) {
            if pair[0].0 == pair[1].0 {
                return Err(corruption(
                    "entity state pack contains duplicate state keys",
                ));
            }
        }
        Self::encode_pages(state_root, rows)
    }

    pub(crate) fn encode_pages(
        state_root: ObjectId,
        rows: Vec<(StateKey, StateValue)>,
    ) -> Result<EncodedEntityStatePack, StorageError> {
        let mut pages = Vec::new();
        let mut page_objects = Vec::new();
        for chunk in rows.chunks(Self::PAGE_ROWS) {
            let page = EntityStatePackPageV1 {
                rows: chunk.to_vec(),
            };
            let (page_id, page_bytes) = page.encode()?;
            let first_key = chunk
                .first()
                .ok_or_else(|| corruption("entity state pack created an empty page"))?
                .0
                .clone();
            let last_key = chunk
                .last()
                .ok_or_else(|| corruption("entity state pack created an empty page"))?
                .0
                .clone();
            pages.push(EntityStatePackPageRef {
                object_id: page_id,
                first_key,
                last_key,
                row_count: chunk.len(),
            });
            page_objects.push((page_id, page_bytes));
        }
        let pack = Self {
            state_root,
            pages,
            row_count: rows.len(),
        };
        let (root_id, root_bytes) = pack.encode_root()?;
        Ok(EncodedEntityStatePack {
            root_id,
            root_bytes,
            page_objects,
        })
    }

    pub(crate) fn encode_root(self) -> Result<(ObjectId, Bytes), StorageError> {
        if self.row_count > Self::MAX_ROWS {
            return Err(corruption(
                "entity state pack exceeds its bounded row contract",
            ));
        }
        if self.pages.len() > self.row_count.div_ceil(Self::PAGE_ROWS) {
            return Err(corruption("entity state pack has too many pages"));
        }
        for pair in self.pages.windows(2) {
            if pair[0].last_key >= pair[1].first_key {
                return Err(corruption("entity state pack page ranges overlap"));
            }
        }
        let (id, bytes) = encode_object(ObjectDomain::EntityStatePackV1, |encoder| {
            encode_id(encoder, self.state_root);
            encoder.u32(
                u32::try_from(self.row_count)
                    .map_err(|_| corruption("pack row count overflows u32"))?,
            );
            encoder.u32(
                u32::try_from(self.pages.len())
                    .map_err(|_| corruption("pack page count overflows u32"))?,
            );
            for page in self.pages {
                encode_id(encoder, page.object_id);
                encoder.u32(
                    u32::try_from(page.row_count)
                        .map_err(|_| corruption("pack page row count overflows u32"))?,
                );
                encoder.bytes(&super::state::encode_state_key(super::state::StateKeyRef {
                    schema_key: &page.first_key.schema_key,
                    file_id: page.first_key.file_id.as_deref(),
                    entity_pk: &page.first_key.entity_pk,
                }))?;
                encoder.bytes(&super::state::encode_state_key(super::state::StateKeyRef {
                    schema_key: &page.last_key.schema_key,
                    file_id: page.last_key.file_id.as_deref(),
                    entity_pk: &page.last_key.entity_pk,
                }))?;
            }
            Ok(())
        })?;
        Ok((id, bytes))
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::EntityStatePackV1, bytes)?;
        let state_root = decode_id(&mut decoder)?;
        if state_root == ObjectId::ZERO {
            return Err(corruption("entity state pack names the zero state root"));
        }
        let row_count = decoder.usize("entity state pack row count")?;
        let page_count = decoder.usize("entity state pack page count")?;
        if row_count > Self::MAX_ROWS
            || page_count > row_count.div_ceil(Self::PAGE_ROWS)
            || (row_count == 0 && page_count != 0)
        {
            return Err(corruption(
                "entity state pack page/count bounds are invalid",
            ));
        }
        let mut pages = Vec::with_capacity(page_count);
        for _ in 0..page_count {
            let object_id = decode_id(&mut decoder)?;
            let page_row_count = decoder.usize("entity state pack page row count")?;
            let first_key = decode_state_key(&decoder.bytes("entity state pack first key")?)
                .map_err(|error| corruption(error.to_string()))?;
            let last_key = decode_state_key(&decoder.bytes("entity state pack last key")?)
                .map_err(|error| corruption(error.to_string()))?;
            if first_key > last_key {
                return Err(corruption("entity state pack page range is inverted"));
            }
            pages.push(EntityStatePackPageRef {
                object_id,
                first_key,
                last_key,
                row_count: page_row_count,
            });
        }
        decoder.finish()?;
        for pair in pages.windows(2) {
            if pair[0].last_key >= pair[1].first_key {
                return Err(corruption("entity state pack page ranges overlap"));
            }
        }
        Ok(Self {
            state_root,
            pages,
            row_count,
        })
    }

    pub(crate) fn page_index_for_key(&self, key: &StateKey) -> usize {
        self.pages
            .partition_point(|page| page.last_key < *key)
            .min(self.pages.len().saturating_sub(1))
    }
}

impl EntityStatePackPageV1 {
    pub(crate) fn encode(self) -> Result<(ObjectId, Bytes), StorageError> {
        if self.rows.is_empty() || self.rows.len() > EntityStatePackV1::PAGE_ROWS {
            return Err(corruption("entity state pack page row bounds are invalid"));
        }
        for pair in self.rows.windows(2) {
            if pair[0].0 >= pair[1].0 {
                return Err(corruption("entity state pack page rows are not ordered"));
            }
        }
        let (id, bytes) = encode_object(ObjectDomain::EntityStatePackPageV1, |encoder| {
            encoder.u32(
                u32::try_from(self.rows.len())
                    .map_err(|_| corruption("pack page row count overflows u32"))?,
            );
            for (key, value) in self.rows {
                encoder.bytes(&super::state::encode_state_key(super::state::StateKeyRef {
                    schema_key: &key.schema_key,
                    file_id: key.file_id.as_deref(),
                    entity_pk: &key.entity_pk,
                }))?;
                let encoded_value = super::state::encode_state_value(super::state::StateValueRef {
                    change_id: value.change_id,
                    commit_id: value.commit_id,
                    created_at: value.created_at,
                    updated_at: value.updated_at,
                    cell: match &value.cell {
                        super::state::StateCell::Value(value) => {
                            super::state::StateCellRef::Value(value)
                        }
                        super::state::StateCell::Null => super::state::StateCellRef::Null,
                        super::state::StateCell::Tombstone => super::state::StateCellRef::Tombstone,
                    },
                    metadata: value.metadata.as_deref(),
                    origin_key: value.origin_key.as_deref(),
                    blob_manifest_object_ids: &value.blob_manifest_object_ids,
                })
                .map_err(|error| corruption(error.to_string()))?;
                encoder.bytes(&encoded_value)?;
            }
            Ok(())
        })?;
        Ok((id, bytes))
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::EntityStatePackPageV1, bytes)?;
        let count = decoder.usize("entity state pack page row count")?;
        if count == 0 || count > EntityStatePackV1::PAGE_ROWS {
            return Err(corruption("entity state pack page row count is invalid"));
        }
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            let key = decode_state_key(&decoder.bytes("entity state pack page key")?)
                .map_err(|error| corruption(error.to_string()))?;
            let value = decode_state_value(&decoder.bytes("entity state pack page value")?)
                .map_err(|error| corruption(error.to_string()))?;
            rows.push((key, value));
        }
        decoder.finish()?;
        for pair in rows.windows(2) {
            if pair[0].0 >= pair[1].0 {
                return Err(corruption("entity state pack page rows are not ordered"));
            }
        }
        Ok(Self { rows })
    }

    pub(crate) fn decode_snapshot_rows(
        id: ObjectId,
        bytes: &Bytes,
    ) -> Result<Vec<(StateKey, Option<Bytes>, bool)>, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::EntityStatePackPageV1, bytes)?;
        let count = decoder.usize("entity state pack page row count")?;
        if count == 0 || count > EntityStatePackV1::PAGE_ROWS {
            return Err(corruption("entity state pack page row count is invalid"));
        }
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            let key = decode_state_key(&decoder.bytes("entity state pack page key")?)
                .map_err(|error| corruption(error.to_string()))?;
            let value_bytes = decoder.bytes_ref("entity state pack page value")?;
            let projection = super::state::decode_state_snapshot_projection(value_bytes)
                .map_err(|error| corruption(error.to_string()))?;
            let value_start = value_bytes.as_ptr() as usize - bytes.as_ptr() as usize;
            let snapshot = projection
                .snapshot
                .map(|range| bytes.slice((value_start + range.start)..(value_start + range.end)));
            rows.push((key, snapshot, projection.deleted));
        }
        decoder.finish()?;
        for pair in rows.windows(2) {
            if pair[0].0 >= pair[1].0 {
                return Err(corruption("entity state pack page rows are not ordered"));
            }
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_key(value: &str) -> StateKey {
        StateKey {
            schema_key: "app.row".to_owned(),
            file_id: Some("file".to_owned()),
            entity_pk: crate::entity_pk::EntityPk::single(value),
        }
    }

    fn sample_value(byte: u8) -> StateValue {
        StateValue {
            change_id: crate::changelog::ChangeId::new(uuid::Uuid::from_bytes([byte; 16])),
            commit_id: crate::changelog::CommitId::new(uuid::Uuid::from_bytes(
                [byte.wrapping_add(1); 16],
            )),
            created_at: crate::common::LixTimestamp::from_unix_millis_utc_lossy(1),
            updated_at: crate::common::LixTimestamp::from_unix_millis_utc_lossy(2),
            cell: super::super::state::StateCell::Null,
            metadata: None,
            origin_key: None,
            blob_manifest_object_ids: Vec::new(),
        }
    }

    #[test]
    fn manifest_and_pages_bind_root_and_reject_duplicate_or_corrupt_rows() {
        let state_root = ObjectId::from_bytes([0x11; 32]);
        let rows = vec![
            (sample_key("a"), sample_value(1)),
            (sample_key("b"), sample_value(2)),
        ];
        let encoded = EntityStatePackV1::from_rows(state_root, rows.clone()).expect("pack");
        let manifest =
            EntityStatePackV1::decode(encoded.root_id, &encoded.root_bytes).expect("manifest");
        assert_eq!(manifest.state_root, state_root);
        assert_eq!(manifest.row_count, 2);
        assert_eq!(manifest.pages.len(), 1);
        let page_ref = &manifest.pages[0];
        let (_, page_bytes) = encoded.page_objects[0].clone();
        let page = EntityStatePackPageV1::decode(page_ref.object_id, &page_bytes).expect("page");
        assert_eq!(page.rows, rows);

        let mut corrupt = encoded.root_bytes.to_vec();
        corrupt.pop();
        assert!(EntityStatePackV1::decode(encoded.root_id, &corrupt).is_err());
        assert!(
            EntityStatePackV1::from_rows(
                state_root,
                vec![
                    (sample_key("a"), sample_value(1)),
                    (sample_key("a"), sample_value(2))
                ]
            )
            .is_err()
        );
    }
}
