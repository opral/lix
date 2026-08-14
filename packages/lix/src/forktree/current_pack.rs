use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::storage::StorageError;

use super::codec::{Decoder, Encoder, corruption};
use super::model::{CommitId, StatePageLocation};
use super::object::{ObjectDomain, ObjectId, decode_object, encode_object};
use super::state::{StateValue, decode_current_state_value, encode_current_state_value};

pub(crate) const CURRENT_STATE_PACK_MAX_ROWS: usize = 128;
const CURRENT_STATE_PACK_MAX_DECODED_BYTES: usize = 8 * 1024 * 1024;
const CURRENT_STATE_PACK_BODY_PREFIX_BYTES: usize = 16 + 1 + 4;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CurrentStatePackRowV1 {
    pub(crate) encoded_key: Vec<u8>,
    pub(crate) value: StateValue,
    pub(crate) history_page_object_id: ObjectId,
    pub(crate) history_page_ordinal: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CurrentStatePackV1 {
    pub(crate) owner_commit_id: CommitId,
    pub(crate) global: bool,
    pub(crate) rows: Vec<CurrentStatePackRowV1>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CurrentStatePackLocation {
    pub(crate) pack_object_id: ObjectId,
    pub(crate) pack_ordinal: u32,
}

#[derive(Debug)]
pub(crate) struct PreparedCurrentStatePacks {
    pub(crate) objects: Vec<(ObjectId, Bytes)>,
    pub(crate) locations: BTreeMap<Vec<u8>, CurrentStatePackLocation>,
}

impl CurrentStatePackV1 {
    pub(crate) fn encode(&self) -> Result<(ObjectId, Bytes), StorageError> {
        self.validate()?;
        let mut body = Encoder::default();
        body.fixed(self.owner_commit_id.as_bytes());
        body.u8(u8::from(self.global));
        body.u32(
            u32::try_from(self.rows.len())
                .map_err(|_| corruption("current-state pack row count exceeds u32"))?,
        );
        for row in &self.rows {
            body.bytes(&row.encoded_key)?;
            body.bytes(
                &encode_current_state_value(&row.value)
                    .map_err(|error| corruption(error.to_string()))?,
            )?;
            body.fixed(row.history_page_object_id.as_bytes());
            body.u32(row.history_page_ordinal);
        }
        let body = body.into_vec();
        if body.len() > CURRENT_STATE_PACK_MAX_DECODED_BYTES {
            return Err(corruption("current-state pack exceeds its byte bound"));
        }
        let compressed = crate::compression::compress_zstd_level_1(&body).map_err(|error| {
            corruption(format!("current-state pack compression failed: {error}"))
        })?;
        encode_object(ObjectDomain::CurrentStatePackV1, |encoder| {
            encoder.u32(
                u32::try_from(body.len())
                    .map_err(|_| corruption("current-state pack length exceeds u32"))?,
            );
            encoder.bytes(&compressed)
        })
    }

    pub(crate) fn decode(id: ObjectId, bytes: &[u8]) -> Result<Self, StorageError> {
        let mut decoder = decode_object(id, ObjectDomain::CurrentStatePackV1, bytes)?;
        let decoded_len = decoder.usize("current-state pack decoded length")?;
        if decoded_len == 0 || decoded_len > CURRENT_STATE_PACK_MAX_DECODED_BYTES {
            return Err(corruption(
                "current-state pack decoded length exceeds its bound",
            ));
        }
        let compressed = decoder.bytes("compressed current-state pack")?;
        decoder.finish()?;
        let body =
            crate::compression::decompress_zstd(&compressed, decoded_len).map_err(|error| {
                corruption(format!("current-state pack decompression failed: {error}"))
            })?;
        if body.len() != decoded_len {
            return Err(corruption(
                "current-state pack decoded length is inconsistent",
            ));
        }
        let mut body = Decoder::after_prefix(&body, &[])?;
        let owner_commit_id = CommitId::from_bytes(body.fixed()?);
        let global = match body.u8()? {
            0 => false,
            1 => true,
            tag => {
                return Err(corruption(format!(
                    "current-state pack has invalid global tag {tag}"
                )));
            }
        };
        let count = body.usize("current-state pack row count")?;
        if count == 0 || count > CURRENT_STATE_PACK_MAX_ROWS {
            return Err(corruption("current-state pack row count exceeds its bound"));
        }
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            let encoded_key = body.bytes("current-state pack key")?;
            let value = decode_current_state_value(&body.bytes("current-state pack value")?)
                .map_err(|error| corruption(error.to_string()))?;
            let history_page_object_id = ObjectId::from_bytes(body.fixed()?);
            let history_page_ordinal = body.u32()?;
            rows.push(CurrentStatePackRowV1 {
                encoded_key,
                value,
                history_page_object_id,
                history_page_ordinal,
            });
        }
        body.finish()?;
        let value = Self {
            owner_commit_id,
            global,
            rows,
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn object_edges(&self) -> impl Iterator<Item = (ObjectId, ObjectDomain)> + '_ {
        self.rows.iter().flat_map(|row| {
            std::iter::once((row.history_page_object_id, ObjectDomain::CommitChangePageV2)).chain(
                row.value
                    .blob_manifest_object_ids
                    .iter()
                    .copied()
                    .map(|id| (id, ObjectDomain::BlobManifest)),
            )
        })
    }

    fn validate(&self) -> Result<(), StorageError> {
        if self.rows.is_empty() || self.rows.len() > CURRENT_STATE_PACK_MAX_ROWS {
            return Err(corruption("current-state pack row count exceeds its bound"));
        }
        let mut prior = None::<&[u8]>;
        let mut history_locations = BTreeSet::new();
        for row in &self.rows {
            if row.encoded_key.is_empty() {
                return Err(corruption("current-state pack contains an empty state key"));
            }
            if prior.is_some_and(|prior| prior >= row.encoded_key.as_slice()) {
                return Err(corruption(
                    "current-state pack keys are not strictly ordered",
                ));
            }
            prior = Some(&row.encoded_key);
            if *row.value.commit_id.as_uuid().as_bytes() != *self.owner_commit_id.as_bytes() {
                return Err(corruption(
                    "current-state pack row commit differs from its owner",
                ));
            }
            if let super::state::StateCell::NativeRow(native) = &row.value.cell
                && native.global != self.global
            {
                return Err(corruption(
                    "current-state native row domain differs from its pack",
                ));
            }
            if row.history_page_object_id == ObjectId::ZERO {
                return Err(corruption(
                    "current-state pack contains a zero history page id",
                ));
            }
            if !history_locations.insert((row.history_page_object_id, row.history_page_ordinal)) {
                return Err(corruption(
                    "current-state pack repeats a history page location",
                ));
            }
        }
        Ok(())
    }
}

pub(crate) fn encode_current_state_packs(
    owner_commit_id: CommitId,
    global: bool,
    mut rows: Vec<(Vec<u8>, StateValue, StatePageLocation)>,
) -> Result<PreparedCurrentStatePacks, StorageError> {
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    let mut objects = Vec::new();
    let mut locations = BTreeMap::new();
    let mut pack_rows = Vec::with_capacity(CURRENT_STATE_PACK_MAX_ROWS);
    let mut decoded_bytes = CURRENT_STATE_PACK_BODY_PREFIX_BYTES;
    for (encoded_key, value, history) in rows {
        let encoded_value =
            encode_current_state_value(&value).map_err(|error| corruption(error.to_string()))?;
        let row_bytes = 4_usize
            .checked_add(encoded_key.len())
            .and_then(|length| length.checked_add(4))
            .and_then(|length| length.checked_add(encoded_value.len()))
            .and_then(|length| length.checked_add(32 + 4))
            .ok_or_else(|| corruption("current-state pack row length overflows"))?;
        if CURRENT_STATE_PACK_BODY_PREFIX_BYTES
            .checked_add(row_bytes)
            .is_none_or(|length| length > CURRENT_STATE_PACK_MAX_DECODED_BYTES)
        {
            return Err(corruption(
                "one current-state row exceeds the pack byte bound",
            ));
        }
        if !pack_rows.is_empty()
            && (pack_rows.len() == CURRENT_STATE_PACK_MAX_ROWS
                || decoded_bytes
                    .checked_add(row_bytes)
                    .is_none_or(|length| length > CURRENT_STATE_PACK_MAX_DECODED_BYTES))
        {
            stage_pack(
                owner_commit_id,
                global,
                std::mem::take(&mut pack_rows),
                &mut objects,
                &mut locations,
            )?;
            decoded_bytes = CURRENT_STATE_PACK_BODY_PREFIX_BYTES;
        }
        decoded_bytes += row_bytes;
        pack_rows.push(CurrentStatePackRowV1 {
            encoded_key,
            value,
            history_page_object_id: history.page_object_id,
            history_page_ordinal: history.page_ordinal,
        });
    }
    if !pack_rows.is_empty() {
        stage_pack(
            owner_commit_id,
            global,
            pack_rows,
            &mut objects,
            &mut locations,
        )?;
    }
    Ok(PreparedCurrentStatePacks { objects, locations })
}

fn stage_pack(
    owner_commit_id: CommitId,
    global: bool,
    rows: Vec<CurrentStatePackRowV1>,
    objects: &mut Vec<(ObjectId, Bytes)>,
    locations: &mut BTreeMap<Vec<u8>, CurrentStatePackLocation>,
) -> Result<(), StorageError> {
    let pack = CurrentStatePackV1 {
        owner_commit_id,
        global,
        rows,
    };
    let (pack_object_id, bytes) = pack.encode()?;
    for (pack_ordinal, row) in pack.rows.iter().enumerate() {
        if locations
            .insert(
                row.encoded_key.clone(),
                CurrentStatePackLocation {
                    pack_object_id,
                    pack_ordinal: u32::try_from(pack_ordinal)
                        .expect("current-state pack row bound fits u32"),
                },
            )
            .is_some()
        {
            return Err(corruption("current-state pack repeats a state key"));
        }
    }
    objects.push((pack_object_id, bytes));
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::common::LixTimestamp;

    use super::*;
    use crate::entity_pk::EntityPk;
    use crate::forktree::state::{StateCell, StateKeyRef, encode_state_key};

    fn row(label: &str, byte: u8, history_byte: u8, history_ordinal: u32) -> CurrentStatePackRowV1 {
        let entity_pk = EntityPk::single(label);
        let schema = crate::native_row::seed_schema("lix_key_value").expect("key-value schema");
        let native = crate::native_row::encode(
            &schema,
            &entity_pk,
            false,
            None,
            &serde_json::json!({"key": label, "value": format!("value-{byte}")}),
        )
        .expect("native test row");
        CurrentStatePackRowV1 {
            encoded_key: encode_state_key(StateKeyRef {
                schema_key: "lix_key_value",
                file_id: None,
                entity_pk: &entity_pk,
            }),
            value: StateValue {
                change_id: crate::changelog::ChangeId::new(uuid::Uuid::from_bytes([byte; 16])),
                commit_id: crate::changelog::CommitId::new(uuid::Uuid::from_bytes([0x41; 16])),
                created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(2),
                cell: StateCell::NativeRow(native),
                metadata: Some("metadata".into()),
                origin_key: Some("origin".to_owned()),
                blob_manifest_object_ids: vec![ObjectId::from_bytes([byte.wrapping_add(1); 32])],
            },
            history_page_object_id: ObjectId::from_bytes([history_byte; 32]),
            history_page_ordinal: history_ordinal,
        }
    }

    #[test]
    fn current_state_pack_round_trips_and_rejects_identity_substitution() {
        let pack = CurrentStatePackV1 {
            owner_commit_id: CommitId::from_bytes([0x41; 16]),
            global: false,
            rows: vec![
                row("key-a", 1, 0x51, 3),
                row("key-b", 2, 0x52, 4),
            ],
        };
        let (id, bytes) = pack.encode().expect("current-state pack");
        assert_eq!(CurrentStatePackV1::decode(id, &bytes).unwrap(), pack);
        assert!(
            CurrentStatePackV1::decode(ObjectId::from_bytes([0x99; 32]), &bytes).is_err(),
            "a content-valid pack must not be accepted under another ObjectId"
        );
        let mut substituted = bytes.to_vec();
        let index = substituted.len() / 2;
        substituted[index] ^= 1;
        assert!(
            CurrentStatePackV1::decode(id, &substituted).is_err(),
            "same-size pack substitution must fail content authentication"
        );
    }

    #[test]
    fn current_state_pack_rejects_wrong_owner_order_and_history_identity() {
        let canonical = row("key-a", 1, 0x51, 3);
        let mut wrong_owner = canonical.clone();
        wrong_owner.value.commit_id =
            crate::changelog::CommitId::new(uuid::Uuid::from_bytes([0x42; 16]));
        assert!(
            CurrentStatePackV1 {
                owner_commit_id: CommitId::from_bytes([0x41; 16]),
                global: false,
                rows: vec![wrong_owner],
            }
            .encode()
            .is_err()
        );
        let mut duplicate = canonical.clone();
        duplicate.history_page_ordinal = canonical.history_page_ordinal;
        assert!(
            CurrentStatePackV1 {
                owner_commit_id: CommitId::from_bytes([0x41; 16]),
                global: false,
                rows: vec![canonical.clone(), duplicate],
            }
            .encode()
            .is_err(),
            "duplicate keys/history locations are not canonical"
        );
        let mut zero_history = canonical;
        zero_history.history_page_object_id = ObjectId::ZERO;
        assert!(
            CurrentStatePackV1 {
                owner_commit_id: CommitId::from_bytes([0x41; 16]),
                global: false,
                rows: vec![zero_history],
            }
            .encode()
            .is_err()
        );
    }
}
