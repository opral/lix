use std::collections::{BTreeMap, HashMap};
#[cfg(test)]
use std::mem::size_of;
#[cfg(test)]
use std::ops::Range;

use crate::LixError;
use crate::changelog::{
    ChangeId, ChangeRecordProjection, CommitId, MaterializedChangePayload,
    materialize_known_change_payloads,
};
use crate::common::{LixTimestamp, SharedStr, StringDictionary, StringDictionaryBuilder};
use crate::entity_pk::EntityPk;
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::MaterializedTrackedStateRow;
use crate::tracked_state::types::{TrackedStateIndexValue, TrackedStateKey, TrackedStateKeyRef};

#[derive(Debug)]
struct MaterializedTrackedStateDescriptor {
    entity_pk: EntityPk,
    schema_key: u32,
    file_id: Option<u32>,
    snapshot_content: Option<SharedStr>,
    metadata: Option<SharedStr>,
    deleted: bool,
    created_at: LixTimestamp,
    updated_at: LixTimestamp,
    change_id: ChangeId,
    commit_id: CommitId,
}

/// Typed owner for historical tracked-state materialization.
///
/// Identity strings are dictionary encoded into one immutable byte arena,
/// fixed row metadata occupies one contiguous descriptor column, timestamps
/// and ids stay typed, and JSON slots retain their existing shared buffers.
/// The legacy owned row is constructed only by an explicit terminal adapter.
#[derive(Debug, Default)]
pub(crate) struct MaterializedTrackedStateBatch {
    strings: StringDictionary,
    rows: Vec<MaterializedTrackedStateDescriptor>,
}

impl MaterializedTrackedStateBatch {
    pub(crate) fn len(&self) -> usize {
        self.rows.len()
    }

    pub(crate) fn row(&self, index: usize) -> MaterializedTrackedStateRowRef<'_> {
        assert!(
            index < self.rows.len(),
            "tracked-state materialized row ordinal out of bounds"
        );
        MaterializedTrackedStateRowRef { batch: self, index }
    }

    pub(crate) fn iter(&self) -> MaterializedTrackedStateBatchIter<'_> {
        MaterializedTrackedStateBatchIter {
            batch: self,
            next: 0,
        }
    }

    pub(crate) fn into_rows(self) -> Vec<MaterializedTrackedStateRow> {
        self.iter()
            .map(MaterializedTrackedStateRowRef::to_owned)
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn from_rows(rows: Vec<MaterializedTrackedStateRow>) -> Result<Self, LixError> {
        let mut builder = MaterializedTrackedStateBatchBuilder::with_capacity(rows.len());
        for row in rows {
            let created_at = LixTimestamp::parse(&row.created_at).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("invalid test tracked-state created_at: {error}"),
                )
            })?;
            let updated_at = LixTimestamp::parse(&row.updated_at).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("invalid test tracked-state updated_at: {error}"),
                )
            })?;
            builder.push(
                TrackedStateKey {
                    entity_pk: row.entity_pk,
                    schema_key: row.schema_key,
                    file_id: row.file_id,
                },
                TrackedStateIndexValue {
                    change_id: row.change_id,
                    commit_id: row.commit_id,
                    deleted: row.deleted,
                    created_at,
                    updated_at,
                },
                row.snapshot_content,
                row.metadata,
            );
        }
        Ok(builder.finish())
    }

    #[cfg(test)]
    pub(crate) fn dictionary_entry_count(&self) -> usize {
        self.strings.len()
    }

    #[cfg(test)]
    pub(crate) fn large_buffer_count(&self, threshold: usize) -> usize {
        [
            self.rows.capacity() * size_of::<MaterializedTrackedStateDescriptor>(),
            self.strings.byte_len(),
            self.strings.len() * size_of::<Range<u32>>(),
        ]
        .into_iter()
        .filter(|bytes| *bytes >= threshold)
        .count()
    }

    #[cfg(test)]
    pub(crate) fn dictionary_arena_allocation_count(&self) -> usize {
        self.strings.arena_allocation_count()
    }

    #[cfg(test)]
    pub(crate) fn dictionary_arena_large_allocation_count(&self) -> usize {
        self.strings.arena_large_allocation_count()
    }
}

/// Borrowed historical row view over one shared materialization batch.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MaterializedTrackedStateRowRef<'a> {
    batch: &'a MaterializedTrackedStateBatch,
    index: usize,
}

impl<'a> MaterializedTrackedStateRowRef<'a> {
    fn descriptor(self) -> &'a MaterializedTrackedStateDescriptor {
        &self.batch.rows[self.index]
    }

    pub(crate) fn entity_pk(self) -> &'a EntityPk {
        &self.descriptor().entity_pk
    }

    pub(crate) fn schema_key(self) -> &'a str {
        self.batch.strings.get(self.descriptor().schema_key)
    }

    pub(crate) fn schema_key_shared(self) -> SharedStr {
        self.batch.strings.shared(self.descriptor().schema_key)
    }

    pub(crate) fn file_id(self) -> Option<&'a str> {
        self.descriptor()
            .file_id
            .map(|ordinal| self.batch.strings.get(ordinal))
    }

    pub(crate) fn file_id_shared(self) -> Option<SharedStr> {
        self.descriptor()
            .file_id
            .map(|ordinal| self.batch.strings.shared(ordinal))
    }

    pub(crate) fn snapshot_content(self) -> Option<&'a SharedStr> {
        self.descriptor().snapshot_content.as_ref()
    }

    pub(crate) fn metadata(self) -> Option<&'a SharedStr> {
        self.descriptor().metadata.as_ref()
    }

    pub(crate) fn deleted(self) -> bool {
        self.descriptor().deleted
    }

    pub(crate) fn created_at(self) -> LixTimestamp {
        self.descriptor().created_at
    }

    pub(crate) fn updated_at(self) -> LixTimestamp {
        self.descriptor().updated_at
    }

    pub(crate) fn change_id(self) -> ChangeId {
        self.descriptor().change_id
    }

    pub(crate) fn commit_id(self) -> CommitId {
        self.descriptor().commit_id
    }

    /// Converts into the legacy DTO only at a terminal compatibility boundary.
    pub(crate) fn to_owned(self) -> MaterializedTrackedStateRow {
        MaterializedTrackedStateRow {
            entity_pk: self.entity_pk().clone(),
            schema_key: self.schema_key().to_owned(),
            file_id: self.file_id().map(str::to_owned),
            snapshot_content: self.snapshot_content().cloned(),
            metadata: self.metadata().cloned(),
            deleted: self.deleted(),
            created_at: self.created_at().to_string(),
            updated_at: self.updated_at().to_string(),
            change_id: self.change_id(),
            commit_id: self.commit_id(),
        }
    }
}

pub(crate) struct MaterializedTrackedStateBatchIter<'a> {
    batch: &'a MaterializedTrackedStateBatch,
    next: usize,
}

impl<'a> Iterator for MaterializedTrackedStateBatchIter<'a> {
    type Item = MaterializedTrackedStateRowRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let index = self.next;
        if index == self.batch.len() {
            return None;
        }
        self.next += 1;
        Some(self.batch.row(index))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.batch.len() - self.next;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for MaterializedTrackedStateBatchIter<'_> {}

/// Positional exact-read owner. Missing input identities remain `None`, while
/// duplicate present identities can share one materialized batch ordinal.
#[derive(Debug, Default)]
pub(crate) struct MaterializedTrackedStateExactBatch {
    batch: MaterializedTrackedStateBatch,
    slots: Vec<Option<u32>>,
}

impl MaterializedTrackedStateExactBatch {
    pub(crate) fn new(
        batch: MaterializedTrackedStateBatch,
        slots: Vec<Option<u32>>,
    ) -> Result<Self, LixError> {
        if u32::try_from(batch.len()).is_err()
            || slots
                .iter()
                .flatten()
                .any(|ordinal| *ordinal as usize >= batch.len())
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "exact tracked-state result contains an invalid batch ordinal",
            ));
        }
        Ok(Self { batch, slots })
    }

    pub(crate) fn len(&self) -> usize {
        self.slots.len()
    }

    pub(crate) fn row(&self, slot: usize) -> Option<MaterializedTrackedStateRowRef<'_>> {
        self.slots
            .get(slot)
            .copied()
            .flatten()
            .map(|ordinal| self.batch.row(ordinal as usize))
    }

    pub(crate) fn into_rows(self) -> Vec<Option<MaterializedTrackedStateRow>> {
        let Self { batch, slots } = self;
        slots
            .into_iter()
            .map(|ordinal| ordinal.map(|ordinal| batch.row(ordinal as usize).to_owned()))
            .collect()
    }
}

struct MaterializedTrackedStateBatchBuilder {
    strings: StringDictionaryBuilder,
    rows: Vec<MaterializedTrackedStateDescriptor>,
}

impl MaterializedTrackedStateBatchBuilder {
    #[cfg(test)]
    fn with_capacity(row_count: usize) -> Self {
        Self::with_capacities(row_count, row_count.saturating_mul(2), 0)
    }

    fn with_capacities(
        row_count: usize,
        dictionary_entry_capacity: usize,
        dictionary_byte_capacity: usize,
    ) -> Self {
        Self {
            // The canonical plane sizes its dictionary directly, so it has no
            // separate rows × identity-columns projection to offer.
            strings: StringDictionaryBuilder::with_capacity(
                0,
                dictionary_entry_capacity,
                dictionary_byte_capacity,
                dictionary_byte_capacity != 0,
            ),
            rows: Vec::with_capacity(row_count),
        }
    }

    fn intern_owned(&mut self, value: String) -> u32 {
        self.strings.intern(value.as_str())
    }

    fn intern_str(&mut self, value: &str) -> u32 {
        self.strings.intern(value)
    }

    fn push(
        &mut self,
        key: TrackedStateKey,
        value: TrackedStateIndexValue,
        snapshot_content: Option<SharedStr>,
        metadata: Option<SharedStr>,
    ) {
        let schema_key = self.intern_owned(key.schema_key);
        let file_id = key.file_id.map(|file_id| self.intern_owned(file_id));
        self.rows.push(MaterializedTrackedStateDescriptor {
            entity_pk: key.entity_pk,
            schema_key,
            file_id,
            snapshot_content,
            metadata,
            deleted: value.deleted(),
            created_at: value.created_at(),
            updated_at: value.updated_at(),
            change_id: value.change_id,
            commit_id: value.commit_id,
        });
    }

    fn push_ref(
        &mut self,
        key: TrackedStateKeyRef<'_>,
        value: TrackedStateIndexValue,
        snapshot_content: Option<SharedStr>,
        metadata: Option<SharedStr>,
    ) {
        let schema_key = self.intern_str(key.schema_key);
        let file_id = key.file_id.map(|file_id| self.intern_str(file_id));
        self.rows.push(MaterializedTrackedStateDescriptor {
            entity_pk: key.entity_pk.clone(),
            schema_key,
            file_id,
            snapshot_content,
            metadata,
            deleted: value.deleted(),
            created_at: value.created_at(),
            updated_at: value.updated_at(),
            change_id: value.change_id,
            commit_id: value.commit_id,
        });
    }

    fn finish(self) -> MaterializedTrackedStateBatch {
        MaterializedTrackedStateBatch {
            strings: self.strings.finish(),
            rows: self.rows,
        }
    }
}

async fn materialize_index_payloads<'a, S>(
    store: &S,
    entries: impl Iterator<Item = (TrackedStateKeyRef<'a>, &'a TrackedStateIndexValue)>,
    projection: ChangeRecordProjection,
) -> Result<HashMap<ChangeId, MaterializedChangePayload>, LixError>
where
    S: StorageAdapterRead,
{
    let mut by_commit = BTreeMap::<CommitId, Vec<(TrackedStateKey, ChangeId, LixTimestamp)>>::new();
    for (key, value) in entries.filter(|(_, value)| !value.deleted) {
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_materialize_owned_key(
            key.schema_key.len() + key.file_id.map_or(0, str::len),
        );
        by_commit.entry(value.commit_id).or_default().push((
            TrackedStateKey {
                schema_key: key.schema_key.to_owned(),
                file_id: key.file_id.map(str::to_owned),
                entity_pk: key.entity_pk.clone(),
            },
            value.change_id,
            value.updated_at,
        ));
    }

    let mut records = Vec::new();
    for (commit_id, expected) in by_commit {
        let keys = expected
            .iter()
            .map(|(key, _, _)| key.clone())
            .collect::<Vec<_>>();
        let loaded =
            super::storage::load_commit_delta_change_records(store, commit_id, &keys).await?;
        for ((key, change_id, updated_at), record) in expected.into_iter().zip(loaded) {
            let record = record.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked-state row references change '{change_id}' that is missing from owning commit '{commit_id}'"
                    ),
                )
            })?;
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_materialize_reverify_row();
            if record.change_id != change_id
                || record.schema_key != key.schema_key
                || record.file_id != key.file_id
                || record.entity_pk != key.entity_pk
                || record.snapshot.is_none()
                || record.created_at != updated_at
            {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "tracked-state row '{change_id}' does not match its authoritative payload in commit '{commit_id}'"
                    ),
                ));
            }
            records.push(record);
        }
    }
    materialize_known_change_payloads(store, records.into_iter(), projection).await
}

/// Materializes tracked-state index entries into one typed batch.
///
/// Every tracked index value carries its payload-owning commit. Hydration
/// routes exact identities to those packed deltas and retains the decoded
/// records through JSON materialization; there is no global changelog
/// fallback.
pub(crate) async fn materialize_batch_from_index_entries<S>(
    store: &S,
    entries: Vec<(TrackedStateKey, TrackedStateIndexValue)>,
    materialization: &ChangeRecordProjection,
) -> Result<MaterializedTrackedStateBatch, LixError>
where
    S: StorageAdapterRead,
{
    let dictionary_entry_capacity = entries
        .iter()
        .map(|(key, _)| 1 + usize::from(key.file_id.is_some()))
        .sum();
    let mut rows = MaterializedTrackedStateBatchBuilder::with_capacities(
        entries.len(),
        dictionary_entry_capacity,
        0,
    );
    if !materialization.snapshot_content && !materialization.metadata {
        for (key, value) in entries {
            rows.push(key, value, None, None);
        }
        return Ok(rows.finish());
    }

    let payloads = materialize_index_payloads(
        store,
        entries.iter().map(|(key, value)| {
            (
                TrackedStateKeyRef {
                    schema_key: &key.schema_key,
                    file_id: key.file_id.as_deref(),
                    entity_pk: &key.entity_pk,
                },
                value,
            )
        }),
        *materialization,
    )
    .await?;

    for (key, value) in entries {
        let (snapshot_content, metadata) = if value.deleted {
            (None, None)
        } else {
            shared_payload_fields(
                &payloads,
                TrackedStateKeyRef {
                    schema_key: key.schema_key.as_str(),
                    file_id: key.file_id.as_deref(),
                    entity_pk: &key.entity_pk,
                },
                value.change_id,
            )?
        };
        rows.push(key, value, snapshot_content, metadata);
    }
    Ok(rows.finish())
}

/// Borrowed-key counterpart for exact historical reads.
///
/// The caller retains one compact key-reference column through the async
/// lookup. Identity strings are copied only when first inserted into the
/// materialized batch dictionary, never once per requested row.
pub(crate) async fn materialize_batch_from_index_entry_refs<'a, S>(
    store: &S,
    entries: Vec<(TrackedStateKeyRef<'a>, TrackedStateIndexValue)>,
    materialization: &ChangeRecordProjection,
) -> Result<MaterializedTrackedStateBatch, LixError>
where
    S: StorageAdapterRead,
{
    let dictionary_entry_capacity = entries
        .iter()
        .map(|(key, _)| 1 + usize::from(key.file_id.is_some()))
        .sum();
    let mut rows = MaterializedTrackedStateBatchBuilder::with_capacities(
        entries.len(),
        dictionary_entry_capacity,
        0,
    );
    if !materialization.snapshot_content && !materialization.metadata {
        for (key, value) in entries {
            rows.push_ref(key, value, None, None);
        }
        return Ok(rows.finish());
    }

    let payloads = materialize_index_payloads(
        store,
        entries.iter().map(|(key, value)| (*key, value)),
        *materialization,
    )
    .await?;

    for (key, value) in entries {
        let (snapshot_content, metadata) = if value.deleted {
            (None, None)
        } else {
            shared_payload_fields(&payloads, key, value.change_id)?
        };
        rows.push_ref(key, value, snapshot_content, metadata);
    }
    Ok(rows.finish())
}

/// Returns cheap views of the materialized payload retained by the batch map.
///
/// A change can back multiple tracked-state rows during historical reads.
/// `SharedStr` lets every use retain the same immutable JSON-store buffer
/// without a use-count map or a new owned string allocation per repeated row.
fn shared_payload_fields(
    payloads: &HashMap<ChangeId, MaterializedChangePayload>,
    key: TrackedStateKeyRef<'_>,
    change_id: ChangeId,
) -> Result<(Option<SharedStr>, Option<SharedStr>), LixError> {
    let payload = payloads.get(&change_id).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked-state row references ChangeRecord '{change_id}' that was not materialized"
            ),
        )
    })?;
    if let Some(identity) = payload.identity.as_ref()
        && (identity.schema_key != key.schema_key
            || identity.entity_pk != *key.entity_pk
            || identity.file_id.as_deref() != key.file_id)
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked-state row identity does not match referenced ChangeRecord '{change_id}'"
            ),
        ));
    }
    Ok((payload.snapshot_content.clone(), payload.metadata.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::MaterializedChangeIdentity;
    use crate::entity_pk::{EntityPk, EntityPkComponent};

    fn integer_entity_pk(value: i64) -> EntityPk {
        EntityPk::from_components(smallvec::smallvec![EntityPkComponent::Integer(value)])
            .expect("one integer is a valid entity primary key")
    }

    /// Which `EntityPk` shapes survive the JSON identity round trip that the
    /// **columnar** commit-delta route uses to match a row.
    ///
    /// `materialize_index_payloads` re-checks `schema_key`/`file_id`/`entity_pk`
    /// on every fetched row. On the packed route that check is
    /// `decode(encode(K)) == K`, guaranteed by the byte-equality assert in
    /// `find_commit_delta_entry_index`. The columnar route
    /// (`load_columnar_owned_entries`) has no such assert: it matches on
    /// `as_json_array_text` and rebuilds the identity with
    /// `from_json_array_text`, and `untyped_component_from_json_value` maps
    /// every JSON string back to `EntityPkComponent::String`.
    ///
    /// So `Uuid` and `Bytes` components do **not** survive, and on a route that
    /// admitted them the re-check would reject a correctly-fetched row. This
    /// test pins exactly where that boundary is; the companion test below pins
    /// the gate that keeps those shapes off the columnar route entirely.
    #[test]
    fn only_string_and_integer_entity_pk_components_survive_the_json_identity_round_trip() {
        fn round_trips(entity_pk: &EntityPk) -> bool {
            let text = entity_pk
                .as_json_array_text()
                .expect("identity should encode as JSON");
            let decoded =
                EntityPk::from_json_array_text(&text).expect("identity should decode from JSON");
            decoded == *entity_pk
        }

        let string_pk =
            EntityPk::from_components(smallvec::smallvec![EntityPkComponent::String("row-0".into())])
                .expect("one string is a valid entity primary key");
        assert!(
            round_trips(&string_pk),
            "a single-string identity must survive; it is the only shape the columnar \
             staging gate admits"
        );

        assert!(
            round_trips(&integer_entity_pk(42)),
            "an integer identity survives as a JSON number"
        );

        let composite = EntityPk::from_components(smallvec::smallvec![
            EntityPkComponent::String("left".into()),
            EntityPkComponent::String("right".into())
        ])
        .expect("two strings are a valid entity primary key");
        assert!(round_trips(&composite), "composite strings survive");

        let uuid_pk = EntityPk::from_components(smallvec::smallvec![EntityPkComponent::Uuid(
            *uuid::Uuid::from_u128(7).as_bytes()
        )])
        .expect("one uuid is a valid entity primary key");
        assert!(
            !round_trips(&uuid_pk),
            "a uuid identity must NOT survive the JSON round trip -- it returns as a \
             String component. If this ever starts passing, the columnar identity \
             re-check stops being load-bearing and this test should be re-read, not deleted."
        );

        let bytes_pk = EntityPk::from_components(smallvec::smallvec![EntityPkComponent::Bytes(
            bytes::Bytes::from_static(&[1, 2, 3])
        )])
        .expect("one byte string is a valid entity primary key");
        assert!(
            !round_trips(&bytes_pk),
            "a bytes identity must NOT survive the JSON round trip -- it returns as a \
             base64 String component"
        );
    }

    /// The gate that makes the above safe.
    ///
    /// `try_stage_lossless_columnar_mutations` refuses to stage a commit
    /// columnar unless **every** row's identity passes
    /// `EntityPk::as_single_string()`, which accepts exactly one
    /// `EntityPkComponent::String`. That is the same predicate, asserted
    /// directly: the shapes the JSON round trip loses are precisely the shapes
    /// that can never reach the columnar route.
    #[test]
    fn the_columnar_staging_gate_rejects_every_identity_shape_the_json_round_trip_loses() {
        let string_pk =
            EntityPk::from_components(smallvec::smallvec![EntityPkComponent::String("row-0".into())])
                .expect("one string is a valid entity primary key");
        assert_eq!(
            string_pk
                .as_single_string()
                .expect("a single string identity is columnar-eligible"),
            "row-0"
        );

        let uuid_pk = EntityPk::from_components(smallvec::smallvec![EntityPkComponent::Uuid(
            *uuid::Uuid::from_u128(7).as_bytes()
        )])
        .expect("one uuid is a valid entity primary key");
        assert!(
            uuid_pk.as_single_string().is_err(),
            "a uuid identity must be refused by the columnar staging gate"
        );

        let bytes_pk = EntityPk::from_components(smallvec::smallvec![EntityPkComponent::Bytes(
            bytes::Bytes::from_static(&[1, 2, 3])
        )])
        .expect("one byte string is a valid entity primary key");
        assert!(
            bytes_pk.as_single_string().is_err(),
            "a bytes identity must be refused by the columnar staging gate"
        );

        assert!(
            integer_entity_pk(42).as_single_string().is_err(),
            "an integer identity is refused too, so the columnar route never sees one"
        );

        let composite = EntityPk::from_components(smallvec::smallvec![
            EntityPkComponent::String("left".into()),
            EntityPkComponent::String("right".into())
        ])
        .expect("two strings are a valid entity primary key");
        assert!(
            composite.as_single_string().is_err(),
            "a composite identity is refused"
        );
    }

    fn index_value(index: usize) -> TrackedStateIndexValue {
        let timestamp = LixTimestamp::from_unix_millis_utc_lossy(1_700_000_000_000);
        TrackedStateIndexValue {
            change_id: ChangeId::new(uuid::Uuid::from_u128(
                u128::try_from(index).expect("test index fits u128") + 1,
            )),
            commit_id: CommitId::new(uuid::Uuid::from_u128(1)),
            deleted: false,
            created_at: timestamp,
            updated_at: timestamp,
        }
    }

    /// The constructed case for the columnar identity round trip.
    ///
    /// Arm A uses a single-string primary key, which the staging gate admits,
    /// and asserts the columnar route actually served the rows. Without that
    /// assertion arm B proves nothing: "no columnar rows" would be
    /// indistinguishable from "the test never built a columnar commit".
    ///
    /// Arm B declares the same schema with `"format": "uuid"` on the primary
    /// key, which makes the identity an `EntityPkComponent::Uuid` — the shape
    /// that does not survive the JSON round trip. It must read back correctly
    /// **and** never take the columnar route. If a future change widens the
    /// staging gate, arm B's rows would be fetched through a JSON identity
    /// match, the re-check in `materialize_index_payloads` would reject them,
    /// and this test fails instead of the read failing in production.
    #[cfg(feature = "storage-benches")]
    #[tokio::test]
    async fn a_uuid_primary_key_never_reaches_the_columnar_commit_delta_route() {
        use crate::engine::Engine;
        use crate::storage_adapter::Memory;
        use serde_json::json;

        const ROWS: usize = 250;

        async fn run(schema_key: &str, uuid_pk: bool) -> (u64, u64, usize) {
            let storage = Memory::new();
            Engine::initialize(storage.clone())
                .await
                .expect("engine should initialize");
            let engine = Engine::new(storage.clone())
                .await
                .expect("engine should open");
            let session = engine.open_session().await.expect("session should open");

            let mut id_property = json!({ "type": "string" });
            if uuid_pk {
                id_property["format"] = json!("uuid");
            }
            let schema = json!({
                "x-lix-key": schema_key,
                "x-lix-primary-key": ["/id"],
                "type": "object",
                "properties": { "id": id_property, "locale": { "type": "string" } },
                "required": ["id", "locale"],
                "additionalProperties": false
            });
            session
                .execute(
                    "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
                    &[crate::Value::Text(schema.to_string())],
                )
                .await
                .expect("schema should register");

            // One dense multi-row INSERT: the shape the lossless columnar
            // staging path is written for.
            let values = (0..ROWS)
                .map(|index| {
                    let id = if uuid_pk {
                        uuid::Uuid::from_u128(index as u128 + 1).to_string()
                    } else {
                        format!("row-{index}")
                    };
                    let locale = if index == 0 { "keep" } else { "drop" };
                    format!("('{id}', '{locale}')")
                })
                .collect::<Vec<_>>()
                .join(",");
            session
                .execute(
                    &format!("INSERT INTO {schema_key} (id, locale) VALUES {values}"),
                    &[],
                )
                .await
                .expect("rows should insert");

            // Rotate onto a branch so the read is served from a root current
            // base and must fetch payloads from the owning commit delta.
            let branch = session
                .create_branch(crate::CreateBranchOptions {
                    id: None,
                    name: format!("e52-columnar-{schema_key}"),
                    from_commit_id: None,
                })
                .await
                .expect("branch should create");
            session
                .switch_branch(crate::SwitchBranchOptions {
                    branch_id: branch.id.clone(),
                })
                .await
                .expect("branch should switch");

            let _ = crate::storage_bench::take_tracked_key_allocation_census();
            let rows = session
                .execute(&format!("SELECT id FROM {schema_key} WHERE locale = 'keep'"), &[])
                .await
                .expect("the rotated scan must succeed, not reject its own payload");
            let census = crate::storage_bench::take_tracked_key_allocation_census();
            (
                census.commit_delta_columnar_rows,
                census.commit_delta_rows_loaded,
                rows.len(),
            )
        }

        let (string_columnar, string_packed, string_rows) = run("colstr", false).await;
        assert_eq!(string_rows, 1, "the string-pk arm must answer one row");
        assert!(
            string_columnar > 0,
            "arm A must actually reach the columnar route, otherwise arm B's zero \
             proves nothing (columnar={string_columnar} packed={string_packed})"
        );

        let (uuid_columnar, uuid_packed, uuid_rows) = run("coluuid", true).await;
        assert_eq!(
            uuid_rows, 1,
            "the uuid-pk arm must answer one row -- a rejected payload would surface here"
        );
        assert_eq!(
            uuid_columnar, 0,
            "a uuid primary key must never be served by the columnar route, whose \
             identity match cannot round-trip it (columnar={uuid_columnar} packed={uuid_packed})"
        );
        assert!(
            uuid_packed > 0,
            "the uuid arm must still have fetched payloads, through the packed route"
        );
    }

    #[test]
    fn ten_thousand_rows_share_identity_dictionary_and_constant_batch_buffers() {
        const ROW_COUNT: usize = 10_000;
        let snapshot = SharedStr::from_static(r#"{"value":"shared"}"#);
        let metadata = SharedStr::from_static(r#"{"impact":"format"}"#);
        let mut builder = MaterializedTrackedStateBatchBuilder::with_capacity(ROW_COUNT);
        for index in 0..ROW_COUNT {
            let entity_pk =
                integer_entity_pk(i64::try_from(index).expect("test row index fits i64"));
            builder.push_ref(
                TrackedStateKeyRef {
                    schema_key: "shared_schema",
                    file_id: Some("shared_file"),
                    entity_pk: &entity_pk,
                },
                index_value(index),
                Some(snapshot.clone()),
                Some(metadata.clone()),
            );
        }
        let batch = builder.finish();

        assert_eq!(batch.len(), ROW_COUNT);
        assert_eq!(
            batch.dictionary_entry_count(),
            2,
            "schema and file metadata must each be retained once"
        );
        assert_eq!(
            batch.large_buffer_count(4 * 1024),
            1,
            "row count must not change the constant descriptor/dictionary column count"
        );
        let first = batch.row(0);
        let last = batch.row(ROW_COUNT - 1);
        assert_eq!(first.schema_key().as_ptr(), last.schema_key().as_ptr());
        assert_eq!(
            first.file_id().expect("first file").as_ptr(),
            last.file_id().expect("last file").as_ptr()
        );
        let first_schema = first.schema_key_shared();
        let last_schema = last.schema_key_shared();
        let first_file = first.file_id_shared().expect("first shared file");
        assert!(first_schema.shares_buffer_with(&last_schema));
        assert!(
            first_schema.shares_buffer_with(&first_file),
            "all identity strings must retain the same dictionary arena"
        );
        assert!(
            first
                .snapshot_content()
                .expect("first snapshot")
                .shares_buffer_with(last.snapshot_content().expect("last snapshot"))
        );
        assert_eq!(
            first.created_at(),
            LixTimestamp::from_unix_millis_utc_lossy(1_700_000_000_000)
        );
    }

    #[test]
    fn ten_thousand_unique_file_ids_append_to_one_historical_identity_arena() {
        const ROW_COUNT: usize = 10_000;
        let mut builder =
            MaterializedTrackedStateBatchBuilder::with_capacities(ROW_COUNT, ROW_COUNT * 2, 0);
        for index in 0..ROW_COUNT {
            let entity_pk =
                integer_entity_pk(i64::try_from(index).expect("test row index fits i64"));
            let file_id = format!("file-{index:05}");
            builder.push_ref(
                TrackedStateKeyRef {
                    schema_key: "shared_schema",
                    file_id: Some(file_id.as_str()),
                    entity_pk: &entity_pk,
                },
                index_value(index),
                None,
                None,
            );
        }
        let batch = builder.finish();

        assert_eq!(batch.len(), ROW_COUNT);
        assert_eq!(batch.dictionary_entry_count(), ROW_COUNT + 1);
        assert_eq!(
            batch.dictionary_arena_allocation_count(),
            2,
            "the small arena and one promoted arena must cover every unique identity"
        );
        assert_eq!(
            batch.dictionary_arena_large_allocation_count(),
            1,
            "historical identity lowering must allocate one large UTF-8 arena per batch"
        );
        assert_eq!(batch.row(0).file_id(), Some("file-00000"));
        assert_eq!(batch.row(ROW_COUNT - 1).file_id(), Some("file-09999"));
        assert!(
            batch
                .row(0)
                .file_id_shared()
                .expect("first file")
                .shares_buffer_with(
                    &batch
                        .row(ROW_COUNT - 1)
                        .file_id_shared()
                        .expect("last file")
                ),
            "all unique identities must remain slices of the same immutable arena"
        );
    }

    #[test]
    fn projected_exact_batch_preserves_duplicate_and_missing_input_slots() {
        let mut builder = MaterializedTrackedStateBatchBuilder::with_capacity(1);
        builder.push(
            TrackedStateKey {
                schema_key: "message".to_owned(),
                file_id: Some("file.md".to_owned()),
                entity_pk: integer_entity_pk(7),
            },
            index_value(0),
            Some(SharedStr::from_static(r#"{"id":7}"#)),
            None,
        );
        let exact =
            MaterializedTrackedStateExactBatch::new(builder.finish(), vec![Some(0), None, Some(0)])
                .expect("aligned exact batch");

        assert_eq!(exact.len(), 3);
        let first = exact.row(0).expect("first duplicate");
        assert!(exact.row(1).is_none());
        let duplicate = exact.row(2).expect("second duplicate");
        assert_eq!(first.entity_pk(), duplicate.entity_pk());
        assert_eq!(first.schema_key().as_ptr(), duplicate.schema_key().as_ptr());
        assert!(
            first
                .snapshot_content()
                .expect("first payload")
                .shares_buffer_with(duplicate.snapshot_content().expect("duplicate payload"))
        );
    }

    fn fixture(
        schema_key: &str,
    ) -> (
        ChangeId,
        TrackedStateKey,
        HashMap<ChangeId, MaterializedChangePayload>,
        SharedStr,
    ) {
        let change_id = ChangeId::new(uuid::Uuid::from_bytes([7; 16]));
        let key = TrackedStateKey {
            schema_key: schema_key.to_owned(),
            file_id: Some("file.md".to_owned()),
            entity_pk: EntityPk::single("entity"),
        };
        let snapshot = SharedStr::from(r#"{"id":"entity"}"#.to_owned());
        let payload = MaterializedChangePayload {
            identity: Some(MaterializedChangeIdentity {
                schema_key: "message".to_owned(),
                entity_pk: EntityPk::single("entity"),
                file_id: Some("file.md".to_owned()),
            }),
            snapshot_content: Some(snapshot.clone()),
            metadata: None,
        };
        (
            change_id,
            key,
            HashMap::from([(change_id, payload)]),
            snapshot,
        )
    }

    #[test]
    fn repeated_payload_uses_share_the_materialized_json_buffer() {
        let (change_id, key, payloads, source) = fixture("message");

        let key_ref = TrackedStateKeyRef {
            schema_key: key.schema_key.as_str(),
            file_id: key.file_id.as_deref(),
            entity_pk: &key.entity_pk,
        };
        let first = shared_payload_fields(&payloads, key_ref, change_id)
            .expect("first payload use")
            .0
            .expect("snapshot");
        let second = shared_payload_fields(&payloads, key_ref, change_id)
            .expect("second payload use")
            .0
            .expect("snapshot");

        assert!(source.shares_buffer_with(&first));
        assert!(first.shares_buffer_with(&second));
    }

    #[test]
    fn payload_identity_mismatch_is_rejected() {
        let (change_id, key, payloads, _) = fixture("wrong-schema");

        let error = shared_payload_fields(
            &payloads,
            TrackedStateKeyRef {
                schema_key: key.schema_key.as_str(),
                file_id: key.file_id.as_deref(),
                entity_pk: &key.entity_pk,
            },
            change_id,
        )
        .expect_err("mismatched identity must fail");
        assert!(
            error
                .to_string()
                .contains("identity does not match referenced ChangeRecord")
        );
    }
}
