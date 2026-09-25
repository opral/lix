//! Durable create-context authority for production component plugins.
//!
//! Components see a compact UUIDv7 create context. The engine retains the full
//! operation proof, binds the context to the file authority, and writes one
//! fixed-shape tracked reservation row when that context first creates an
//! row. A colliding context with a different proof is rejected before any
//! semantic rows are staged.

use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::LixError;
use crate::binary_cas::BlobId;
use crate::catalog::CatalogSnapshot;
#[cfg(test)]
use crate::common::LixTimestamp;
use crate::common::MutationIdentity;
#[cfg(test)]
use crate::hot_state::MaterializedHotStateBatchBuilder;
use crate::hot_state::{MaterializedHotStateExactBatch, MaterializedHotStateRow};
use crate::plugin::runtime::{
    WasmChangeEffect, WasmCreateContext, WasmHostBytes, WasmHostRowChanges, WasmRow, WasmRowChange,
    WasmRowChanges, WasmRowKey,
};
use crate::row_pk::RowPk;
use crate::transaction_types::{TransactionJson, TransactionWriteRow};

use super::{PluginActorKey, PluginRegistryEntry};

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
const RESERVATION_PREFIX: &str = "lix_plugin_create_v1:";
const RESERVATION_VERSION: u32 = 1;

/// A mutation identity after it has been bound to one durable plugin-file
/// authority. Different operation proofs may deliberately yield the same
/// context when their 128-bit seeds collide; the reservation row detects
/// that condition using `bound_operation_proof`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BoundCreateContext {
    prefix: [u8; 12],
    bound_operation_proof: [u8; 32],
    authority_binding: [u8; 32],
}

impl BoundCreateContext {
    pub(crate) fn bind(
        identity: MutationIdentity,
        actor_key: &PluginActorKey,
    ) -> Result<Self, LixError> {
        let namespace_uuid = uuid::Uuid::from_bytes(identity.namespace_seed);
        if namespace_uuid.get_version_num() != 7
            || namespace_uuid.get_variant() != uuid::Variant::RFC4122
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "mutation identity namespace_seed must be RFC 9562 UUIDv7 bytes",
            ));
        }
        let authority_binding = authority_binding(actor_key);
        let namespace_digest = framed_digest(
            b"lix.plugin-component.bound-namespace.component\0",
            &[&identity.namespace_seed, &authority_binding],
        );
        let bound_operation_proof = framed_digest(
            b"lix.plugin-component.bound-operation-proof.component\0",
            &[&identity.operation_proof, &authority_binding],
        );
        let mut prefix = [0_u8; 12];
        prefix[..6].copy_from_slice(&identity.namespace_seed[..6]);
        prefix[6..].copy_from_slice(&namespace_digest[..6]);
        prefix[6] = (prefix[6] & 0x0f) | 0x70;
        prefix[8] = (prefix[8] & 0x3f) | 0x80;
        Ok(Self {
            prefix,
            bound_operation_proof,
            authority_binding,
        })
    }

    pub(crate) fn creates(self) -> WasmCreateContext {
        WasmCreateContext {
            high: u64::from_be_bytes(
                self.prefix[..8]
                    .try_into()
                    .expect("UUID prefix has high bytes"),
            ),
            low: u32::from_be_bytes(
                self.prefix[8..]
                    .try_into()
                    .expect("UUID prefix has low bytes"),
            ),
        }
    }

    pub(crate) fn reservation_key(self) -> String {
        format!("{RESERVATION_PREFIX}{}", encode_hex(&self.prefix))
    }
}

/// Creates a complete proof for local calls that do not arrive through the
/// remote mutation protocol. The caller must supply a freshly minted UUIDv7.
pub(crate) fn local_mutation_identity(namespace_seed: [u8; 16]) -> MutationIdentity {
    MutationIdentity {
        namespace_seed,
        operation_proof: framed_digest(
            b"lix.plugin-component.local-operation-proof.component\0",
            &[&namespace_seed],
        ),
    }
}

fn authority_binding(actor_key: &PluginActorKey) -> [u8; 32] {
    framed_digest(
        b"lix.plugin-component.id-authority.component\0",
        &[
            actor_key.branch_id.as_bytes(),
            actor_key.file_id.as_bytes(),
            actor_key.owner_change_id.as_bytes(),
            actor_key.plugin_key.as_bytes(),
            actor_key.plugin_generation.as_bytes(),
        ],
    )
}

fn framed_digest(domain: &[u8], fields: &[&[u8]]) -> [u8; 32] {
    let capacity = domain.len()
        + fields
            .iter()
            .map(|field| 8usize.saturating_add(field.len()))
            .sum::<usize>();
    let mut input = Vec::with_capacity(capacity);
    input.extend_from_slice(domain);
    for field in fields {
        input.extend_from_slice(&u64::try_from(field.len()).unwrap_or(u64::MAX).to_be_bytes());
        input.extend_from_slice(field);
    }
    BlobId::from_content(&input).into_bytes()
}

/// Result of validating generated identities in one sparse guest transition.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct CreateValidation {
    /// A keyless create was emitted. The transaction must check or
    /// create the corresponding durable reservation before staging changes.
    pub(crate) requires_reservation: bool,
    /// Non-current compact identities are valid only when the exact durable
    /// row already exists.
    pub(crate) existing_authorities: Vec<WasmRowKey>,
}

pub(crate) fn validate_create_changes<B>(
    plugin: &PluginRegistryEntry,
    changes: &WasmRowChanges<B>,
) -> Result<CreateValidation, LixError> {
    let creatable = plugin.create_schema_keys();
    let mut validation = CreateValidation::default();
    for change in &changes.changes {
        match change {
            WasmRowChange::Create {
                schema_key,
                local_ref,
                ..
            } => {
                if creatable
                    .binary_search_by(|candidate| candidate.as_str().cmp(schema_key.as_str()))
                    .is_err()
                {
                    return Err(invalid_id(format!(
                        "plugin '{}' emitted a keyless create for schema '{}' without a UUIDv7 primary-key default",
                        plugin.key(),
                        schema_key
                    )));
                }
                u32::try_from(*local_ref).map_err(|_| {
                    invalid_id(format!(
                        "plugin '{}' create local reference {} exceeds the u32 allocation range",
                        plugin.key(),
                        local_ref
                    ))
                })?;
                validation.requires_reservation = true;
            }
            WasmRowChange::Upsert { row, .. }
                if creatable
                    .binary_search_by(|candidate| {
                        candidate.as_str().cmp(row.key.schema_key.as_str())
                    })
                    .is_ok() =>
            {
                validation.existing_authorities.push(row.key.clone());
            }
            WasmRowChange::Upsert { .. } | WasmRowChange::Delete(_) => {}
        }
    }
    validation.existing_authorities.sort();
    validation.existing_authorities.dedup();
    Ok(validation)
}

pub(crate) fn materialize_keyless_creates(
    changes: &mut WasmHostRowChanges,
    creates: WasmCreateContext,
    schemas: &CatalogSnapshot,
) -> Result<(), LixError> {
    for change in &mut changes.changes {
        let WasmRowChange::Create {
            schema_key,
            local_ref,
            payload,
            ..
        } = change
        else {
            continue;
        };
        let id = creates.component(*local_ref)?;
        let WasmHostBytes::Typed(typed) = payload;
        if !typed.boundary_create_validation_certified() {
            return Err(invalid_id(
                "typed create reached identity materialization without boundary validation",
            ));
        }
        let (_, schema_plan) = schemas.plan_for_key(schema_key.as_str()).ok_or_else(|| {
            invalid_id(format!(
                "typed create references unknown schema '{}'",
                schema_key.as_str()
            ))
        })?;
        if !schema_plan
            .fingerprint()
            .matches_bytes(&typed.schema_fingerprint)
        {
            return Err(invalid_id(format!(
                "typed create fingerprint does not match schema '{}'",
                schema_key.as_str()
            )));
        }
        let [primary_key] = schema_plan.compiled_schema.primary_key() else {
            return Err(invalid_id(format!(
                "typed create schema '{}' must have one generated primary key",
                schema_key.as_str()
            )));
        };
        let fingerprint = typed.schema_fingerprint;
        let typed_mut = std::sync::Arc::make_mut(typed);
        typed_mut.invalidate_durable_payload();
        let generated = lix_schema::Value::Uuid(id);
        match typed_mut.row.get(primary_key) {
            Some(lix_schema::Value::Uuid(existing)) if *existing == id => {}
            Some(_) => {
                return Err(invalid_id(
                    "typed create identity does not match its create-context reference",
                ));
            }
            None => {
                typed_mut.row.insert(primary_key.clone(), generated.clone());
            }
        }
        typed_mut.row_pk = vec![generated.clone()].into();
        // Boundary validation already proved every supplied column and that
        // the generated UUID column is the create's only omitted requirement.
        // Inserting that exact UUID completes the row; validating every other
        // column a second time here only repeats the page-level proof.
        typed_mut.certify_boundary_validation().map_err(|error| {
            invalid_id(format!(
                "materialized typed create is not durably encodable: {error:?}"
            ))
        })?;
        let key = WasmRowKey::from_typed_parts(schema_key.clone(), fingerprint, vec![generated])?;
        let payload = WasmHostBytes::Typed(std::sync::Arc::clone(typed));
        *change = WasmRowChange::Upsert {
            row: WasmRow { key, payload },
            effect: WasmChangeEffect::Content,
        };
    }
    Ok(())
}

pub(crate) fn require_existing_id_authorities(
    plugin: &PluginRegistryEntry,
    keys: &[WasmRowKey],
    rows: &MaterializedHotStateExactBatch,
    file_id: &str,
    branch_id: &str,
    untracked: bool,
) -> Result<(), LixError> {
    if keys.len() != rows.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "create authority lookup returned the wrong cardinality",
        ));
    }
    for (slot, key) in keys.iter().enumerate() {
        let observed = rows.row(slot);
        let valid = observed.is_some_and(|row| {
            !row.deleted()
                && row.decoded_snapshot().is_some()
                && key.schema_key == row.schema_key()
                && key.row_pk.len() == 1
                && typed_key_uuid(key)
                    .is_ok_and(|row_pk| row.row_pk() == &row_pk)
                && row.file_id() == Some(file_id)
                && row.branch_id() == branch_id
                && !row.global()
                // The authority row lives in its own file's lane.
                && row.untracked() == untracked
        });
        if !valid {
            return Err(LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "plugin '{}' emitted a keyed update for creatable schema '{}' row {:?}, but no matching typed row exists in the file lane (observed={:?})",
                    plugin.key(),
                    key.schema_key,
                    key.row_pk,
                    observed.map(|row| (
                        row.schema_key(),
                        row.row_pk(),
                        row.file_id(),
                        row.branch_id(),
                        row.untracked(),
                        row.deleted(),
                        row.decoded_snapshot().is_some(),
                    )),
                ),
            )
            .with_hint(
                "Use keyless Create for new rows and keyed Upsert only for existing rows.",
            ));
        }
    }
    Ok(())
}

fn typed_key_uuid(key: &WasmRowKey) -> Result<RowPk, LixError> {
    let [lix_schema::Value::Uuid(_)] = key.row_pk.as_ref() else {
        return Err(LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            "creatable row primary key is not a UUID",
        ));
    };
    RowPk::from_schema_values(&key.row_pk).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PLUGIN,
            format!("invalid typed UUID primary key: {error}"),
        )
    })
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReservationValue {
    version: u32,
    operation_proof: String,
    authority_binding: String,
}

/// Returns a row to stage when the reservation is absent, accepts an exact
/// same-proof replay without another write, and rejects a truncated-context
/// collision before semantic rows enter the transaction buffer.
pub(crate) fn reserve_create_row(
    existing: Option<&MaterializedHotStateRow>,
    bound: BoundCreateContext,
    file_id: &str,
    branch_id: &str,
    untracked: bool,
) -> Result<Option<TransactionWriteRow>, LixError> {
    validate_create_reservation(existing, bound, file_id, branch_id, untracked)?;
    if existing.is_some() {
        return Ok(None);
    }

    let key = bound.reservation_key();
    let snapshot = json!({
        "key": key,
        "value": {
            "version": RESERVATION_VERSION,
            "operation_proof": encode_hex(&bound.bound_operation_proof),
            "authority_binding": encode_hex(&bound.authority_binding),
        }
    });
    Ok(Some(reservation_row(
        bound.reservation_key(),
        Some(snapshot),
        file_id,
        branch_id,
        untracked,
    )?))
}

/// Validates an already-reserved create context before entering a guest transition.
///
/// This preflight is deliberately independent of whether the eventual sparse
/// change set creates a row. A client presenting a reserved context with a
/// different full proof has already violated the mutation-identity contract;
/// rejecting it here prevents guest-local allocator errors from obscuring the
/// public constraint violation.
pub(crate) fn validate_create_reservation(
    existing: Option<&MaterializedHotStateRow>,
    bound: BoundCreateContext,
    file_id: &str,
    branch_id: &str,
    untracked: bool,
) -> Result<(), LixError> {
    let Some(row) = existing else {
        return Ok(());
    };
    let key = bound.reservation_key();
    validate_reservation_identity(row, &key, file_id, branch_id, untracked)?;
    let snapshot = row
        .snapshot_content
        .as_deref()
        .ok_or_else(|| invalid_id(format!("create reservation '{key}' has no snapshot")))?;
    let snapshot: JsonValue = serde_json::from_str(snapshot).map_err(|error| {
        invalid_id(format!(
            "create reservation '{key}' is invalid JSON: {error}"
        ))
    })?;
    let object = snapshot
        .as_object()
        .ok_or_else(|| invalid_id(format!("create reservation '{key}' must be an object")))?;
    if object.len() != 2 || object.get("key").and_then(JsonValue::as_str) != Some(&key) {
        return Err(invalid_id(format!(
            "create reservation '{key}' has invalid key-value shape"
        )));
    }
    let value: ReservationValue = serde_json::from_value(
        object
            .get("value")
            .cloned()
            .ok_or_else(|| invalid_id(format!("create reservation '{key}' has no value")))?,
    )
    .map_err(|error| {
        invalid_id(format!(
            "create reservation '{key}' has an invalid value: {error}"
        ))
    })?;
    let operation_proof = decode_hex_32(&value.operation_proof).ok_or_else(|| {
        invalid_id(format!(
            "create reservation '{key}' has an invalid operation proof"
        ))
    })?;
    let authority_binding = decode_hex_32(&value.authority_binding).ok_or_else(|| {
        invalid_id(format!(
            "create reservation '{key}' has an invalid authority binding"
        ))
    })?;
    if value.version != RESERVATION_VERSION || authority_binding != bound.authority_binding {
        return Err(invalid_id(format!(
            "create reservation '{key}' does not match the current file authority"
        )));
    }
    if operation_proof != bound.bound_operation_proof {
        return Err(LixError::new(
            LixError::CODE_CONSTRAINT_VIOLATION,
            "create-context collision: the context is reserved by a different operation proof",
        ));
    }
    Ok(())
}

pub(crate) fn reservation_tombstone_row(
    key: &str,
    file_id: &str,
    branch_id: &str,
    untracked: bool,
) -> Result<TransactionWriteRow, LixError> {
    if !is_reservation_key(key) {
        return Err(invalid_id("invalid create reservation key"));
    }
    reservation_row(key.to_string(), None, file_id, branch_id, untracked)
}

pub(crate) fn is_reservation_key(key: &str) -> bool {
    key.strip_prefix(RESERVATION_PREFIX)
        .is_some_and(|suffix| suffix.len() == 24 && suffix.bytes().all(is_lower_hex))
}

fn reservation_row(
    key: String,
    snapshot: Option<JsonValue>,
    file_id: &str,
    branch_id: &str,
    untracked: bool,
) -> Result<TransactionWriteRow, LixError> {
    if file_id.is_empty() || branch_id.is_empty() || branch_id == crate::GLOBAL_BRANCH_ID {
        return Err(invalid_id(
            "create reservations require a file-scoped tracked branch",
        ));
    }
    Ok(TransactionWriteRow {
        row_pk: Some(RowPk::single(key)),
        schema_key: KEY_VALUE_SCHEMA_KEY.into(),
        file_id: Some(file_id.into()),
        snapshot: snapshot
            .map(|value| TransactionJson::from_value(value, "plugin create reservation"))
            .transpose()?,
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: false,
        change_id: None,
        commit_id: None,
        untracked,
        branch_id: branch_id.into(),
    })
}

fn validate_reservation_identity(
    row: &MaterializedHotStateRow,
    key: &str,
    file_id: &str,
    branch_id: &str,
    untracked: bool,
) -> Result<(), LixError> {
    if row.schema_key != KEY_VALUE_SCHEMA_KEY
        || row.row_pk.as_single_string().ok() != Some(key)
        || row.file_id.as_deref() != Some(file_id)
        || row.branch_id.as_ref() != branch_id
        || row.global
        || row.untracked != untracked
        || row.deleted
    {
        return Err(invalid_id(format!(
            "create reservation '{key}' has invalid file scope"
        )));
    }
    Ok(())
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn decode_hex_32(value: &str) -> Option<[u8; 32]> {
    if value.len() != 64 || !value.bytes().all(is_lower_hex) {
        return None;
    }
    let mut decoded = [0u8; 32];
    for (index, output) in decoded.iter_mut().enumerate() {
        let high = decode_hex(value.as_bytes()[index * 2])?;
        let low = decode_hex(value.as_bytes()[index * 2 + 1])?;
        *output = high << 4 | low;
    }
    Some(decoded)
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

fn decode_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn invalid_id(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PLUGIN, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::runtime::{PluginRegistryEntryInput, PluginRuntime};
    use crate::plugin::runtime::{WasmChangeEffect, WasmHostBytes, WasmRow, WasmTypedRow};

    fn actor_key() -> PluginActorKey {
        PluginActorKey {
            branch_id: "main".to_string(),
            file_id: "01920000-0000-7000-8000-0000000000a2".to_string(),
            path: "/a.csv".to_string(),
            owner_change_id: "owner-a".to_string(),
            plugin_key: "plugin_csv".to_string(),
            plugin_generation: "a".repeat(64),
        }
    }

    fn mutation_identity(seed_suffix: u8, proof: u8) -> MutationIdentity {
        let mut namespace_seed = uuid::Uuid::parse_str("01920000-0000-7000-8000-000000000000")
            .expect("fixture UUID")
            .into_bytes();
        namespace_seed[15] = seed_suffix;
        MutationIdentity {
            namespace_seed,
            operation_proof: [proof; 32],
        }
    }

    fn plugin() -> PluginRegistryEntry {
        PluginRegistryEntry::new(PluginRegistryEntryInput {
            key: "plugin_csv".to_string(),
            runtime: PluginRuntime::WasmComponent,
            api_version: "2.0.0".to_string(),
            capabilities: crate::plugin::runtime::PluginCapabilities {
                column_merger: true,
                file_projection: true,
            },
            path_glob: Some("*.csv".to_string()),
            content: None,
            entry: Some("plugin.wasm".to_string()),
            schema_keys: vec!["csv_row".to_string()],
            create_schema_keys: vec!["csv_row".to_string()],
            manifest_json: r#"{"entry":"plugin.wasm","file_match":{"path_glob":"*.csv"},"key":"plugin_csv","schemas":["schema/csv_row.json"]}"#.to_string(),
            archive_file_id: crate::plugin::runtime::plugin_storage_archive_file_id("plugin_csv"),
            archive_path: "/.lix/plugins/plugin_csv.lixplugin".to_string(),
            archive_blob_hash: "a".repeat(64),
            wasm_blob_hash: Some("b".repeat(64)),
        })
        .expect("plugin")
    }

    fn create_catalog() -> CatalogSnapshot {
        CatalogSnapshot::from_visible_schemas(&[json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "csv_row",
            "columns": [
                { "name": "row_id", "type": "uuid", "nullable": false, "default_expression": "uuidv7()" },
                { "name": "cells", "type": "jsonb", "nullable": false },
                { "name": "order_key", "type": "text", "nullable": false }
            ],
            "primary_key": ["row_id"]
        })])
        .expect("create test catalog")
    }

    fn typed_row(id: &str) -> WasmHostBytes {
        let id = id.to_owned();
        WasmHostBytes::Typed(std::sync::Arc::new(WasmTypedRow {
            schema_fingerprint: [0; 32],
            row_pk: vec![lix_schema::Value::Text(id.clone())].into(),
            row: lix_schema::Row::from([
                (
                    "cells".to_owned(),
                    lix_schema::Value::Jsonb(JsonValue::Array(Vec::new()).into()),
                ),
                ("id".to_owned(), lix_schema::Value::Text(id)),
                (
                    "order_key".to_owned(),
                    lix_schema::Value::Text("a".to_owned()),
                ),
            ]),
            native_payload: std::sync::OnceLock::new(),
            boundary_create_validation: std::sync::OnceLock::new(),
        }))
    }

    fn upsert(id: uuid::Uuid) -> WasmRowChange<WasmHostBytes> {
        WasmRowChange::Upsert {
            row: WasmRow {
                key: WasmRowKey::from_typed_parts(
                    "csv_row",
                    [0; 32],
                    vec![lix_schema::Value::Uuid(id)],
                )
                .unwrap(),
                payload: typed_row("id"),
            },
            effect: WasmChangeEffect::Content,
        }
    }

    fn create(local_ref: u64) -> WasmRowChange<WasmHostBytes> {
        let mut payload = typed_row("created");
        let WasmHostBytes::Typed(row) = &mut payload;
        let row = std::sync::Arc::make_mut(row);
        row.row_pk = std::sync::Arc::from([]);
        row.row.remove("id");
        row.schema_fingerprint = create_catalog()
            .plan_for_key("csv_row")
            .expect("create schema")
            .1
            .fingerprint()
            .bytes();
        row.certify_boundary_create_validation();
        WasmRowChange::Create {
            schema_key: "csv_row".into(),
            local_ref,
            resolved_key: None,
            payload,
        }
    }

    fn row_for(bound: BoundCreateContext) -> MaterializedHotStateRow {
        let write = reserve_create_row(
            None,
            bound,
            "01920000-0000-7000-8000-0000000000a2",
            "main",
            false,
        )
        .expect("reserve")
        .expect("new row");
        MaterializedHotStateRow {
            row_pk: write.row_pk.expect("pk"),
            schema_key: write.schema_key.into(),
            file_id: write.file_id.map(Into::into),
            snapshot_content: write.snapshot.map(|snapshot| snapshot.normalized().into()),
            metadata: None,
            deleted: false,
            created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
            updated_at: LixTimestamp::from_unix_millis_utc_lossy(1),
            global: false,
            change_id: None,
            commit_id: None,
            untracked: false,
            branch_id: "main".into(),
        }
    }

    #[test]
    fn create_context_produces_stable_uuid_v7_values() {
        let creates = BoundCreateContext::bind(mutation_identity(7, 8), &actor_key())
            .expect("valid UUIDv7 seed")
            .creates();
        let value = creates.component(42).expect("uuid");
        assert_eq!(value.get_version_num(), 7);
        assert_eq!(&value.as_bytes()[12..], &42_u32.to_be_bytes());
    }

    #[test]
    fn materializing_create_identity_reuses_the_unique_typed_row_owner() {
        let creates = BoundCreateContext::bind(mutation_identity(7, 8), &actor_key())
            .expect("valid UUIDv7 seed")
            .creates();
        let mut changes = WasmHostRowChanges {
            changes: vec![create(42)],
        };
        let before = match &changes.changes[0] {
            WasmRowChange::Create {
                payload: WasmHostBytes::Typed(row),
                ..
            } => std::sync::Arc::as_ptr(row),
            _ => unreachable!("fixture is a typed create"),
        };

        materialize_keyless_creates(&mut changes, creates, &create_catalog())
            .expect("materialize create");

        let (after, generated) = match &changes.changes[0] {
            WasmRowChange::Upsert { row, .. } => {
                let WasmHostBytes::Typed(payload) = &row.payload;
                (std::sync::Arc::as_ptr(payload), row.key.row_pk[0].clone())
            }
            _ => panic!("create must become an upsert"),
        };
        assert_eq!(
            before, after,
            "unique typed row ownership should move in place"
        );
        assert!(matches!(generated, lix_schema::Value::Uuid(_)));
    }

    #[test]
    fn materializing_create_rejects_a_conflicting_embedded_identity() {
        let bound = BoundCreateContext::bind(mutation_identity(7, 8), &actor_key())
            .expect("valid UUIDv7 seed");
        let expected = bound.creates().component(42).expect("generated id");
        let mut change = create(42);
        let WasmRowChange::Create {
            payload: WasmHostBytes::Typed(row),
            ..
        } = &mut change
        else {
            unreachable!("fixture is a typed create");
        };
        std::sync::Arc::make_mut(row).row.insert(
            "row_id".to_owned(),
            lix_schema::Value::Uuid(uuid::Uuid::from_u128(expected.as_u128() + 1)),
        );
        let error = materialize_keyless_creates(
            &mut WasmHostRowChanges {
                changes: vec![change],
            },
            bound.creates(),
            &create_catalog(),
        )
        .expect_err("conflicting plugin identity must be rejected");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
    }

    #[test]
    fn create_context_rejects_a_namespace_seed_without_uuid_v7_time_semantics() {
        let error = BoundCreateContext::bind(
            MutationIdentity {
                namespace_seed: [0x31; 16],
                operation_proof: [0x41; 32],
            },
            &actor_key(),
        )
        .expect_err("arbitrary retry bytes must not become a UUID timestamp");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(error.message.contains("UUIDv7"));
    }

    #[test]
    fn validation_distinguishes_current_existing_and_malformed_ids() {
        let old = BoundCreateContext::bind(mutation_identity(6, 5), &actor_key())
            .expect("valid UUIDv7 seed");
        let old_id = old.creates().component(1).unwrap();
        let changes = WasmRowChanges {
            changes: vec![create(0), upsert(old_id)],
        };
        let validation = validate_create_changes(&plugin(), &changes).expect("validate");
        assert!(validation.requires_reservation);
        assert_eq!(validation.existing_authorities.len(), 1);

        let malformed = WasmRowChanges {
            changes: vec![WasmRowChange::Create {
                schema_key: "other".into(),
                local_ref: 0,
                resolved_key: None,
                payload: typed_row("malformed"),
            }],
        };
        assert!(validate_create_changes(&plugin(), &malformed).is_err());
    }

    #[test]
    fn existing_authority_accepts_a_typed_uuid_primary_key() {
        let id = BoundCreateContext::bind(mutation_identity(6, 5), &actor_key())
            .expect("valid UUIDv7 seed")
            .creates()
            .component(1)
            .expect("generated UUID");
        let key =
            WasmRowKey::from_typed_parts("csv_row", [0; 32], vec![lix_schema::Value::Uuid(id)])
                .unwrap();
        let row = MaterializedHotStateRow {
            row_pk: RowPk::from_schema_values(&[lix_schema::Value::Uuid(id)])
                .expect("typed UUID primary key"),
            schema_key: "csv_row".into(),
            file_id: Some("01920000-0000-7000-8000-0000000000a2".into()),
            snapshot_content: None,
            metadata: None,
            deleted: false,
            created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
            updated_at: LixTimestamp::from_unix_millis_utc_lossy(1),
            global: false,
            change_id: None,
            commit_id: None,
            untracked: false,
            branch_id: "main".into(),
        };

        let mut batch = MaterializedHotStateBatchBuilder::with_capacity(1);
        batch.push_owned(row.clone());
        let ordinal = 0;
        batch.set_decoded_snapshot(
            ordinal,
            Some(std::sync::Arc::new(WasmTypedRow {
                schema_fingerprint: [0; 32],
                row_pk: vec![lix_schema::Value::Uuid(id)].into(),
                row: lix_schema::Row::from([("id".to_owned(), lix_schema::Value::Uuid(id))]),
                native_payload: std::sync::OnceLock::new(),
                boundary_create_validation: std::sync::OnceLock::new(),
            })),
        );
        let exact = MaterializedHotStateExactBatch::new(
            batch.finish(),
            vec![Some(u32::try_from(ordinal).expect("one test row fits u32"))],
        )
        .expect("typed authority batch");

        require_existing_id_authorities(
            &plugin(),
            &[key.clone()],
            &exact,
            "01920000-0000-7000-8000-0000000000a2",
            "main",
            false,
        )
        .expect("typed UUID authority must compare without string-only accessors");

        // The authority row must be matched in the requesting file's own lane.
        // A tracked row can never satisfy an untracked file's create authority
        // and vice versa, or a create would reserve across the lane boundary.
        require_existing_id_authorities(
            &plugin(),
            &[key],
            &exact,
            "01920000-0000-7000-8000-0000000000a2",
            "main",
            true,
        )
        .expect_err("a tracked authority row must not satisfy an untracked create");
    }

    #[test]
    fn reservation_accepts_same_proof_and_rejects_seed_collision() {
        let first = BoundCreateContext::bind(mutation_identity(7, 8), &actor_key())
            .expect("valid UUIDv7 seed");
        let existing = row_for(first);
        assert!(
            reserve_create_row(
                Some(&existing),
                first,
                "01920000-0000-7000-8000-0000000000a2",
                "main",
                false
            )
            .expect("same proof")
            .is_none()
        );

        let collision = BoundCreateContext::bind(mutation_identity(7, 9), &actor_key())
            .expect("valid UUIDv7 seed");
        assert_eq!(first.prefix, collision.prefix);
        let error = reserve_create_row(
            Some(&existing),
            collision,
            "01920000-0000-7000-8000-0000000000a2",
            "main",
            false,
        )
        .expect_err("different proof must fail");
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
    }

    #[test]
    fn reservation_preflight_reports_seed_collision_as_constraint_violation() {
        let first = BoundCreateContext::bind(mutation_identity(0x31, 0x41), &actor_key())
            .expect("valid UUIDv7 seed");
        let existing = row_for(first);
        let collision = BoundCreateContext::bind(mutation_identity(0x31, 0x42), &actor_key())
            .expect("valid UUIDv7 seed");

        let error = validate_create_reservation(
            Some(&existing),
            collision,
            "01920000-0000-7000-8000-0000000000a2",
            "main",
            false,
        )
        .expect_err("preflight must reject a reused seed before entering the guest");
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(error.message.contains("different operation proof"));
    }

    #[test]
    fn large_cold_import_and_sparse_insert_use_one_reservation_each() {
        const ROWS: u64 = 220_000;
        let actor_key = actor_key();
        let cold = BoundCreateContext::bind(mutation_identity(1, 2), &actor_key)
            .expect("valid UUIDv7 seed");
        let cold_changes = WasmRowChanges {
            changes: (0..ROWS).map(create).collect(),
        };
        let plugin = plugin();
        let validation =
            validate_create_changes(&plugin, &cold_changes).expect("large cold import creates");
        assert!(validation.requires_reservation);
        assert!(validation.existing_authorities.is_empty());
        assert_eq!(
            reserve_create_row(
                None,
                cold,
                "01920000-0000-7000-8000-0000000000a2",
                "main",
                false
            )
            .expect("cold reservation")
            .into_iter()
            .count(),
            1,
        );

        let edit = BoundCreateContext::bind(mutation_identity(3, 4), &actor_key)
            .expect("valid UUIDv7 seed");
        let edit_changes = WasmRowChanges {
            changes: vec![upsert(cold.creates().component(17).unwrap())],
        };
        let validation =
            validate_create_changes(&plugin, &edit_changes).expect("existing-row edit IDs");
        assert!(!validation.requires_reservation);
        assert_eq!(validation.existing_authorities.len(), 1);

        let insert_changes = WasmRowChanges {
            changes: vec![create(0)],
        };
        let validation =
            validate_create_changes(&plugin, &insert_changes).expect("sparse insert IDs");
        assert!(validation.requires_reservation);
        assert!(validation.existing_authorities.is_empty());
        assert_eq!(
            reserve_create_row(
                None,
                edit,
                "01920000-0000-7000-8000-0000000000a2",
                "main",
                false
            )
            .expect("insert reservation")
            .into_iter()
            .count(),
            1,
        );
    }
}
