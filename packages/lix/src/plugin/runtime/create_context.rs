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
#[cfg(test)]
use crate::common::LixTimestamp;
use crate::common::MutationIdentity;
use crate::plugin::runtime::{
    WasmChangeEffect, WasmCreateContext, WasmHostBytes, WasmHostRowChanges, WasmRow, WasmRowChange,
    WasmRowChanges, WasmRowKey,
};
use crate::row_pk::RowPk;
use crate::state::StateRow;
use crate::transaction::types::{TransactionJson, TransactionWriteRow};

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
    BlobId::from_canonical_content(&input).into_bytes()
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
                if creatable.binary_search(schema_key).is_err() {
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
) -> Result<(), LixError> {
    for change in &mut changes.changes {
        let WasmRowChange::Create {
            schema_key,
            local_ref,
            resolved_key,
            snapshot_content,
        } = change
        else {
            continue;
        };
        let id = creates.component(*local_ref)?;
        let WasmHostBytes::CanonicalJson(canonical) = snapshot_content else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "validated keyless creates must own parsed canonical snapshots",
            ));
        };
        let key = resolved_key.take().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "validated keyless create has no schema-resolved primary key",
            )
        })?;
        if key.schema_key.as_str() != schema_key
            || key.row_pk.len() != 1
            || key.row_pk[0].as_str() != id
        {
            return Err(invalid_id(format!(
                "resolved keyless create for schema '{schema_key}' does not match its create context"
            )));
        }
        if canonical.certificate().is_none() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "resolved keyless create did not retain its schema-validation certificate",
            ));
        }
        *change = WasmRowChange::Upsert {
            row: WasmRow {
                key,
                snapshot_content: WasmHostBytes::CanonicalJson(canonical.clone()),
            },
            effect: WasmChangeEffect::Content,
        };
    }
    Ok(())
}

pub(crate) fn require_existing_id_authorities(
    plugin: &PluginRegistryEntry,
    keys: &[WasmRowKey],
    rows: &[Option<StateRow>],
    file_id: &str,
    branch_id: &str,
) -> Result<(), LixError> {
    if keys.len() != rows.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "create authority lookup returned the wrong cardinality",
        ));
    }
    for (slot, key) in keys.iter().enumerate() {
        let valid = rows.get(slot).and_then(Option::as_ref).is_some_and(|row| {
            let Ok(state_key) = crate::forktree::decode_state_key(&row.key) else {
                return false;
            };
            !row.value.cell.deleted()
                && matches!(row.value.cell, crate::forktree::StateCell::NativeRow(_))
                && key.schema_key == state_key.schema_key
                && key.row_pk.len() == 1
                && RowPk::uuid_from_canonical(&key.row_pk[0])
                    .is_ok_and(|row_pk| state_key.row_pk == row_pk)
                && state_key.file_id.as_deref() == Some(file_id)
                && branch_id != crate::GLOBAL_BRANCH_ID
                && matches!(row.source, crate::state::StateRowSource::Branch)
        });
        if !valid {
            return Err(LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "plugin '{}' emitted a keyed update for creatable schema '{}' row {:?}, but that row does not exist",
                    plugin.key(), key.schema_key, key.row_pk
                ),
            )
            .with_hint(
                "Use keyless Create for new rows and keyed Upsert only for existing rows.",
            ));
        }
    }
    Ok(())
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
    existing: Option<&StateRow>,
    bound: BoundCreateContext,
    file_id: &str,
    branch_id: &str,
) -> Result<Option<TransactionWriteRow>, LixError> {
    validate_create_reservation(existing, bound, file_id, branch_id)?;
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
    )?))
}

/// Validates an already-reserved create context before entering a guest transition.
///
/// This preflight is deliberately independent of whether the eventual sparse
/// change set creates an row. A client presenting a reserved context with a
/// different full proof has already violated the mutation-identity contract;
/// rejecting it here prevents guest-local allocator errors from obscuring the
/// public constraint violation.
pub(crate) fn validate_create_reservation(
    existing: Option<&StateRow>,
    bound: BoundCreateContext,
    file_id: &str,
    branch_id: &str,
) -> Result<(), LixError> {
    let Some(row) = existing else {
        return Ok(());
    };
    let key = bound.reservation_key();
    validate_reservation_identity(row, &key, file_id, branch_id)?;
    let snapshot = row
        .seed_logical_snapshot(branch_id)?
        .ok_or_else(|| invalid_id(format!("create reservation '{key}' has no snapshot")))?;
    let snapshot: JsonValue = serde_json::from_str(&snapshot).map_err(|error| {
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
) -> Result<TransactionWriteRow, LixError> {
    if !is_reservation_key(key) {
        return Err(invalid_id("invalid create reservation key"));
    }
    reservation_row(key.to_string(), None, file_id, branch_id)
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
        untracked: false,
        branch_id: branch_id.into(),
    })
}

fn validate_reservation_identity(
    row: &StateRow,
    key: &str,
    file_id: &str,
    branch_id: &str,
) -> Result<(), LixError> {
    let state_key = crate::forktree::decode_state_key(&row.key).map_err(|_| {
        invalid_id(format!(
            "create reservation '{key}' has an invalid state key"
        ))
    })?;
    if state_key.schema_key != KEY_VALUE_SCHEMA_KEY
        || state_key.row_pk.as_single_string().ok() != Some(key)
        || state_key.file_id.as_deref() != Some(file_id)
        || !matches!(row.source, crate::state::StateRowSource::Branch)
        || row.value.cell.deleted()
    {
        return Err(invalid_id(format!(
            "create reservation '{key}' has invalid tracked file scope"
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
    use crate::plugin::runtime::{WasmCanonicalJson, WasmChangeEffect, WasmHostBytes, WasmRow};
    use crate::plugin::{PluginRegistryEntryInput, PluginRuntime};

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
            api_version: "1.0.0".to_string(),
            path_glob: "*.csv".to_string(),
            content: None,
            entry: "plugin.wasm".to_string(),
            schema_keys: vec!["csv_row".to_string()],
            create_schema_keys: vec!["csv_row".to_string()],
            manifest_json: r#"{"entry":"plugin.wasm","key":"plugin_csv","match":{"path_glob":"*.csv"},"schemas":["schema/csv_row.json"]}"#.to_string(),
            archive_file_id: crate::plugin::plugin_storage_archive_file_id("plugin_csv"),
            archive_path: "/.lix/plugins/plugin_csv.lixplugin".to_string(),
            archive_blob_hash: "a".repeat(64),
            wasm_blob_hash: "b".repeat(64),
        })
        .expect("plugin")
    }

    fn upsert(id: String) -> WasmRowChange<WasmHostBytes> {
        WasmRowChange::Upsert {
            row: WasmRow {
                key: WasmRowKey::from_owned_parts("csv_row", vec![id]),
                snapshot_content: WasmHostBytes::Inline(b"{}".to_vec().into()),
            },
            effect: WasmChangeEffect::Content,
        }
    }

    fn canonical(value: JsonValue) -> WasmHostBytes {
        let normalized = serde_json::to_vec(&value).expect("canonical test JSON");
        let normalized_len = u32::try_from(normalized.len()).expect("test JSON fits u32");
        let canonical = WasmCanonicalJson::from_batch_parts(
            vec![value],
            normalized,
            vec![(0, normalized_len)],
            0,
            1,
        )
        .expect("canonical test batch")
        .pop()
        .expect("one canonical test row");
        WasmHostBytes::CanonicalJson(canonical)
    }

    fn create(local_ref: u64) -> WasmRowChange<WasmHostBytes> {
        WasmRowChange::Create {
            schema_key: "csv_row".to_string(),
            local_ref,
            resolved_key: None,
            snapshot_content: canonical(json!({})),
        }
    }

    fn row_for(bound: BoundCreateContext) -> StateRow {
        let write = reserve_create_row(None, bound, "01920000-0000-7000-8000-0000000000a2", "main")
            .expect("reserve")
            .expect("new row");
        state_row_from_write(write)
    }

    #[test]
    fn create_context_produces_stable_uuid_v7_values() {
        let creates = BoundCreateContext::bind(mutation_identity(7, 8), &actor_key())
            .expect("valid UUIDv7 seed")
            .creates();
        let value = creates.component(42).expect("uuid");
        let parsed = uuid::Uuid::parse_str(&value).expect("canonical UUID");
        assert_eq!(parsed.get_version_num(), 7);
        assert_eq!(&parsed.as_bytes()[12..], &42_u32.to_be_bytes());
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
                schema_key: "other".to_string(),
                local_ref: 0,
                resolved_key: None,
                snapshot_content: WasmHostBytes::Inline(Vec::new().into()),
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
        let key = WasmRowKey::from_owned_parts("csv_row", vec![id.clone()]);
        let row = StateRow {
            key: crate::forktree::encode_state_key(crate::forktree::StateKeyRef {
                schema_key: "csv_row",
                file_id: Some("01920000-0000-7000-8000-0000000000a2"),
                row_pk: &RowPk::uuid_from_canonical(&id).expect("typed UUID primary key"),
            }),
            value: crate::forktree::StateValue {
                change_id: crate::changelog::ChangeId::for_test_label("change"),
                commit_id: crate::changelog::CommitId::for_test_label("commit"),
                created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(1),
                cell: crate::forktree::StateCell::Value("{}".into()),
                metadata: None,
                origin_key: None,
                blob_manifest_object_ids: Vec::new(),
            },
            source: crate::state::StateRowSource::Branch,
        };

        require_existing_id_authorities(
            &plugin(),
            &[key],
            &[Some(row)],
            "01920000-0000-7000-8000-0000000000a2",
            "main",
        )
        .expect("typed UUID authority must compare without string-only accessors");
    }

    #[test]
    fn unresolved_create_is_rejected_before_transaction_staging() {
        let context = BoundCreateContext::bind(mutation_identity(7, 8), &actor_key())
            .expect("valid UUIDv7 seed")
            .creates();
        let mut changes = WasmRowChanges {
            changes: vec![WasmRowChange::Create {
                schema_key: "csv_row".to_string(),
                local_ref: 42,
                resolved_key: None,
                snapshot_content: canonical(json!({
                    "cells": ["Alice", "42"],
                    "order_key": "4000000000000001"
                })),
            }],
        };

        let error = materialize_keyless_creates(&mut changes, context)
            .expect_err("schema resolution must precede transaction staging");
        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
        assert!(error.message.contains("schema-resolved primary key"));
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
                "main"
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
        )
        .expect_err("preflight must reject a reused seed before entering the guest");
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(error.message.contains("different operation proof"));
    }

    fn state_row_from_write(write: TransactionWriteRow) -> StateRow {
        let row_pk = write.row_pk.expect("reservation row key");
        let key = crate::forktree::encode_state_key(crate::forktree::StateKeyRef {
            schema_key: &write.schema_key,
            file_id: write.file_id.as_deref(),
            row_pk: &row_pk,
        });
        let snapshot = write.snapshot.map(|snapshot| snapshot.normalized().into());
        StateRow {
            key,
            value: crate::forktree::StateValue {
                change_id: crate::changelog::ChangeId::for_test_label("change"),
                commit_id: crate::changelog::CommitId::for_test_label("commit"),
                created_at: LixTimestamp::from_unix_millis_utc_lossy(1),
                updated_at: LixTimestamp::from_unix_millis_utc_lossy(1),
                cell: snapshot.map_or(
                    crate::forktree::StateCell::Tombstone,
                    crate::forktree::StateCell::Value,
                ),
                metadata: None,
                origin_key: None,
                blob_manifest_object_ids: Vec::new(),
            },
            source: crate::state::StateRowSource::Branch,
        }
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
            reserve_create_row(None, cold, "01920000-0000-7000-8000-0000000000a2", "main")
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
            reserve_create_row(None, edit, "01920000-0000-7000-8000-0000000000a2", "main")
                .expect("insert reservation")
                .into_iter()
                .count(),
            1,
        );
    }
}
