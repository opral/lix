//! Durable generated-ID namespace authority for production v2 plugins.
//!
//! Components see only a compact 128-bit namespace. The engine retains the
//! full operation proof, binds both values to the file authority, and writes
//! one fixed-shape tracked reservation row when that namespace first emits an
//! identity. A colliding namespace seed with a different proof is rejected
//! before any semantic rows are staged.

use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::LixError;
use crate::binary_cas::BlobHash;
use crate::entity_pk::EntityPk;
use crate::live_state::MaterializedLiveStateRow;
use crate::session::MutationIdentity;
use crate::transaction::types::{TransactionJson, TransactionWriteRow};
use crate::wasm::v2::{WasmEntityChange, WasmEntityChanges, WasmEntityKey, WasmIdNamespace};

use super::{PluginActorKey, PluginRegistryEntry};

const KEY_VALUE_SCHEMA_KEY: &str = "lix_key_value";
const RESERVATION_PREFIX: &str = "lix_plugin_id_namespace_v1:";
const RESERVATION_VERSION: u32 = 1;

/// A mutation identity after it has been bound to one durable plugin-file
/// authority. Different operation proofs may deliberately yield the same
/// namespace when their 128-bit seeds collide; the reservation row detects
/// that condition using `bound_operation_proof`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BoundIdNamespace {
    namespace: [u8; 16],
    bound_operation_proof: [u8; 32],
    authority_binding: [u8; 32],
}

impl BoundIdNamespace {
    pub(crate) fn bind(identity: MutationIdentity, actor_key: &PluginActorKey) -> Self {
        let authority_binding = authority_binding(actor_key);
        let namespace_digest = framed_digest(
            b"lix.plugin-v2.bound-namespace.v2\0",
            &[&identity.namespace_seed, &authority_binding],
        );
        let bound_operation_proof = framed_digest(
            b"lix.plugin-v2.bound-operation-proof.v1\0",
            &[&identity.operation_proof, &authority_binding],
        );
        Self {
            namespace: namespace_digest[..16]
                .try_into()
                .expect("digest has a 16-byte namespace prefix"),
            bound_operation_proof,
            authority_binding,
        }
    }

    pub(crate) fn ids(self) -> WasmIdNamespace {
        WasmIdNamespace {
            high: u64::from_be_bytes(
                self.namespace[..8]
                    .try_into()
                    .expect("namespace has high bytes"),
            ),
            low: u64::from_be_bytes(
                self.namespace[8..]
                    .try_into()
                    .expect("namespace has low bytes"),
            ),
        }
    }

    pub(crate) fn reservation_key(self) -> String {
        format!("{RESERVATION_PREFIX}{}", encode_hex(&self.namespace))
    }
}

/// Creates a complete proof for local calls that do not arrive through the
/// remote mutation protocol. The caller must supply a fresh 128-bit seed.
pub(crate) fn local_mutation_identity(namespace_seed: [u8; 16]) -> MutationIdentity {
    MutationIdentity {
        namespace_seed,
        operation_proof: framed_digest(
            b"lix.plugin-v2.local-operation-proof.v1\0",
            &[&namespace_seed],
        ),
    }
}

fn authority_binding(actor_key: &PluginActorKey) -> [u8; 32] {
    framed_digest(
        b"lix.plugin-v2.id-authority.v1\0",
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
    BlobHash::from_content(&input).into_bytes()
}

/// Result of validating generated identities in one sparse guest transition.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct IdAllocationValidation {
    /// A current-namespace identity was emitted. The transaction must check or
    /// create the corresponding durable reservation before staging changes.
    pub(crate) requires_reservation: bool,
    /// Non-current compact identities are valid only when the exact durable
    /// entity already exists. UUID identities are the explicit legacy format,
    /// but they remain authorities only when already durable.
    pub(crate) existing_authorities: Vec<WasmEntityKey>,
}

pub(crate) fn validate_host_allocated_changes<B>(
    plugin: &PluginRegistryEntry,
    changes: &WasmEntityChanges<B>,
    bound: BoundIdNamespace,
) -> Result<IdAllocationValidation, LixError> {
    if plugin.host_allocated_schema_keys().is_empty() {
        return Ok(IdAllocationValidation::default());
    }
    let host_allocated = plugin.host_allocated_schema_keys();
    let mut validation = IdAllocationValidation::default();
    for group in &changes.groups {
        for change in &group.changes {
            let WasmEntityChange::Upsert { entity, .. } = change else {
                continue;
            };
            if host_allocated
                .binary_search(&entity.key.schema_key)
                .is_err()
            {
                continue;
            }
            let [component] = entity.key.entity_pk.as_slice() else {
                return Err(invalid_id(format!(
                    "plugin '{}' schema '{}' must emit one-component host-allocated entity keys",
                    plugin.key(),
                    entity.key.schema_key
                )));
            };
            if let Some((namespace, _ordinal)) = decode_compact_id(component) {
                if namespace == bound.namespace {
                    validation.requires_reservation = true;
                } else {
                    validation.existing_authorities.push(entity.key.clone());
                }
            } else if is_legacy_uuid(component) {
                validation.existing_authorities.push(entity.key.clone());
            } else {
                return Err(invalid_id(format!(
                    "plugin '{}' emitted malformed host-allocated ID for schema '{}'; expected a 32-character compact ID or a legacy UUID",
                    plugin.key(),
                    entity.key.schema_key
                )));
            }
        }
    }
    validation.existing_authorities.sort();
    validation.existing_authorities.dedup();
    Ok(validation)
}

pub(crate) fn require_existing_id_authorities(
    plugin: &PluginRegistryEntry,
    keys: &[WasmEntityKey],
    rows: &[Option<MaterializedLiveStateRow>],
    file_id: &str,
    branch_id: &str,
) -> Result<(), LixError> {
    if keys.len() != rows.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "host-allocated ID authority lookup returned the wrong cardinality",
        ));
    }
    for (key, row) in keys.iter().zip(rows) {
        let valid = row.as_ref().is_some_and(|row| {
            !row.deleted
                && row.snapshot_content.is_some()
                && row.schema_key == key.schema_key
                && row.entity_pk.clone().into_parts() == key.entity_pk
                && row.file_id.as_deref() == Some(file_id)
                && row.branch_id == branch_id
                && !row.global
                && !row.untracked
        });
        if !valid {
            return Err(LixError::new(
                LixError::CODE_INVALID_PLUGIN,
                format!(
                    "plugin '{}' emitted non-current host-allocated ID for new schema '{}' entity {:?}",
                    plugin.key(), key.schema_key, key.entity_pk
                ),
            )
            .with_hint(
                "Allocate new identities from the id-namespace supplied to this transition.",
            ));
        }
    }
    Ok(())
}

/// Strictly decodes the 24-byte `namespace || ordinal` representation. The
/// decoder accepts only the unpadded base64url alphabet and exactly 32 bytes;
/// aliases, whitespace, padding, and trailing input are rejected.
pub(crate) fn decode_compact_id(value: &str) -> Option<([u8; 16], u64)> {
    let encoded = value.as_bytes();
    if encoded.len() != 32 {
        return None;
    }
    let mut decoded = [0u8; 24];
    for (input, output) in encoded.chunks_exact(4).zip(decoded.chunks_exact_mut(3)) {
        let a = decode_base64url(input[0])?;
        let b = decode_base64url(input[1])?;
        let c = decode_base64url(input[2])?;
        let d = decode_base64url(input[3])?;
        output[0] = a << 2 | b >> 4;
        output[1] = b << 4 | c >> 2;
        output[2] = c << 6 | d;
    }
    Some((
        decoded[..16].try_into().ok()?,
        u64::from_be_bytes(decoded[16..].try_into().ok()?),
    ))
}

fn decode_base64url(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    }
}

fn is_legacy_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|uuid| uuid.hyphenated().to_string() == value)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReservationValue {
    version: u32,
    operation_proof: String,
    authority_binding: String,
}

/// Returns a row to stage when the reservation is absent, accepts an exact
/// same-proof replay without another write, and rejects a truncated-namespace
/// collision before semantic rows enter the transaction buffer.
pub(crate) fn reserve_namespace_row(
    existing: Option<&MaterializedLiveStateRow>,
    bound: BoundIdNamespace,
    file_id: &str,
    branch_id: &str,
) -> Result<Option<TransactionWriteRow>, LixError> {
    validate_namespace_reservation(existing, bound, file_id, branch_id)?;
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

/// Validates an already-reserved namespace before entering a guest transition.
///
/// This preflight is deliberately independent of whether the eventual sparse
/// change set allocates an ID. A client presenting a reserved namespace with a
/// different full proof has already violated the mutation-identity contract;
/// rejecting it here prevents guest-local allocator errors from obscuring the
/// public constraint violation.
pub(crate) fn validate_namespace_reservation(
    existing: Option<&MaterializedLiveStateRow>,
    bound: BoundIdNamespace,
    file_id: &str,
    branch_id: &str,
) -> Result<(), LixError> {
    let Some(row) = existing else {
        return Ok(());
    };
    let key = bound.reservation_key();
    validate_reservation_identity(row, &key, file_id, branch_id)?;
    let snapshot = row
        .snapshot_content
        .as_deref()
        .ok_or_else(|| invalid_id(format!("namespace reservation '{key}' has no snapshot")))?;
    let snapshot: JsonValue = serde_json::from_str(snapshot).map_err(|error| {
        invalid_id(format!(
            "namespace reservation '{key}' is invalid JSON: {error}"
        ))
    })?;
    let object = snapshot
        .as_object()
        .ok_or_else(|| invalid_id(format!("namespace reservation '{key}' must be an object")))?;
    if object.len() != 2 || object.get("key").and_then(JsonValue::as_str) != Some(&key) {
        return Err(invalid_id(format!(
            "namespace reservation '{key}' has invalid key-value shape"
        )));
    }
    let value: ReservationValue = serde_json::from_value(
        object
            .get("value")
            .cloned()
            .ok_or_else(|| invalid_id(format!("namespace reservation '{key}' has no value")))?,
    )
    .map_err(|error| {
        invalid_id(format!(
            "namespace reservation '{key}' has an invalid value: {error}"
        ))
    })?;
    let operation_proof = decode_hex_32(&value.operation_proof).ok_or_else(|| {
        invalid_id(format!(
            "namespace reservation '{key}' has an invalid operation proof"
        ))
    })?;
    let authority_binding = decode_hex_32(&value.authority_binding).ok_or_else(|| {
        invalid_id(format!(
            "namespace reservation '{key}' has an invalid authority binding"
        ))
    })?;
    if value.version != RESERVATION_VERSION || authority_binding != bound.authority_binding {
        return Err(invalid_id(format!(
            "namespace reservation '{key}' does not match the current file authority"
        )));
    }
    if operation_proof != bound.bound_operation_proof {
        return Err(LixError::new(
            LixError::CODE_CONSTRAINT_VIOLATION,
            "generated-ID namespace collision: the namespace is reserved by a different operation proof",
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
        return Err(invalid_id("invalid namespace reservation key"));
    }
    reservation_row(key.to_string(), None, file_id, branch_id)
}

pub(crate) fn is_reservation_key(key: &str) -> bool {
    key.strip_prefix(RESERVATION_PREFIX)
        .is_some_and(|suffix| suffix.len() == 32 && suffix.bytes().all(is_lower_hex))
}

fn reservation_row(
    key: String,
    snapshot: Option<JsonValue>,
    file_id: &str,
    branch_id: &str,
) -> Result<TransactionWriteRow, LixError> {
    if file_id.is_empty() || branch_id.is_empty() || branch_id == crate::GLOBAL_BRANCH_ID {
        return Err(invalid_id(
            "namespace reservations require a file-scoped tracked branch",
        ));
    }
    Ok(TransactionWriteRow {
        entity_pk: Some(EntityPk::single(key)),
        schema_key: KEY_VALUE_SCHEMA_KEY.to_string(),
        file_id: Some(file_id.to_string()),
        snapshot: snapshot
            .map(|value| TransactionJson::from_value(value, "plugin ID namespace reservation"))
            .transpose()?,
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: false,
        change_id: None,
        commit_id: None,
        untracked: false,
        branch_id: branch_id.to_string(),
    })
}

fn validate_reservation_identity(
    row: &MaterializedLiveStateRow,
    key: &str,
    file_id: &str,
    branch_id: &str,
) -> Result<(), LixError> {
    if row.schema_key != KEY_VALUE_SCHEMA_KEY
        || row.entity_pk.as_single_string().ok() != Some(key)
        || row.file_id.as_deref() != Some(file_id)
        || row.branch_id != branch_id
        || row.global
        || row.untracked
        || row.deleted
    {
        return Err(invalid_id(format!(
            "namespace reservation '{key}' has invalid tracked file scope"
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
    use crate::plugin::{PluginRegistryEntryInput, PluginRuntime};
    use crate::wasm::v2::{WasmChangeEffect, WasmEntity, WasmHostBytes, WasmMergeGroup};

    fn actor_key() -> PluginActorKey {
        PluginActorKey {
            branch_id: "main".to_string(),
            file_id: "file-a".to_string(),
            path: "/a.csv".to_string(),
            owner_change_id: "owner-a".to_string(),
            plugin_key: "plugin_csv_v2".to_string(),
            plugin_generation: "a".repeat(64),
        }
    }

    fn plugin() -> PluginRegistryEntry {
        PluginRegistryEntry::new(PluginRegistryEntryInput {
            key: "plugin_csv_v2".to_string(),
            runtime: PluginRuntime::WasmComponentV2,
            api_version: "2.0.0".to_string(),
            path_glob: "*.csv".to_string(),
            content_type: None,
            entry: "plugin.wasm".to_string(),
            schema_keys: vec!["csv_row".to_string()],
            host_allocated_schema_keys: vec!["csv_row".to_string()],
            manifest_json: r#"{"key":"plugin_csv_v2","runtime":"wasm-component-v2","api_version":"2.0.0","match":{"path_glob":"*.csv"},"entry":"plugin.wasm","schemas":["schema/csv_row.json"]}"#.to_string(),
            archive_file_id: "lix_plugin_archive::plugin_csv_v2".to_string(),
            archive_path: "/.lix/plugins/plugin_csv_v2.lixplugin".to_string(),
            archive_blob_hash: "a".repeat(64),
            wasm_blob_hash: "b".repeat(64),
        })
        .expect("plugin")
    }

    fn upsert(id: String) -> WasmEntityChange<WasmHostBytes> {
        WasmEntityChange::Upsert {
            entity: WasmEntity {
                key: WasmEntityKey {
                    schema_key: "csv_row".to_string(),
                    entity_pk: vec![id],
                },
                snapshot_content: WasmHostBytes::Inline(b"{}".to_vec()),
            },
            effect: WasmChangeEffect::Content,
        }
    }

    fn row_for(bound: BoundIdNamespace) -> MaterializedLiveStateRow {
        let write = reserve_namespace_row(None, bound, "file-a", "main")
            .expect("reserve")
            .expect("new row");
        MaterializedLiveStateRow {
            entity_pk: write.entity_pk.expect("pk"),
            schema_key: write.schema_key,
            file_id: write.file_id,
            snapshot_content: write
                .snapshot
                .map(|snapshot| snapshot.normalized().to_string()),
            metadata: None,
            deleted: false,
            created_at: "1".to_string(),
            updated_at: "1".to_string(),
            global: false,
            change_id: None,
            commit_id: None,
            untracked: false,
            branch_id: "main".to_string(),
        }
    }

    fn authority_row(id: &str) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: EntityPk::single(id),
            schema_key: "csv_row".to_string(),
            file_id: Some("file-a".to_string()),
            snapshot_content: Some(r#"{"id":"legacy"}"#.to_string()),
            metadata: None,
            deleted: false,
            created_at: "1".to_string(),
            updated_at: "1".to_string(),
            global: false,
            change_id: None,
            commit_id: None,
            untracked: false,
            branch_id: "main".to_string(),
        }
    }

    #[test]
    fn compact_id_decoder_is_strict() {
        let ids = BoundIdNamespace::bind(
            MutationIdentity {
                namespace_seed: [7; 16],
                operation_proof: [8; 32],
            },
            &actor_key(),
        )
        .ids();
        let value = ids.component(42);
        let (namespace, ordinal) = decode_compact_id(&value).expect("decode");
        assert_eq!(namespace[..8], ids.high.to_be_bytes());
        assert_eq!(namespace[8..], ids.low.to_be_bytes());
        assert_eq!(ordinal, 42);
        assert!(decode_compact_id(&(value.clone() + "=")).is_none());
        assert!(decode_compact_id(&value.replace('-', "+")).is_none());
        assert!(decode_compact_id("short").is_none());
    }

    #[test]
    fn validation_distinguishes_current_existing_and_malformed_ids() {
        let bound = BoundIdNamespace::bind(
            MutationIdentity {
                namespace_seed: [7; 16],
                operation_proof: [8; 32],
            },
            &actor_key(),
        );
        let old = BoundIdNamespace::bind(
            MutationIdentity {
                namespace_seed: [6; 16],
                operation_proof: [5; 32],
            },
            &actor_key(),
        );
        let changes = WasmEntityChanges {
            groups: vec![WasmMergeGroup {
                changes: vec![
                    upsert(bound.ids().component(0)),
                    upsert(old.ids().component(1)),
                ],
            }],
        };
        let validation =
            validate_host_allocated_changes(&plugin(), &changes, bound).expect("validate");
        assert!(validation.requires_reservation);
        assert_eq!(validation.existing_authorities.len(), 1);

        let malformed = WasmEntityChanges {
            groups: vec![WasmMergeGroup {
                changes: vec![upsert("not-an-id".to_string())],
            }],
        };
        assert!(validate_host_allocated_changes(&plugin(), &malformed, bound).is_err());
    }

    #[test]
    fn reservation_accepts_same_proof_and_rejects_seed_collision() {
        let first = BoundIdNamespace::bind(
            MutationIdentity {
                namespace_seed: [7; 16],
                operation_proof: [8; 32],
            },
            &actor_key(),
        );
        let existing = row_for(first);
        assert!(
            reserve_namespace_row(Some(&existing), first, "file-a", "main")
                .expect("same proof")
                .is_none()
        );

        let collision = BoundIdNamespace::bind(
            MutationIdentity {
                namespace_seed: [7; 16],
                operation_proof: [9; 32],
            },
            &actor_key(),
        );
        assert_eq!(first.namespace, collision.namespace);
        let error = reserve_namespace_row(Some(&existing), collision, "file-a", "main")
            .expect_err("different proof must fail");
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
    }

    #[test]
    fn reservation_preflight_reports_seed_collision_as_constraint_violation() {
        let first = BoundIdNamespace::bind(
            MutationIdentity {
                namespace_seed: [0x31; 16],
                operation_proof: [0x41; 32],
            },
            &actor_key(),
        );
        let existing = row_for(first);
        let collision = BoundIdNamespace::bind(
            MutationIdentity {
                namespace_seed: [0x31; 16],
                operation_proof: [0x42; 32],
            },
            &actor_key(),
        );

        let error = validate_namespace_reservation(Some(&existing), collision, "file-a", "main")
            .expect_err("preflight must reject a reused seed before entering the guest");
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(error.message.contains("different operation proof"));
    }

    #[test]
    fn large_cold_import_and_sparse_insert_use_one_reservation_each() {
        const ROWS: u64 = 220_000;
        let actor_key = actor_key();
        let cold = BoundIdNamespace::bind(
            MutationIdentity {
                namespace_seed: [1; 16],
                operation_proof: [2; 32],
            },
            &actor_key,
        );
        let cold_changes = WasmEntityChanges {
            groups: vec![WasmMergeGroup {
                changes: (0..ROWS)
                    .map(|ordinal| upsert(cold.ids().component(ordinal)))
                    .collect(),
            }],
        };
        let plugin = plugin();
        let validation = validate_host_allocated_changes(&plugin, &cold_changes, cold)
            .expect("large cold import IDs");
        assert!(validation.requires_reservation);
        assert!(validation.existing_authorities.is_empty());
        assert_eq!(
            reserve_namespace_row(None, cold, "file-a", "main")
                .expect("cold reservation")
                .into_iter()
                .count(),
            1,
        );

        let edit = BoundIdNamespace::bind(
            MutationIdentity {
                namespace_seed: [3; 16],
                operation_proof: [4; 32],
            },
            &actor_key,
        );
        let edit_changes = WasmEntityChanges {
            groups: vec![WasmMergeGroup {
                changes: vec![upsert(cold.ids().component(17))],
            }],
        };
        let validation = validate_host_allocated_changes(&plugin, &edit_changes, edit)
            .expect("existing-row edit IDs");
        assert!(!validation.requires_reservation);
        assert_eq!(validation.existing_authorities.len(), 1);

        let insert_changes = WasmEntityChanges {
            groups: vec![WasmMergeGroup {
                changes: vec![upsert(edit.ids().component(0))],
            }],
        };
        let validation = validate_host_allocated_changes(&plugin, &insert_changes, edit)
            .expect("sparse insert IDs");
        assert!(validation.requires_reservation);
        assert!(validation.existing_authorities.is_empty());
        assert_eq!(
            reserve_namespace_row(None, edit, "file-a", "main")
                .expect("insert reservation")
                .into_iter()
                .count(),
            1,
        );
    }

    #[test]
    fn legacy_uuid_requires_exact_existing_authority() {
        let bound = BoundIdNamespace::bind(
            MutationIdentity {
                namespace_seed: [7; 16],
                operation_proof: [8; 32],
            },
            &actor_key(),
        );
        let uuid = "018f47d2-7b2e-7b4c-8e3a-0123456789ab";
        let changes = WasmEntityChanges {
            groups: vec![WasmMergeGroup {
                changes: vec![upsert(uuid.to_string())],
            }],
        };
        let plugin = plugin();
        let validation =
            validate_host_allocated_changes(&plugin, &changes, bound).expect("classify UUID");
        assert_eq!(validation.existing_authorities.len(), 1);
        assert!(
            require_existing_id_authorities(
                &plugin,
                &validation.existing_authorities,
                &[None],
                "file-a",
                "main",
            )
            .is_err()
        );
        require_existing_id_authorities(
            &plugin,
            &validation.existing_authorities,
            &[Some(authority_row(uuid))],
            "file-a",
            "main",
        )
        .expect("durable legacy UUID authority");
    }
}
