//! Engine-owned typed row payloads and durable encoding.
//!
//! SQL and plugin ingress share this owner. State readers use this contract
//! without depending on plugin execution or actor lifecycle.

use crate::{LixError, common::SharedStr, row_pk::RowPk};
use bytes::Bytes;
use std::sync::{Arc, OnceLock};

pub(crate) const COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION: u8 = 4;
// Zstd's fixed per-frame workspace dominates the small engine rows that make
// up SQL state. Keep those rows in the compact typed encoding directly; only
// pay the compression/decompression cost for payloads large enough to amortize
// it.
const ENGINE_ROW_COMPRESSION_THRESHOLD: usize = 4 * 1024;
const ENGINE_ROW_PAYLOAD_MAX_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug)]
pub struct TypedRow {
    pub(crate) schema_fingerprint: [u8; 32],
    pub(crate) row_pk: Arc<[lix_schema::Value]>,
    pub(crate) row: lix_schema::Row,
    pub(crate) native_payload: OnceLock<NativePayloadCache>,
    pub(crate) boundary_create_validation: OnceLock<BoundaryValidationToken>,
}

impl Clone for TypedRow {
    fn clone(&self) -> Self {
        // Boundary certificates authorize this exact owner and must never be
        // copied onto a value that `Arc::make_mut` is about to modify. Encoded
        // bytes are cleared for the same reason: the clone may be mutated and
        // must not retain a payload for its predecessor row.
        Self {
            schema_fingerprint: self.schema_fingerprint,
            row_pk: self.row_pk.clone(),
            row: self.row.clone(),
            native_payload: OnceLock::new(),
            boundary_create_validation: OnceLock::new(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum NativePayloadCache {
    /// Bytes reconstructed from durable state or encoded by an internal
    /// caller. Their presence alone carries no ingress-validation authority.
    Durable {
        bytes: Arc<[u8]>,
        boundary_validation: OnceLock<BoundaryValidationToken>,
    },
    /// Bytes encoded only after the catalog-backed component boundary proved
    /// the complete row shape and its typed identity.
    BoundaryValidated {
        bytes: Arc<[u8]>,
        _proof: BoundaryValidationToken,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct BoundaryValidationToken {
    _private: (),
}

impl NativePayloadCache {
    fn bytes(&self) -> &Arc<[u8]> {
        match self {
            Self::Durable { bytes, .. } | Self::BoundaryValidated { bytes, .. } => bytes,
        }
    }
}

impl PartialEq for TypedRow {
    fn eq(&self, other: &Self) -> bool {
        self.schema_fingerprint == other.schema_fingerprint
            && self.row_pk == other.row_pk
            && self.row == other.row
    }
}

fn json_ingress_error(schema_key: &str, error: lix_schema::Error) -> LixError {
    LixError::new(
        LixError::CODE_SCHEMA_VALIDATION,
        format!("snapshot_content conversion failed for schema '{schema_key}': {error}"),
    )
}

impl TypedRow {
    /// Encodes the exact two-column row shape certified by the SQL
    /// `path`/`value` replacement fast path without first materializing and
    /// reparsing an outer JSON object.
    pub(crate) fn append_certified_path_value_payload(
        output: &mut Vec<u8>,
        plan: &crate::catalog::SchemaPlan,
        path: &str,
        value: serde_json::Value,
    ) -> Result<(), LixError> {
        let engine_compact = crate::catalog::CatalogSnapshot::builtin()
            .plan_for_key(&plan.key.schema_key)
            .is_some_and(|(_, builtin)| builtin.fingerprint() == plan.fingerprint());
        let value = lix_schema::Jsonb::from(value);
        if !engine_compact {
            return crate::plugin::wire::typed::append_native_path_value_payload(
                output,
                &plan.fingerprint().bytes(),
                path,
                &value,
            )
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!(
                        "typed row for schema '{}' is not durably encodable: {error:?}",
                        plan.key.schema_key
                    ),
                )
            });
        }
        let row = lix_schema::Row::from([
            ("path", lix_schema::Value::Text(path.to_owned())),
            ("value", lix_schema::Value::Jsonb(value)),
        ]);
        plan.compiled_schema
            .validate_complete_row(&row)
            .map_err(|error| json_ingress_error(&plan.key.schema_key, error))?;
        let payload =
            crate::plugin::wire::typed::encode_engine_row_payload(&plan.compiled_schema, &row)
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_SCHEMA_VALIDATION,
                        format!(
                            "typed row for schema '{}' is not durably encodable: {error:?}",
                            plan.key.schema_key
                        ),
                    )
                })?;
        let payload = compress_durable_payload(payload).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "cannot compress typed row for schema '{}': {error:?}",
                    plan.key.schema_key
                ),
            )
        })?;
        output.extend_from_slice(&payload);
        Ok(())
    }

    /// Canonical-text counterpart used by bound SQL batches. Plugin-owned
    /// rows can retain the validated parameter bytes directly; built-in
    /// compact rows keep the general schema-validation fallback.
    pub(crate) fn try_append_certified_path_value_payload_from_canonical_json(
        output: &mut Vec<u8>,
        plan: &crate::catalog::SchemaPlan,
        path: &str,
        canonical_json: &[u8],
    ) -> Result<bool, LixError> {
        let engine_compact = crate::catalog::CatalogSnapshot::builtin()
            .plan_for_key(&plan.key.schema_key)
            .is_some_and(|(_, builtin)| builtin.fingerprint() == plan.fingerprint());
        if engine_compact {
            if lix_schema::validate_canonical_json_text(canonical_json).is_err() {
                return Ok(false);
            }
            let value = serde_json::from_slice(canonical_json).map_err(|error| {
                LixError::new(
                    LixError::CODE_SCHEMA_VALIDATION,
                    format!("canonical JSON parameter could not be decoded: {error}"),
                )
            })?;
            Self::append_certified_path_value_payload(output, plan, path, value)?;
            return Ok(true);
        }
        crate::plugin::wire::typed::try_append_native_path_value_payload_from_canonical_json(
            output,
            &plan.fingerprint().bytes(),
            path,
            canonical_json,
        )
        .map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "typed row for schema '{}' is not durably encodable: {error:?}",
                    plan.key.schema_key
                ),
            )
        })
    }

    #[cfg(any(test, feature = "storage-benches"))]
    pub(crate) fn from_test_json_unchecked(
        stored_row_pk: &RowPk,
        value: &serde_json::Value,
    ) -> Result<Self, LixError> {
        let object = value.as_object().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "test row must be a JSON object",
            )
        })?;
        let row_pk = stored_row_pk
            .components
            .iter()
            .map(|component| match component {
                crate::row_pk::RowPkComponent::Uuid(value) => {
                    lix_schema::Value::Uuid(uuid::Uuid::from_bytes(*value))
                }
                crate::row_pk::RowPkComponent::Integer(value) => lix_schema::Value::Int8(*value),
                crate::row_pk::RowPkComponent::String(value) => {
                    lix_schema::Value::Text(value.as_str().to_owned())
                }
                crate::row_pk::RowPkComponent::Bytes(value) => {
                    lix_schema::Value::Text(String::from_utf8_lossy(value).into_owned())
                }
            })
            .collect::<Vec<_>>();
        let row = object
            .iter()
            .map(|(name, value)| {
                let value = match value {
                    serde_json::Value::Null => lix_schema::Value::Null,
                    serde_json::Value::Bool(value) => lix_schema::Value::Boolean(*value),
                    serde_json::Value::Number(value) if value.is_i64() => {
                        lix_schema::Value::Int8(value.as_i64().expect("checked integer"))
                    }
                    serde_json::Value::Number(value) => lix_schema::Value::Float8(
                        value.as_f64().expect("JSON number converts to f64"),
                    ),
                    serde_json::Value::String(value) => lix_schema::Value::Text(value.clone()),
                    value => lix_schema::Value::Jsonb(value.clone().into()),
                };
                (name.clone(), value)
            })
            .collect();
        Ok(Self {
            schema_fingerprint: [0; 32],
            row_pk: row_pk.into(),
            row,
            native_payload: OnceLock::new(),
            boundary_create_validation: OnceLock::new(),
        })
    }

    /// Materializes the public JSON row shape only for legacy/API consumers
    /// that explicitly project `snapshot_content`. Durable state never calls
    /// this conversion.
    pub(crate) fn to_json_value(&self) -> Result<serde_json::Value, LixError> {
        let mut object = serde_json::Map::with_capacity(self.row.len());
        for (name, value) in &self.row {
            let value = match value {
                lix_schema::Value::Null => serde_json::Value::Null,
                lix_schema::Value::Text(value) => serde_json::Value::String(value.clone()),
                lix_schema::Value::Uuid(value) => serde_json::Value::String(value.to_string()),
                lix_schema::Value::Int8(value) => (*value).into(),
                lix_schema::Value::Float8(value) => serde_json::Number::from_f64(*value)
                    .map(serde_json::Value::Number)
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "typed row contains a non-finite float",
                        )
                    })?,
                lix_schema::Value::Boolean(value) => (*value).into(),
                lix_schema::Value::Jsonb(value) => value.as_value().clone(),
                lix_schema::Value::Timestamptz(value) => {
                    let timestamp =
                        chrono::DateTime::from_timestamp_micros(*value).ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "typed row contains an out-of-range timestamp",
                            )
                        })?;
                    let format = if value.rem_euclid(1_000) == 0 {
                        chrono::SecondsFormat::Millis
                    } else {
                        chrono::SecondsFormat::Micros
                    };
                    serde_json::Value::String(timestamp.to_rfc3339_opts(format, true))
                }
            };
            object.insert(name.to_owned(), value);
        }
        Ok(serde_json::Value::Object(object))
    }

    pub(crate) fn to_json_shared(&self) -> Result<SharedStr, LixError> {
        serde_json::to_string(&self.to_json_value()?)
            .map(Into::into)
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("cannot materialize typed row JSON projection: {error}"),
                )
            })
    }

    /// Builds an engine-owned row from a schema embedded in the binary. This
    /// is available before the persisted schema catalog can be hydrated.
    pub(crate) fn from_builtin_json(
        schema_key: &str,
        stored_row_pk: &RowPk,
        value: &serde_json::Value,
    ) -> Result<Self, LixError> {
        let (_, plan) = crate::catalog::CatalogSnapshot::builtin()
            .plan_for_key(schema_key)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("embedded schema catalog is missing '{schema_key}'"),
                )
            })?;
        let row = plan
            .compiled_schema
            .row_from_json(value)
            .map_err(|error| json_ingress_error(schema_key, error))?;
        Self::from_compiled_row(
            schema_key,
            &plan.compiled_schema,
            plan.fingerprint().bytes(),
            stored_row_pk,
            row,
            true,
        )
    }

    /// Converts an engine-owned canonical JSON row with a resolved Schema v1
    /// plan into the sole durable row representation.
    pub(crate) fn from_normalized_json(
        plan: &crate::catalog::SchemaPlan,
        stored_row_pk: &RowPk,
        value: &serde_json::Value,
    ) -> Result<Self, LixError> {
        Self::from_compiled_normalized_json(
            &plan.key.schema_key,
            &plan.compiled_schema,
            plan.fingerprint().bytes(),
            stored_row_pk,
            value,
        )
    }

    pub(crate) fn from_compiled_normalized_json(
        schema_key: &str,
        compiled_schema: &lix_schema::CompiledSchema,
        schema_fingerprint: [u8; 32],
        stored_row_pk: &RowPk,
        value: &serde_json::Value,
    ) -> Result<Self, LixError> {
        let row = compiled_schema
            .row_from_json(value)
            .map_err(|error| json_ingress_error(schema_key, error))?;
        let engine_compact = crate::catalog::CatalogSnapshot::builtin()
            .plan_for_key(schema_key)
            .is_some_and(|(_, plan)| plan.fingerprint().bytes() == schema_fingerprint);
        Self::from_compiled_row(
            schema_key,
            compiled_schema,
            schema_fingerprint,
            stored_row_pk,
            row,
            engine_compact,
        )
    }

    fn from_compiled_row(
        schema_key: &str,
        compiled_schema: &lix_schema::CompiledSchema,
        schema_fingerprint: [u8; 32],
        stored_row_pk: &RowPk,
        row: lix_schema::Row,
        engine_compact: bool,
    ) -> Result<Self, LixError> {
        let row_pk = compiled_schema
            .primary_key()
            .iter()
            .map(|column| {
                row.get(column).cloned().ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "typed row for schema '{}' is missing primary-key column '{column}'",
                            schema_key
                        ),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let durable_row_pk = RowPk::from_schema_values(&row_pk).map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "typed row for schema '{}' has an invalid primary key: {error}",
                    schema_key
                ),
            )
        })?;
        if &durable_row_pk != stored_row_pk {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "typed row for schema '{}' changed identity during JSON ingress conversion",
                    schema_key
                ),
            ));
        }
        let payload = (if engine_compact {
            crate::plugin::wire::typed::encode_engine_row_payload(compiled_schema, &row)
        } else {
            crate::plugin::wire::typed::encode_native_row_payload(
                &schema_fingerprint,
                &row_pk,
                &row,
            )
        })
        .map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "typed row for schema '{}' is not durably encodable: {error:?}",
                    schema_key
                ),
            )
        })?;
        let payload: Arc<[u8]> = if engine_compact {
            compress_durable_payload(payload).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("cannot compress typed row for schema '{schema_key}': {error:?}"),
                )
            })?
        } else {
            payload
        }
        .into();
        let typed = Self {
            schema_fingerprint,
            row_pk: row_pk.into(),
            row,
            native_payload: OnceLock::from(NativePayloadCache::Durable {
                bytes: payload,
                boundary_validation: OnceLock::new(),
            }),
            boundary_create_validation: OnceLock::new(),
        };
        Ok(typed)
    }

    pub(crate) fn validate_durable_envelope(
        &self,
        stored_schema_key: &str,
        stored_row_pk: &RowPk,
    ) -> Result<(), LixError> {
        let payload_row_pk = RowPk::from_schema_values(&self.row_pk).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "durable typed payload for schema '{stored_schema_key}' has an invalid row identity: {error}"
                ),
            )
        })?;
        if &payload_row_pk != stored_row_pk {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "durable typed payload identity does not match the stored envelope for schema '{stored_schema_key}'"
                ),
            ));
        }
        Ok(())
    }

    /// Decodes a durable typed row and binds its embedded identity to the
    /// authoritative storage envelope before the row can be materialized.
    pub(crate) fn decode_durable_payload(
        payload: Arc<[u8]>,
        stored_schema_key: &str,
        stored_row_pk: &RowPk,
    ) -> Result<Self, LixError> {
        let decoded_engine_payload =
            if payload.first().copied() == Some(COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION) {
                Some(decompress_engine_row_payload(&payload)?)
            } else {
                None
            };
        let engine_payload = decoded_engine_payload.as_deref().unwrap_or(&payload);
        let (schema_fingerprint, row_pk, row) = if engine_payload.first().copied()
            == Some(crate::plugin::wire::typed::ENGINE_ROW_PAYLOAD_VERSION)
        {
            let (_, plan) = crate::catalog::CatalogSnapshot::builtin()
                .plan_for_key(stored_schema_key)
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "compact engine typed payload references non-built-in schema '{stored_schema_key}'"
                        ),
                    )
                })?;
            let row = crate::plugin::wire::typed::decode_engine_row_payload(
                engine_payload,
                &plan.compiled_schema,
            )
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "cannot decode durable typed payload for schema '{stored_schema_key}': {error:?}"
                    ),
                )
            })?;
            let row_pk = plan
                .compiled_schema
                .primary_key()
                .iter()
                .map(|column| {
                    row.get(column).cloned().ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!(
                                "compact engine typed row for schema '{stored_schema_key}' is missing primary-key column '{column}'"
                            ),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            (plan.fingerprint().bytes(), row_pk, row)
        } else {
            let decoded = crate::plugin::wire::typed::decode_native_row_payload(engine_payload)
                .map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "cannot decode durable typed payload for schema '{stored_schema_key}': {error:?}"
                        ),
                    )
                })?;
            let row_pk = if decoded.row_pk.is_empty()
                && engine_payload.first().copied()
                    == Some(crate::plugin::wire::typed::STORAGE_ROW_PAYLOAD_VERSION)
            {
                stored_row_pk
                    .components
                    .iter()
                    .map(|component| match component {
                        crate::row_pk::RowPkComponent::String(value) => {
                            Ok(lix_schema::Value::Text(value.as_str().to_owned()))
                        }
                        crate::row_pk::RowPkComponent::Uuid(value) => {
                            Ok(lix_schema::Value::Uuid(uuid::Uuid::from_bytes(*value)))
                        }
                        crate::row_pk::RowPkComponent::Integer(value) => {
                            Ok(lix_schema::Value::Int8(*value))
                        }
                        crate::row_pk::RowPkComponent::Bytes(_) => Err(LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "Schema v1 storage payload has a non-schema primary-key component",
                        )),
                    })
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                decoded.row_pk
            };
            (decoded.schema_fingerprint, row_pk, decoded.row)
        };
        let row = Self {
            schema_fingerprint,
            row_pk: row_pk.into(),
            row,
            native_payload: OnceLock::from(NativePayloadCache::Durable {
                bytes: payload,
                boundary_validation: OnceLock::new(),
            }),
            boundary_create_validation: OnceLock::new(),
        };
        row.validate_durable_envelope(stored_schema_key, stored_row_pk)?;
        Ok(row)
    }

    /// Binds a decoded durable row's storage envelope to the schema selected
    /// by its consumer. This must run before typed values are filtered,
    /// projected, or otherwise exposed.
    pub(crate) fn validate_resolved_schema_binding(
        &self,
        stored_schema_key: &str,
        resolved_schema_key: &str,
        resolved_schema_fingerprint: &[u8; 32],
    ) -> Result<(), LixError> {
        if stored_schema_key != resolved_schema_key {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "durable typed row stored as schema '{stored_schema_key}' cannot be exposed as resolved schema '{resolved_schema_key}'"
                ),
            ));
        }
        if &self.schema_fingerprint != resolved_schema_fingerprint {
            return Err(LixError::new(
                LixError::CODE_SCHEMA_VALIDATION,
                format!(
                    "durable typed row fingerprint for schema '{stored_schema_key}' does not match the resolved schema"
                ),
            ));
        }
        Ok(())
    }

    pub(crate) fn invalidate_durable_payload(&mut self) {
        self.native_payload.take();
    }

    pub(crate) fn durable_payload(&self) -> Result<Arc<[u8]>, crate::plugin::wire::typed::Error> {
        if let Some(payload) = self.native_payload.get() {
            return Ok(Arc::clone(payload.bytes()));
        }
        let payload: Arc<[u8]> = crate::plugin::wire::typed::encode_native_row_payload(
            &self.schema_fingerprint,
            &self.row_pk,
            &self.row,
        )?
        .into();
        let _ = self.native_payload.set(NativePayloadCache::Durable {
            bytes: Arc::clone(&payload),
            boundary_validation: OnceLock::new(),
        });
        Ok(self
            .native_payload
            .get()
            .map_or(payload, |cached| Arc::clone(cached.bytes())))
    }

    /// Returns the canonical durable payload without cloning its cached owner.
    ///
    /// Terminal storage carriers borrow this slice from the typed row so their
    /// physical snapshot has one representation regardless of whether the
    /// cache was populated by decoding or by first-time encoding.
    pub(crate) fn durable_payload_ref(&self) -> Result<&[u8], crate::plugin::wire::typed::Error> {
        if self.native_payload.get().is_none() {
            let payload: Arc<[u8]> = crate::plugin::wire::typed::encode_native_row_payload(
                &self.schema_fingerprint,
                &self.row_pk,
                &self.row,
            )?
            .into();
            let _ = self.native_payload.set(NativePayloadCache::Durable {
                bytes: payload,
                boundary_validation: OnceLock::new(),
            });
        }
        Ok(self
            .native_payload
            .get()
            .expect("durable payload cache was initialized")
            .bytes()
            .as_ref())
    }

    /// Records the stronger proof produced by catalog-backed component
    /// ingress. Callers must first validate the complete Schema v1 row and
    /// its primary-key envelope.
    pub(crate) fn certify_boundary_validation(
        &self,
    ) -> Result<(), crate::plugin::wire::typed::Error> {
        if let Some(NativePayloadCache::Durable {
            boundary_validation,
            ..
        }) = self.native_payload.get()
        {
            let _ = boundary_validation.set(BoundaryValidationToken { _private: () });
        } else if self.native_payload.get().is_none() {
            let payload: Arc<[u8]> = crate::plugin::wire::typed::encode_native_row_payload(
                &self.schema_fingerprint,
                &self.row_pk,
                &self.row,
            )?
            .into();
            let _ = self
                .native_payload
                .set(NativePayloadCache::BoundaryValidated {
                    bytes: payload,
                    _proof: BoundaryValidationToken { _private: () },
                });
        }
        Ok(())
    }

    pub(crate) fn boundary_validation_certified(&self) -> bool {
        matches!(
            self.native_payload.get(),
            Some(NativePayloadCache::BoundaryValidated { .. })
        ) || matches!(
            self.native_payload.get(),
            Some(NativePayloadCache::Durable {
                boundary_validation,
                ..
            }) if boundary_validation.get().is_some()
        )
    }

    pub(crate) fn certify_boundary_create_validation(&self) {
        let _ = self
            .boundary_create_validation
            .set(BoundaryValidationToken { _private: () });
    }

    pub(crate) fn boundary_create_validation_certified(&self) -> bool {
        self.boundary_create_validation.get().is_some()
    }

    pub fn estimated_size(&self) -> u64 {
        let key = 4_u64.saturating_add(self.row_pk.iter().map(typed_value_size).sum::<u64>());
        let values = self
            .row
            .iter()
            .map(|(name, value)| {
                4_u64
                    .saturating_add(name.len() as u64)
                    .saturating_add(typed_value_size(value))
            })
            .sum::<u64>();
        key.saturating_add(4)
            .saturating_add(values)
            .saturating_add(64)
    }
}

pub(crate) fn compress_durable_payload(
    payload: Vec<u8>,
) -> Result<Vec<u8>, crate::plugin::wire::typed::Error> {
    if payload.len() < ENGINE_ROW_COMPRESSION_THRESHOLD
        || payload.first().copied() != Some(crate::plugin::wire::typed::ENGINE_ROW_PAYLOAD_VERSION)
        || payload.first().copied() == Some(COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION)
    {
        return Ok(payload);
    }
    compress_native_snapshot_payload(payload)
}

pub(crate) fn compress_hot_payload(
    payload: Vec<u8>,
) -> Result<Vec<u8>, crate::plugin::wire::typed::Error> {
    if payload.len() < 256
        || payload.first().is_none_or(|version| {
            !matches!(
                *version,
                crate::plugin::wire::typed::NATIVE_ROW_PAYLOAD_VERSION
                    | crate::plugin::wire::typed::ENGINE_ROW_PAYLOAD_VERSION
                    | crate::plugin::wire::typed::STORAGE_ROW_PAYLOAD_VERSION
            )
        })
    {
        return Ok(payload);
    }
    compress_native_snapshot_payload(payload)
}

fn compress_native_snapshot_payload(
    payload: Vec<u8>,
) -> Result<Vec<u8>, crate::plugin::wire::typed::Error> {
    let compressed = lz4_flex::block::compress(&payload);
    if compressed.len().saturating_add(5) >= payload.len() {
        return Ok(payload);
    }
    let mut framed = Vec::with_capacity(5 + compressed.len());
    framed.push(COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION);
    framed.extend_from_slice(
        &u32::try_from(payload.len())
            .map_err(|_| {
                crate::plugin::wire::typed::Error::Invalid(
                    "durable typed payload exceeds u32 framing",
                )
            })?
            .to_le_bytes(),
    );
    framed.extend_from_slice(&compressed);
    Ok(framed)
}

pub(crate) fn decompress_engine_row_payload(payload: &[u8]) -> Result<Arc<[u8]>, LixError> {
    let expected_len = u32::from_le_bytes(
        payload
            .get(1..5)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "compressed engine typed payload is truncated",
                )
            })?
            .try_into()
            .expect("four-byte compact payload length"),
    ) as usize;
    if expected_len > ENGINE_ROW_PAYLOAD_MAX_BYTES {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "compressed engine typed payload exceeds its decoded size limit",
        ));
    }
    let decoded = lz4_flex::block::decompress(&payload[5..], expected_len).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("cannot decompress compact engine typed payload: {error}"),
        )
    })?;
    if decoded.len() != expected_len
        || !matches!(
            decoded.first().copied(),
            Some(crate::plugin::wire::typed::ENGINE_ROW_PAYLOAD_VERSION)
                | Some(crate::plugin::wire::typed::NATIVE_ROW_PAYLOAD_VERSION)
                | Some(crate::plugin::wire::typed::STORAGE_ROW_PAYLOAD_VERSION)
        )
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "compressed engine typed payload has an invalid decoded envelope",
        ));
    }
    Ok(decoded.into())
}

fn typed_value_size(value: &lix_schema::Value) -> u64 {
    match value {
        lix_schema::Value::Null => 1,
        lix_schema::Value::Text(value) => value.len() as u64 + 5,
        lix_schema::Value::Uuid(_) => 17,
        lix_schema::Value::Int8(_) | lix_schema::Value::Float8(_) => 9,
        lix_schema::Value::Boolean(_) => 2,
        lix_schema::Value::Jsonb(value) => value.estimated_binary_size().saturating_add(5),
        lix_schema::Value::Timestamptz(_) => 9,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowCreateContext {
    pub high: u64,
    pub low: u32,
}

impl RowCreateContext {
    pub fn row_pk(self, local_ref: u64) -> Result<Vec<lix_schema::Value>, LixError> {
        Ok(vec![lix_schema::Value::Uuid(self.component(local_ref)?)])
    }

    pub fn component(self, local_ref: u64) -> Result<uuid::Uuid, LixError> {
        Ok(uuid::Uuid::from_bytes(
            self.component_uuid_bytes(local_ref)?,
        ))
    }

    pub(crate) fn component_uuid_bytes(self, local_ref: u64) -> Result<[u8; 16], LixError> {
        let local_ref = u32::try_from(local_ref).map_err(|_| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "component create local references must fit in an unsigned 32-bit integer",
            )
        })?;
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&self.high.to_be_bytes());
        bytes[8..12].copy_from_slice(&self.low.to_be_bytes());
        bytes[12..].copy_from_slice(&local_ref.to_be_bytes());
        Ok(bytes)
    }
}

/// One immutable, host-validated semantic batch which remains encoded until
/// storage/query consumption.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertifiedCreateRange {
    pub schema_key: String,
    pub first_local_ref: u32,
    pub last_local_ref: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertifiedRowBatch {
    pub format: u16,
    pub schema_keys: Vec<String>,
    pub row_count: u64,
    pub creates: RowCreateContext,
    pub create_ranges: Vec<CertifiedCreateRange>,
    pub complete_file_state: bool,
    pub pages: Vec<Bytes>,
}

pub(crate) const HOST_CERTIFIED_PACKET_FORMAT: u16 = 3;
pub(crate) const HOST_CERTIFIED_ZSTD_PACKET_FORMAT: u16 = 4;

#[cfg(test)]
mod tests {
    use super::*;
    fn path_value_plan() -> crate::catalog::SchemaPlan {
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "canonical_path_value_probe",
            "columns": [
                {"name": "path", "type": "text", "nullable": false},
                {"name": "value", "type": "jsonb", "nullable": false}
            ],
            "primary_key": ["path"]
        });
        crate::catalog::SchemaPlan::compile_standalone_for_test(
            crate::catalog::SchemaCatalogKey {
                schema_key: "canonical_path_value_probe".to_owned(),
            },
            schema,
            &std::collections::BTreeMap::new(),
            &std::collections::BTreeMap::new(),
        )
        .expect("path/value schema should compile")
    }

    #[test]
    fn canonical_parameter_route_matches_owned_route_and_rejects_noncanonical_input() {
        let plan = path_value_plan();
        assert!(plan.accepts_canonical_certificate());
        let value = serde_json::json!({"z": [2, 1], "a": "β"});
        let canonical = lix_schema::Jsonb::from(value.clone())
            .to_json_string()
            .unwrap();

        let mut expected = Vec::new();
        TypedRow::append_certified_path_value_payload(&mut expected, &plan, "/packages/β", value)
            .unwrap();
        let mut actual = Vec::new();
        assert!(
            TypedRow::try_append_certified_path_value_payload_from_canonical_json(
                &mut actual,
                &plan,
                "/packages/β",
                canonical.as_bytes(),
            )
            .unwrap()
        );
        assert_eq!(actual, expected);

        let mut retained = b"retained".to_vec();
        assert!(
            !TypedRow::try_append_certified_path_value_payload_from_canonical_json(
                &mut retained,
                &plan,
                "/packages/β",
                br#" {"z":2,"a":1}"#,
            )
            .unwrap()
        );
        assert_eq!(retained, b"retained");
    }

    #[test]
    fn durable_payload_cache_can_be_boundary_certified_after_encoding() {
        let row = TypedRow {
            schema_fingerprint: [7; 32],
            row_pk: vec![lix_schema::Value::Text("row-1".to_owned())].into(),
            row: lix_schema::Row::from([("id", lix_schema::Value::Text("row-1".to_owned()))]),
            native_payload: OnceLock::new(),
            boundary_create_validation: OnceLock::new(),
        };

        row.durable_payload().expect("encode durable payload");
        assert!(!row.boundary_validation_certified());
        row.certify_boundary_validation()
            .expect("certify encoded payload");
        assert!(row.boundary_validation_certified());
    }

    #[test]
    fn durable_payload_ref_borrows_the_cached_native_bytes() {
        let row = TypedRow {
            schema_fingerprint: [8; 32],
            row_pk: vec![lix_schema::Value::Text("row-ref".to_owned())].into(),
            row: lix_schema::Row::from([("id", lix_schema::Value::Text("row-ref".to_owned()))]),
            native_payload: OnceLock::new(),
            boundary_create_validation: OnceLock::new(),
        };

        let first = row
            .durable_payload_ref()
            .expect("borrowed durable payload should encode");
        let first_pointer = first.as_ptr();
        let second = row
            .durable_payload_ref()
            .expect("borrowed durable payload should reuse cache");
        assert_eq!(second.as_ptr(), first_pointer);
        assert_eq!(second, row.durable_payload().unwrap().as_ref());
    }

    #[test]
    fn compressed_durable_payload_round_trips_and_rejects_invalid_framing() {
        let payload = vec![
            crate::plugin::wire::typed::ENGINE_ROW_PAYLOAD_VERSION;
            ENGINE_ROW_COMPRESSION_THRESHOLD * 2
        ];
        let compressed = compress_durable_payload(payload.clone())
            .expect("repetitive engine payload should compress");
        assert_eq!(
            compressed.first().copied(),
            Some(COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION)
        );
        assert_eq!(
            decompress_engine_row_payload(&compressed)
                .expect("compressed engine payload should decode")
                .as_ref(),
            payload
        );

        let truncated = [COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION];
        assert!(decompress_engine_row_payload(&truncated).is_err());

        let mut oversized = vec![COMPRESSED_ENGINE_ROW_PAYLOAD_VERSION];
        oversized.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decompress_engine_row_payload(&oversized).is_err());

        let mut invalid_inner = vec![99; ENGINE_ROW_COMPRESSION_THRESHOLD * 2];
        invalid_inner[0] = 99;
        let invalid_inner = compress_durable_payload(invalid_inner)
            .expect("invalid inner payload should still frame");
        assert!(decompress_engine_row_payload(&invalid_inner).is_err());
    }
}
