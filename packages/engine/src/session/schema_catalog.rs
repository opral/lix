use std::cmp::Ordering;
use std::collections::HashMap;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::contracts::traits::{PendingSemanticStorage, PendingView};
use crate::live_state::{RegisteredSchemaCatalog, SqlRegisteredSchemaCatalog};
use crate::schema::{schema_from_registered_snapshot, SchemaKey};
use crate::{LixBackend, LixError};

pub(crate) struct SessionSchemaCatalog<'a> {
    base: SqlRegisteredSchemaCatalog<'a>,
    pending: HashMap<SchemaKey, JsonValue>,
}

impl<'a> SessionSchemaCatalog<'a> {
    pub(crate) fn from_backend(backend: &'a dyn LixBackend) -> Self {
        Self::new(SqlRegisteredSchemaCatalog::new(backend))
    }

    pub(crate) fn new(base: SqlRegisteredSchemaCatalog<'a>) -> Self {
        Self {
            base,
            pending: HashMap::new(),
        }
    }

    pub(crate) fn remember_pending_schema(&mut self, key: SchemaKey, schema: JsonValue) {
        self.pending.insert(key, schema);
    }

    pub(crate) fn remember_pending_schema_from_snapshot(
        &mut self,
        snapshot: &JsonValue,
    ) -> Result<(), LixError> {
        let (key, schema) = schema_from_registered_snapshot(snapshot)?;
        self.pending.insert(key, schema);
        Ok(())
    }

    pub(crate) fn remember_pending_registered_schemas_from_view(
        &mut self,
        pending_view: Option<&dyn PendingView>,
    ) -> Result<(), LixError> {
        let Some(pending_view) = pending_view else {
            return Ok(());
        };

        for (_, snapshot_content) in pending_view.visible_registered_schema_entries() {
            let Some(snapshot_content) = snapshot_content else {
                continue;
            };
            let snapshot =
                serde_json::from_str::<JsonValue>(&snapshot_content).map_err(|error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("registered schema snapshot_content invalid JSON: {error}"),
                    )
                })?;
            self.remember_pending_schema_from_snapshot(&snapshot)?;
        }

        for storage in [
            PendingSemanticStorage::Tracked,
            PendingSemanticStorage::Untracked,
        ] {
            for row in pending_view.visible_semantic_rows(storage, "lix_registered_schema") {
                let Some(snapshot_content) = row.snapshot_content else {
                    continue;
                };
                let snapshot =
                    serde_json::from_str::<JsonValue>(&snapshot_content).map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!("registered schema snapshot_content invalid JSON: {error}"),
                        )
                    })?;
                self.remember_pending_schema_from_snapshot(&snapshot)?;
            }
        }

        Ok(())
    }

    fn latest_pending_schema(&self, schema_key: &str) -> Option<(SchemaKey, JsonValue)> {
        self.pending
            .iter()
            .filter(|(key, _)| key.schema_key == schema_key)
            .max_by(|(left, _), (right, _)| compare_schema_keys(left, right))
            .map(|(key, schema)| (key.clone(), schema.clone()))
    }
}

#[async_trait(?Send)]
impl RegisteredSchemaCatalog for SessionSchemaCatalog<'_> {
    async fn load_schema(&mut self, key: &SchemaKey) -> Result<JsonValue, LixError> {
        if let Some(schema) = self.pending.get(key) {
            return Ok(schema.clone());
        }

        self.base.load_schema(key).await
    }

    async fn load_latest_schema(&mut self, schema_key: &str) -> Result<JsonValue, LixError> {
        let pending_latest = self.latest_pending_schema(schema_key);
        let stored_latest = self.base.load_latest_schema_entry(schema_key).await?;

        match (pending_latest, stored_latest) {
            (Some((pending_key, pending_schema)), Some((stored_key, registered_schema))) => {
                if compare_schema_keys(&pending_key, &stored_key) != Ordering::Less {
                    Ok(pending_schema)
                } else {
                    Ok(registered_schema)
                }
            }
            (Some((_, pending_schema)), None) => Ok(pending_schema),
            (None, Some((_, registered_schema))) => Ok(registered_schema),
            (None, None) => self.base.load_latest_schema(schema_key).await,
        }
    }

    async fn load_visible_schema_entries(
        &mut self,
    ) -> Result<Vec<(SchemaKey, JsonValue)>, LixError> {
        let mut entries_by_key = self
            .base
            .load_visible_schema_entries()
            .await?
            .into_iter()
            .collect::<HashMap<_, _>>();

        for (key, schema) in &self.pending {
            entries_by_key.insert(key.clone(), schema.clone());
        }

        Ok(entries_by_key.into_iter().collect())
    }
}

fn compare_schema_keys(left: &SchemaKey, right: &SchemaKey) -> Ordering {
    match (left.version_number(), right.version_number()) {
        (Some(left_version), Some(right_version)) => left_version.cmp(&right_version),
        _ => left.schema_version.cmp(&right.schema_version),
    }
}
