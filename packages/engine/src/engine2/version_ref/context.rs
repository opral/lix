use std::sync::Arc;

use serde_json::json;

use crate::backend::{KvStore, KvWriter};
use crate::engine2::live_state::{
    LiveStateContext, LiveStateRow, LiveStateRowRequest, LiveStateStoreReader, LiveStateWriter,
};
use crate::engine2::version_ref::VersionHead;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixError, NullableKeyFilter};

const VERSION_REF_SCHEMA_KEY: &str = "lix_version_ref";
const VERSION_REF_SCHEMA_VERSION: &str = "1";

/// Typed access to moving version heads stored in live_state.
///
/// Version refs are domain state layered over live_state, not their own
/// storage engine. This context hides the `lix_version_ref` row shape from
/// transaction, rebuild, and tests.
pub(crate) struct VersionRefContext {
    live_state: Arc<LiveStateContext>,
}

impl VersionRefContext {
    pub(crate) fn new(live_state: Arc<LiveStateContext>) -> Self {
        Self { live_state }
    }

    /// Creates a version-ref reader over a caller-provided KV store.
    pub(crate) fn reader<S>(&self, store: S) -> VersionRefReader<S>
    where
        S: KvStore,
    {
        VersionRefReader {
            live_state_reader: self.live_state.reader(store),
        }
    }

    /// Creates a version-ref writer over a caller-provided KV writer.
    pub(crate) fn writer<S>(&self, store: S) -> VersionRefWriter<S>
    where
        S: KvWriter,
    {
        VersionRefWriter {
            live_state_writer: self.live_state.writer(store),
        }
    }
}

/// Read side for version heads.
pub(crate) struct VersionRefReader<S>
where
    S: KvStore,
{
    live_state_reader: LiveStateStoreReader<S>,
}

impl<S> VersionRefReader<S>
where
    S: KvStore,
{
    pub(crate) async fn load_head(
        &self,
        version_id: &str,
    ) -> Result<Option<VersionHead>, LixError> {
        let Some(row) = self
            .live_state_reader
            .load_row(&LiveStateRowRequest {
                schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: version_id.to_string(),
                file_id: NullableKeyFilter::Null,
            })
            .await?
        else {
            return Ok(None);
        };

        decode_version_head(version_id, &row)
    }

    pub(crate) async fn load_head_commit_id(
        &self,
        version_id: &str,
    ) -> Result<Option<String>, LixError> {
        Ok(self.load_head(version_id).await?.map(|head| head.commit_id))
    }
}

/// Write side for moving version heads.
pub(crate) struct VersionRefWriter<S>
where
    S: KvWriter,
{
    live_state_writer: LiveStateWriter<S>,
}

impl<S> VersionRefWriter<S>
where
    S: KvWriter,
{
    /// Advances a version ref to `commit_id`.
    ///
    /// The row is untracked by design: refs are mutable local pointers over the
    /// changelog, not changelog facts themselves.
    pub(crate) async fn advance_head(
        &mut self,
        version_id: &str,
        commit_id: &str,
        timestamp: &str,
    ) -> Result<(), LixError> {
        let row = version_ref_row(version_id, commit_id, timestamp)?;
        self.live_state_writer.write_rows(&[row]).await
    }
}

fn decode_version_head(
    requested_version_id: &str,
    row: &LiveStateRow,
) -> Result<Option<VersionHead>, LixError> {
    let Some(snapshot_content) = row.snapshot_content.as_deref() else {
        return Ok(None);
    };
    let snapshot =
        serde_json::from_str::<serde_json::Value>(snapshot_content).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("engine2 version-ref snapshot parse failed: {error}"),
            )
        })?;
    let commit_id = snapshot
        .get("commit_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("version ref for version '{requested_version_id}' is missing commit_id"),
            )
        })?;
    Ok(Some(VersionHead {
        version_id: requested_version_id.to_string(),
        commit_id: commit_id.to_string(),
    }))
}

fn version_ref_row(
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> Result<LiveStateRow, LixError> {
    let snapshot_content = serde_json::to_string(&json!({
        "id": version_id,
        "commit_id": commit_id,
    }))
    .map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("engine2 version-ref snapshot serialization failed: {error}"),
        )
    })?;

    Ok(LiveStateRow {
        entity_id: version_id.to_string(),
        schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
        file_id: None,
        plugin_key: None,
        snapshot_content: Some(snapshot_content),
        metadata: None,
        schema_version: VERSION_REF_SCHEMA_VERSION.to_string(),
        created_at: timestamp.to_string(),
        updated_at: timestamp.to_string(),
        global: true,
        change_id: None,
        commit_id: None,
        untracked: true,
        version_id: GLOBAL_VERSION_ID.to_string(),
    })
}
