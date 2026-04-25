use crate::backend::QueryExecutor;
use crate::canonical::{
    append_changes, append_untracked_change_visibility_rows,
    canonical_untracked_visibility_write_from_change_visibility,
    compact_untracked_changes_for_touched_rows_in_transaction, load_exact_row_at_commit,
    CanonicalChangeWrite, CanonicalJson, CanonicalStateIdentity,
};
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::live_state::{
    finalize_live_state_after_immediate_write, key_value_schema_key, key_value_schema_version,
    load_exact_untracked_row_with_executor, load_version_head_commit_id_with_executor,
    write_live_rows, ExactUntrackedRowRequest, LiveRow,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackendTransaction, LixError, NullableKeyFilter};

const DETERMINISTIC_SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";

pub(crate) fn deterministic_sequence_key() -> &'static str {
    DETERMINISTIC_SEQUENCE_KEY
}

pub(crate) async fn ensure_runtime_sequence_initialized_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    functions: &mut dyn LixFunctionProvider,
) -> Result<(), LixError> {
    if !functions.deterministic_sequence_enabled() || functions.deterministic_sequence_initialized()
    {
        return Ok(());
    }
    let untracked_row = {
        let mut executor = &mut *transaction;
        load_runtime_sequence_untracked_row_with_executor(&mut executor).await?
    };

    let highest_seen = if let Some(row) = untracked_row {
        parse_runtime_sequence_highest_seen_from_text(row.property_text("value").as_deref())?
    } else {
        let tracked_highest_seen = {
            let mut executor = &mut *transaction;
            load_runtime_sequence_tracked_highest_seen_with_executor(&mut executor).await?
        }
        .unwrap_or(-1);
        append_runtime_sequence_row_in_transaction(transaction, tracked_highest_seen).await?;
        tracked_highest_seen
    };

    functions.initialize_deterministic_sequence(highest_seen + 1);
    Ok(())
}

pub(crate) async fn persist_runtime_sequence_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    functions: &SharedFunctionProvider<Box<dyn LixFunctionProvider + Send>>,
) -> Result<(), LixError> {
    let Some(highest_seen) = functions.deterministic_sequence_persist_highest_seen() else {
        return Ok(());
    };
    persist_runtime_sequence_highest_seen_in_transaction(transaction, highest_seen).await
}

pub(crate) async fn persist_runtime_sequence_highest_seen_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    highest_seen: i64,
) -> Result<(), LixError> {
    let existing_highest_seen = {
        let mut executor = &mut *transaction;
        load_runtime_sequence_untracked_row_with_executor(&mut executor)
            .await?
            .map(|row| {
                parse_runtime_sequence_highest_seen_from_text(row.property_text("value").as_deref())
            })
            .transpose()?
    };

    if existing_highest_seen == Some(highest_seen) {
        return Ok(());
    }

    append_runtime_sequence_row_in_transaction(transaction, highest_seen).await
}

async fn load_runtime_sequence_untracked_row_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Option<crate::live_state::UntrackedRow>, LixError> {
    load_exact_untracked_row_with_executor(
        executor,
        &ExactUntrackedRowRequest {
            schema_key: key_value_schema_key().to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: deterministic_sequence_key().to_string(),
            file_id: NullableKeyFilter::Null,
            untracked: true,
        },
    )
    .await
}

async fn load_runtime_sequence_tracked_highest_seen_with_executor(
    executor: &mut dyn QueryExecutor,
) -> Result<Option<i64>, LixError> {
    let Some(commit_id) =
        load_version_head_commit_id_with_executor(executor, GLOBAL_VERSION_ID).await?
    else {
        return Ok(None);
    };
    let Some(row) = load_exact_row_at_commit(
        executor,
        &commit_id,
        &CanonicalStateIdentity {
            entity_id: deterministic_sequence_key().to_string(),
            schema_key: key_value_schema_key().to_string(),
            file_id: None,
        },
    )
    .await?
    else {
        return Ok(None);
    };

    Ok(Some(parse_runtime_sequence_highest_seen_from_snapshot(
        &row.snapshot_content,
    )?))
}

async fn append_runtime_sequence_row_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    highest_seen: i64,
) -> Result<(), LixError> {
    let snapshot_content = deterministic_sequence_snapshot_content(highest_seen);
    let change_id = deterministic_sequence_change_id(highest_seen);
    let created_at = deterministic_sequence_created_at(highest_seen);
    let change = deterministic_sequence_change_row(&change_id, &created_at, &snapshot_content)?;
    let mut persistence_functions = DeterministicSequencePersistenceFunctions::new(highest_seen);
    let live_row = LiveRow {
        entity_id: deterministic_sequence_key().to_string(),
        file_id: None,
        schema_key: key_value_schema_key().to_string(),
        schema_version: key_value_schema_version().to_string(),
        version_id: GLOBAL_VERSION_ID.to_string(),
        plugin_key: None,
        metadata: None,
        change_id: Some(change_id),
        commit_id: None,
        global: true,
        untracked: true,
        created_at: Some(created_at.clone()),
        updated_at: Some(created_at.clone()),
        snapshot_content: Some(snapshot_content),
    };
    let visibility_row = canonical_untracked_visibility_write_from_change_visibility(
        &change,
        &live_row.version_id,
        live_row.global,
        live_row.created_at.as_deref(),
    );

    append_changes(
        transaction,
        std::slice::from_ref(&change),
        &mut persistence_functions,
    )
    .await?;
    append_untracked_change_visibility_rows(transaction, std::slice::from_ref(&visibility_row))
        .await?;
    write_live_rows(transaction, &[live_row]).await?;
    finalize_live_state_after_immediate_write(transaction).await?;
    compact_untracked_changes_for_touched_rows_in_transaction(
        transaction,
        std::slice::from_ref(&visibility_row),
    )
    .await?;
    Ok(())
}

fn deterministic_sequence_snapshot_content(highest_seen: i64) -> String {
    serde_json::json!({
        "key": deterministic_sequence_key(),
        "value": highest_seen,
    })
    .to_string()
}

fn deterministic_sequence_change_id(highest_seen: i64) -> String {
    format!(
        "det-seq-change-{value:020}",
        value = deterministic_sequence_journal_ordinal(highest_seen)
    )
}

fn deterministic_sequence_created_at(highest_seen: i64) -> String {
    format!(
        "1970-01-01T00:00:00.{value:020}Z",
        value = deterministic_sequence_journal_ordinal(highest_seen)
    )
}

fn deterministic_sequence_journal_ordinal(highest_seen: i64) -> u64 {
    if highest_seen < 0 {
        0
    } else {
        highest_seen as u64 + 1
    }
}

fn deterministic_sequence_change_row(
    change_id: &str,
    created_at: &str,
    snapshot_content: &str,
) -> Result<CanonicalChangeWrite, LixError> {
    Ok(CanonicalChangeWrite {
        id: change_id.to_string(),
        entity_id: deterministic_sequence_key()
            .to_string()
            .try_into()
            .map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "invalid deterministic sequence entity_id '{}'",
                        deterministic_sequence_key()
                    ),
                )
            })?,
        schema_key: key_value_schema_key().to_string().try_into().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "invalid deterministic sequence schema_key '{}'",
                    key_value_schema_key()
                ),
            )
        })?,
        schema_version: key_value_schema_version()
            .to_string()
            .try_into()
            .map_err(|_| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "invalid deterministic sequence schema_version '{}'",
                        key_value_schema_version()
                    ),
                )
            })?,
        file_id: None,
        plugin_key: None,
        snapshot_content: Some(
            CanonicalJson::from_text(snapshot_content.to_string()).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "invalid deterministic sequence snapshot_content: {}",
                        error.description
                    ),
                )
            })?,
        ),
        metadata: None,
        created_at: created_at.to_string(),
    })
}

fn parse_runtime_sequence_highest_seen_from_text(raw_value: Option<&str>) -> Result<i64, LixError> {
    let Some(raw_value) = raw_value else {
        return Ok(-1);
    };
    raw_value.parse::<i64>().map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "deterministic sequence row contained non-integer value '{raw_value}': {error}"
            ),
        )
    })
}

fn parse_runtime_sequence_highest_seen_from_snapshot(
    snapshot_content: &str,
) -> Result<i64, LixError> {
    let parsed: serde_json::Value = serde_json::from_str(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("deterministic sequence snapshot invalid JSON: {error}"),
        )
    })?;
    Ok(parsed
        .get("value")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(-1))
}

struct DeterministicSequencePersistenceFunctions {
    counter_value: u64,
    next_snapshot_ordinal: u32,
}

impl DeterministicSequencePersistenceFunctions {
    fn new(highest_seen: i64) -> Self {
        Self {
            counter_value: deterministic_sequence_journal_ordinal(highest_seen),
            next_snapshot_ordinal: 0,
        }
    }
}

impl LixFunctionProvider for DeterministicSequencePersistenceFunctions {
    fn uuid_v7(&mut self) -> String {
        let id = format!(
            "det-seq-snapshot-{counter:020}-{ordinal:04}",
            counter = self.counter_value,
            ordinal = self.next_snapshot_ordinal
        );
        self.next_snapshot_ordinal += 1;
        id
    }

    fn timestamp(&mut self) -> String {
        deterministic_sequence_created_at(self.counter_value as i64)
    }
}
