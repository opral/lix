//! The `json_store` publication/reclamation fence.
//!
//! This is a deliberate mirror of `binary_cas`'s fence pair, for the same
//! reason and with the same asymmetry: payload rows are **content addressed**,
//! so a publisher can resolve a write it believes is new onto a row some other
//! writer already produced — and therefore onto a row a sweep planned from an
//! older snapshot has already decided is dead. Nothing about the publisher's
//! own write set reveals that.
//!
//! Why the branch-head-control preconditions the sweep already holds are not
//! enough on their own: they prove that no *observed* branch changed, and a
//! branch created after the sweep's plan was never observed. Such a publisher
//! can stage a payload whose content address collides with a hash this sweep is
//! about to delete, commit first, and lose the row underneath itself. That is
//! precisely the hazard `stage_cas_publication_fence` documents, and it is why
//! `stage_reclaim_retired_change_locators`'s argument for needing no fence does
//! **not** transfer here: a locator belongs to exactly one change and cannot be
//! reused by another writer, while a payload row is shared by construction.
//!
//! The asymmetry is inherited unchanged:
//!
//! * A publisher must not commit if a sweep reclaimed a row it planned against,
//!   so it asserts the reclamation token is unchanged. It does **not** hold a
//!   compare-and-set on the publication token it writes — two publishers
//!   planned from one snapshot are independent and must both be able to commit,
//!   which is exactly the property that keeps unrelated concurrent writers off
//!   `LIX_TRANSACTION_CONFLICT`.
//! * A sweep must not commit if a publication rooted new payload bytes after
//!   its reachability plan, so it asserts the publication token is unchanged,
//!   and rotates the reclamation token under a compare-and-set so two sweeps
//!   planned from one snapshot cannot both commit.

use bytes::Bytes;

use crate::LixError;
use crate::storage_adapter::{
    REVISION_KEY_JSON_STORE_PUBLICATION, REVISION_KEY_JSON_STORE_RECLAMATION, REVISION_SPACE,
    StorageAdapterRead, StoragePrecondition, StorageValue, StorageWriteSet, load_revision,
    load_revisions, revision_key,
};

fn fresh_revision_token() -> StorageValue {
    StorageValue {
        bytes: Bytes::copy_from_slice(uuid::Uuid::now_v7().as_bytes()),
    }
}

fn unchanged_revision_precondition(
    key: &'static [u8],
    token: Option<Bytes>,
) -> StoragePrecondition {
    let key = revision_key(key);
    match token {
        Some(expected) => StoragePrecondition::KeyValueEquals {
            space: REVISION_SPACE,
            key,
            expected,
        },
        None => StoragePrecondition::KeyAbsent {
            space: REVISION_SPACE,
            key,
        },
    }
}

/// Stages the publisher half. Called by every write set that stages
/// out-of-band JSON payload rows.
///
/// A repository that has never published a payload has no token row at all, so
/// the precondition degrades to `KeyAbsent` and the first publisher and first
/// sweep both work against a store written before this fence existed. That is
/// what makes the pair additive rather than a format change.
pub(crate) async fn stage_json_publication_fence(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
) -> Result<(), LixError> {
    let reclamation = load_revision(store, REVISION_KEY_JSON_STORE_RECLAMATION).await?;
    writes.put(
        REVISION_SPACE,
        revision_key(REVISION_KEY_JSON_STORE_PUBLICATION),
        fresh_revision_token(),
    );
    preconditions.push(unchanged_revision_precondition(
        REVISION_KEY_JSON_STORE_RECLAMATION,
        reclamation,
    ));
    Ok(())
}

/// Stages the sweep half. Called only when a sweep actually proposes a payload
/// delete, so a no-op maintenance pass costs no token rotation and cannot
/// conflict with an unrelated writer.
pub(crate) async fn stage_json_reclamation_fence(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
) -> Result<(), LixError> {
    let [publication, reclamation] = load_revisions(
        store,
        [
            REVISION_KEY_JSON_STORE_PUBLICATION,
            REVISION_KEY_JSON_STORE_RECLAMATION,
        ],
    )
    .await?;
    writes.put(
        REVISION_SPACE,
        revision_key(REVISION_KEY_JSON_STORE_RECLAMATION),
        fresh_revision_token(),
    );
    preconditions.push(unchanged_revision_precondition(
        REVISION_KEY_JSON_STORE_PUBLICATION,
        publication,
    ));
    preconditions.push(unchanged_revision_precondition(
        REVISION_KEY_JSON_STORE_RECLAMATION,
        reclamation,
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    async fn stage_publication_only(
        storage: &StorageAdapter<Memory>,
    ) -> (StorageWriteSet, Vec<StoragePrecondition>) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("publication fence read should open");
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        stage_json_publication_fence(&read, &mut writes, &mut preconditions)
            .await
            .expect("publication fence should stage");
        (writes, preconditions)
    }

    async fn stage_reclamation_only(
        storage: &StorageAdapter<Memory>,
    ) -> (StorageWriteSet, Vec<StoragePrecondition>) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("reclamation fence read should open");
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        stage_json_reclamation_fence(&read, &mut writes, &mut preconditions)
            .await
            .expect("reclamation fence should stage");
        (writes, preconditions)
    }

    fn with_preconditions(preconditions: Vec<StoragePrecondition>) -> StorageWriteOptions {
        let mut options = StorageWriteOptions::default();
        options.preconditions.extend(preconditions);
        options
    }

    #[tokio::test]
    async fn concurrent_publication_fences_planned_from_one_snapshot_both_commit() {
        let storage = StorageAdapter::new(Memory::new());
        let (first_writes, first_preconditions) = stage_publication_only(&storage).await;
        let (second_writes, second_preconditions) = stage_publication_only(&storage).await;

        storage
            .commit_write_set(first_writes, with_preconditions(first_preconditions))
            .await
            .expect("first publication fence should commit");
        storage
            .commit_write_set(second_writes, with_preconditions(second_preconditions))
            .await
            .expect("independent publishers must not conflict with each other");
    }

    #[tokio::test]
    async fn a_publication_after_a_sweeps_plan_voids_that_sweep() {
        let storage = StorageAdapter::new(Memory::new());
        let (sweep_writes, sweep_preconditions) = stage_reclamation_only(&storage).await;
        let (publish_writes, publish_preconditions) = stage_publication_only(&storage).await;

        storage
            .commit_write_set(publish_writes, with_preconditions(publish_preconditions))
            .await
            .expect("the publisher planned first and must commit");
        storage
            .commit_write_set(sweep_writes, with_preconditions(sweep_preconditions))
            .await
            .expect_err("a sweep whose plan predates a publication must not commit");
    }

    #[tokio::test]
    async fn a_sweep_after_a_publishers_plan_voids_that_publisher() {
        let storage = StorageAdapter::new(Memory::new());
        let (publish_writes, publish_preconditions) = stage_publication_only(&storage).await;
        let (sweep_writes, sweep_preconditions) = stage_reclamation_only(&storage).await;

        storage
            .commit_write_set(sweep_writes, with_preconditions(sweep_preconditions))
            .await
            .expect("the sweep planned against the same snapshot and must commit");
        storage
            .commit_write_set(publish_writes, with_preconditions(publish_preconditions))
            .await
            .expect_err("a publisher whose plan predates a reclamation must not commit");
    }

    #[tokio::test]
    async fn concurrent_reclamation_fences_planned_from_one_snapshot_conflict() {
        let storage = StorageAdapter::new(Memory::new());
        let (first_writes, first_preconditions) = stage_reclamation_only(&storage).await;
        let (second_writes, second_preconditions) = stage_reclamation_only(&storage).await;

        storage
            .commit_write_set(first_writes, with_preconditions(first_preconditions))
            .await
            .expect("first sweep should win the reclamation fence");
        storage
            .commit_write_set(second_writes, with_preconditions(second_preconditions))
            .await
            .expect_err("a stale sweep must lose the reclamation fence");
    }
}
