use super::types::{
    ChangeId, ChangeLoadBatch, ChangeLoadRequest, ChangeScanBatch, ChangeScanRequest,
    ChangelogAppend, CommitId, CommitLoadBatch, CommitLoadRequest, CommitScanBatch,
    CommitScanRequest,
};
use crate::common::LixError;
use crate::storage_adapter::{StorageSpace, StorageSpaceId, ValueSemantics};
use async_trait::async_trait;

pub(crate) const COMMIT_NAMESPACE: &str = "changelog.commit";
pub(crate) const CHANGE_NAMESPACE: &str = "changelog.change";

pub(crate) const COMMIT_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0006_0001),
    COMMIT_NAMESPACE,
    ValueSemantics::Mutable,
);
pub(crate) const CHANGE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0006_0002),
    CHANGE_NAMESPACE,
    ValueSemantics::Mutable,
);
// The former commit-membership storage space is intentionally retired. Packed
// tracked-state commit deltas are the sole commit-membership authority.
//
// The former `changelog.commit_change_id` reverse index (space 0x0006_0004) is
// also retired. A commit's change id is now derived from its commit id
// (`CommitId::commit_change_id`), so inverting it is `ChangeId::as_commit_change`
// plus the ordinary `changelog.commit` point read the caller already performs.

// Identity keys are the raw 16 UUID bytes. UUIDv7's big-endian byte order
// matches the lexicographic order of its lowercase hyphenated text, so range
// scans and resume tokens behave identically to the former text keys at
// 20 fewer bytes per key.
pub(crate) fn commit_key(commit_id: CommitId) -> Vec<u8> {
    commit_id.as_uuid().as_bytes().to_vec()
}

pub(crate) fn change_key(change_id: ChangeId) -> Vec<u8> {
    change_id.as_uuid().as_bytes().to_vec()
}

pub(crate) fn commit_id_from_key(key: &[u8]) -> Result<CommitId, LixError> {
    uuid_from_key(key, "commit").map(CommitId::new)
}

pub(crate) fn change_id_from_key(key: &[u8]) -> Result<ChangeId, LixError> {
    uuid_from_key(key, "change").map(ChangeId::new)
}

fn uuid_from_key(key: &[u8], kind: &str) -> Result<uuid::Uuid, LixError> {
    uuid::Uuid::from_slice(key).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("changelog {kind} key is not a 16-byte uuid: {error}"),
        )
    })
}

#[async_trait]
pub(crate) trait ChangelogReader {
    async fn load_commits<'a>(
        &mut self,
        request: CommitLoadRequest<'a>,
    ) -> Result<CommitLoadBatch<'a>, LixError>;

    async fn scan_commits(
        &mut self,
        request: CommitScanRequest<'_>,
    ) -> Result<CommitScanBatch, LixError>;

    async fn load_changes<'a>(
        &mut self,
        request: ChangeLoadRequest<'a>,
    ) -> Result<ChangeLoadBatch<'a>, LixError>;

    async fn scan_changes(
        &mut self,
        request: ChangeScanRequest<'_>,
    ) -> Result<ChangeScanBatch, LixError>;
}

#[async_trait]
pub(crate) trait ChangelogWriter {
    async fn stage_append(&mut self, append: ChangelogAppend) -> Result<(), LixError>;

    /// Removes standalone current change records by id.
    ///
    /// Callers obtain these ids from current-state rows whose `commit_id` is
    /// absent. The writer prevents the same transaction from appending or
    /// retaining a deleted id in a commit; committed history is immutable and
    /// never feeds this compaction API.
    async fn stage_delete_standalone_changes(
        &mut self,
        change_ids: &[ChangeId],
    ) -> Result<(), LixError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespaces_are_stable() {
        assert_eq!(COMMIT_NAMESPACE, "changelog.commit");
        assert_eq!(CHANGE_NAMESPACE, "changelog.change");
    }

    #[test]
    fn identity_keys_use_raw_uuid_bytes() {
        let commit_id = CommitId::for_test_label("commit-1");
        let change_id = ChangeId::for_test_label("change-1");
        assert_eq!(
            commit_key(commit_id),
            commit_id.as_uuid().as_bytes().to_vec()
        );
        assert_eq!(
            change_key(change_id),
            change_id.as_uuid().as_bytes().to_vec()
        );
        assert_eq!(commit_key(commit_id).len(), 16);
    }

    #[test]
    fn commit_change_id_is_the_commit_id_at_ordinal_zero() {
        let commit_id =
            CommitId::with_change_address_space(*CommitId::for_test_label("commit-1").as_uuid());
        let change_id = commit_id.commit_change_id();
        assert_eq!(change_key(change_id), commit_key(commit_id));
        assert_eq!(change_id.as_commit_change(), Some(commit_id));
    }

    #[test]
    fn identity_key_order_matches_text_order() {
        let mut ids = (0..32)
            .map(|index| CommitId::for_test_label(&format!("commit-{index}")))
            .collect::<Vec<_>>();
        ids.sort_by_key(|id| commit_key(*id));
        let text_sorted = {
            let mut text = ids.iter().map(CommitId::to_string).collect::<Vec<_>>();
            text.sort();
            text
        };
        assert_eq!(
            ids.iter().map(CommitId::to_string).collect::<Vec<_>>(),
            text_sorted,
            "binary key order must match hyphenated text order"
        );
    }

    /// The set of places that WRITE a commit record must not grow silently.
    ///
    /// # What this defends
    ///
    /// A commit record entering storage is the moment a file name becomes part
    /// of history. Any derived index over those names -- the persisted path
    /// index scoped for `lix_history('lix_file', ...) WHERE path = ?` is the live
    /// proposal -- can only be maintained at one site if there *is* only one
    /// site. That premise was established by compiler enumeration; without a
    /// guard it decays into a comment, and the failure it would hide is the
    /// silent one: an index that misses names under-approximates, and a history
    /// query then drops rows rather than erroring.
    ///
    /// Note that head movement is deliberately NOT part of this set.
    /// `MergeOutcome::FastForward` and branch-ref creation make whole
    /// ancestries reachable without staging any descriptor row, so an index
    /// keyed to *reachability* would need hooks in several places. Keying it to
    /// commit *ingestion* instead makes those cases no-ops by construction --
    /// which is exactly why this set, and not that one, is the thing worth
    /// pinning.
    ///
    /// # How the expectation was derived, and what is production
    ///
    /// By breaking the *type* of the space constant while leaving its name
    /// resolvable, so imports still resolve and every use fails type-check.
    /// **Do not use `#[cfg(any())]` for this**: the constant then fails to
    /// resolve, compilation stops at `E0432 unresolved import`, downstream
    /// users are never type-checked, and the enumeration silently truncates --
    /// measured here at 4 sites instead of 27, which looks complete and is not.
    ///
    /// Against the non-test build (`cargo check -p lix --all-features`), the
    /// only writers are:
    ///
    /// * `changelog/context.rs` x2 -- `stage_transaction_append` and
    ///   `stage_append_records`. **This is the choke point.** Its only
    ///   production caller is `transaction/commit.rs`.
    /// * `changelog/gc.rs` `.delete(` -- reclamation, which removes commit
    ///   records and never introduces a name.
    ///
    /// Every other entry below is absent from the non-test build and is a test
    /// fixture. This scan cannot tell the two apart, which is deliberate: a new
    /// site of either kind should stop a human and make them classify it.
    ///
    /// # What this cannot see
    ///
    /// A write that reaches the space without naming the constant -- through an
    /// alias, or a space value computed at runtime. Nothing does that today
    /// (`server_protocol` writes no storage at all), but a scan is a
    /// convention where the type-break is a mechanism. Re-run the type-break
    /// when this test's answer matters.
    #[test]
    fn commit_record_write_sites_are_the_sanctioned_ones() {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut files = Vec::new();
        let mut stack = vec![src.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("src dir should read") {
                let path = entry.expect("dir entry should read").path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().is_some_and(|extension| extension == "rs") {
                    files.push(path);
                }
            }
        }
        files.sort();

        // Assembled rather than written literally so this test's own source
        // does not register as a use site of the constant it scans for.
        let needle = concat!("COMMIT", "_SPACE");
        let verbs = [".put(", ".delete(", ".delete_batch(", ".stage("];
        let mut found = std::collections::BTreeMap::<String, usize>::new();
        for file in &files {
            let text = std::fs::read_to_string(file).expect("source file should read");
            let relative = file
                .strip_prefix(&src)
                .expect("scanned file should live under src")
                .to_string_lossy()
                .replace('\\', "/");
            let mut from = 0usize;
            while let Some(offset) = text[from..].find(needle) {
                let at = from + offset;
                from = at + needle.len();
                // Walk back to the nearest call verb. A statement terminator or
                // a block end between the two means the constant is not this
                // call's argument.
                let window = &text[at.saturating_sub(160)..at];
                let Some((position, verb)) = verbs
                    .iter()
                    .filter_map(|verb| window.rfind(verb).map(|position| (position, *verb)))
                    .max_by_key(|(position, _)| *position)
                else {
                    continue;
                };
                if window[position..].contains(';') || window[position..].contains('}') {
                    continue;
                }
                *found.entry(format!("{relative} {verb}")).or_default() += 1;
            }
        }

        let expected: std::collections::BTreeMap<String, usize> = [
            // Production. The choke point every commit record enters through.
            ("changelog/context.rs .stage(", 2),
            // Production. Reclamation only: removes records, introduces no name.
            ("changelog/gc.rs .delete(", 1),
            // Test fixtures, all absent from the non-test build.
            ("changelog/gc.rs .delete_batch(", 1),
            ("commit_graph/walker.rs .delete(", 2),
            ("commit_graph/walker.rs .put(", 4),
            ("tracked_state/context.rs .put(", 1),
            ("tracked_state/storage.rs .put(", 1),
        ]
        .into_iter()
        .map(|(site, count)| (site.to_string(), count))
        .collect();

        assert_eq!(
            found, expected,
            "the set of places that write a commit record changed. If you added \
             a writer, the persisted-path-index design's single-choke-point \
             premise needs re-deriving -- see this test's doc comment -- and any \
             index must be maintained at the new site too, or refuse coverage. \
             If you only moved code, update the expectation."
        );
    }
}
