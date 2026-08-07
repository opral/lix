use std::ops::Bound;

use bytes::Bytes;
use lix::integration::Engine;
use lix::storage::{
    CoreProjection, KeyRange, ReadOptions, ScanOptions, SpaceId, Storage, StorageRead,
    StorageSpace, StorageWrite, WriteOptions,
};

const HOT_ROW_SPACE: StorageSpace =
    StorageSpace::mutable(SpaceId(0x0004_001b), "live_state.hot_row.v21");
const SEQUENCE_IDENTITY: &[u8] = b"lix_deterministic_sequence_number";

pub async fn initialize_with_deterministic_mode<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");
    session
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
             VALUES ('lix_deterministic_mode', lix_json('{\"enabled\":true}'), true, true)",
            &[],
        )
        .await
        .expect("deterministic mode should enable without publishing a sequence row");
}

pub async fn assert_next_uuid<S>(storage: S, suffix: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let engine = Engine::new(storage).await.expect("engine should reopen");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should reopen");
    let result = session
        .execute("SELECT lix_uuid_v7() AS value", &[])
        .await
        .expect("valid deterministic authority should produce the next UUID");
    let value = result.rows()[0]
        .get::<String>("value")
        .expect("UUID should be text");
    assert_eq!(value, format!("01920000-0000-7000-8000-{suffix}"));
}

pub async fn delete_selected_sequence_member<S>(storage: &S)
where
    S: Storage + Send + Sync,
{
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("selected HOT generation should read");
    let range = KeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    };
    let mut resume_after = None;
    let mut sequence_keys = Vec::new();
    loop {
        let page = read
            .scan(
                HOT_ROW_SPACE,
                range.clone(),
                ScanOptions {
                    projection: CoreProjection::KeyOnly,
                    resume_after: resume_after.clone(),
                    ..ScanOptions::default()
                },
            )
            .await
            .expect("HOT members should scan");
        resume_after = page.entries.last().map(|entry| entry.key.clone());
        sequence_keys.extend(
            page.entries
                .into_iter()
                .filter(|entry| contains_subslice(entry.key.0.as_ref(), SEQUENCE_IDENTITY))
                .map(|entry| entry.key),
        );
        if !page.has_more || resume_after.is_none() {
            break;
        }
    }
    drop(read);
    assert_eq!(
        sequence_keys.len(),
        1,
        "fixture should have one selected sequence HOT member"
    );

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("physical corruption write should open");
    write
        .delete_many(HOT_ROW_SPACE, &sequence_keys)
        .await
        .expect("selected sequence member deletion should stage");
    write
        .commit()
        .await
        .expect("selected sequence member deletion should commit");
}

pub async fn assert_missing_sequence_member_fails_closed<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let engine = Engine::new(storage)
        .await
        .expect("member-corrupt repository should open structurally");
    let session = engine
        .open_workspace_session()
        .await
        .expect("member-corrupt session should open");
    let error = session
        .execute("SELECT lix_uuid_v7()", &[])
        .await
        .expect_err("missing selected sequence member must fail closed");
    assert!(
        error.message.contains("declares") && error.message.contains("but materializes"),
        "unexpected member-closure error: {error:?}"
    );
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|candidate| candidate == needle)
}
