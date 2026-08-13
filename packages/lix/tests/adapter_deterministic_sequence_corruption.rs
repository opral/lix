use std::ops::Bound;

use bytes::Bytes;
use lix::open_lix;
use lix::storage::{
    BeginScanOptions, CoreProjection, Key, KeyRange, ProjectedValue, PutBatch, PutEntry,
    ReadOptions, SpaceId, Storage, StorageRead, StorageSpace, StorageWrite, StoredValue,
    WriteOptions,
};

const HOT_ROW_SPACE: StorageSpace =
    StorageSpace::mutable(SpaceId(0x0004_001b), "hot_state.row.v21");
const SEQUENCE_IDENTITY: &[u8] = b"lix_deterministic_sequence_number";
const UNRELATED_IDENTITY: &[u8] = b"lix_unrelated_sequence_substitute";

pub async fn initialize_with_deterministic_mode<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("repository should open");
    lix.execute(
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
    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("repository should reopen");
    let result = lix
        .execute("SELECT lix_uuid_v7() AS value", &[])
        .await
        .expect("valid deterministic authority should produce the next UUID");
    let value = result.rows()[0]
        .get::<String>("value")
        .expect("UUID should be text");
    assert_eq!(value, format!("01920000-0000-7000-8000-{suffix}"));
}

pub async fn replace_selected_sequence_member_with_unrelated<S>(storage: &S)
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
    let mut sequence_entries = Vec::new();
    let mut cursor = read
        .begin_scan(
            HOT_ROW_SPACE,
            range,
            BeginScanOptions {
                projection: CoreProjection::FullValue,
                ..BeginScanOptions::default()
            },
        )
        .await
        .expect("HOT member scan should begin");
    loop {
        let (page, page_has_more) = cursor
            .next_page(lix::storage::MAX_SCAN_PAGE_ROWS)
            .await
            .expect("HOT members should scan")
            .into_parts();
        sequence_entries.extend(
            page.into_iter()
                .filter(|entry| contains_subslice(entry.key.0.as_ref(), SEQUENCE_IDENTITY)),
        );
        if !page_has_more {
            break;
        }
    }
    drop(cursor);
    drop(read);
    assert_eq!(
        sequence_entries.len(),
        1,
        "fixture should have one selected sequence HOT member"
    );
    let sequence_entry = sequence_entries.pop().expect("one sequence entry");
    let ProjectedValue::FullValue(value) = sequence_entry.value else {
        panic!("sequence corruption fixture must read the full row value");
    };
    let replacement_key = Key(Bytes::from(replace_subslice(
        sequence_entry.key.0.as_ref(),
        SEQUENCE_IDENTITY,
        UNRELATED_IDENTITY,
    )));

    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("physical corruption write should open");
    write
        .delete_many(HOT_ROW_SPACE, std::slice::from_ref(&sequence_entry.key))
        .await
        .expect("selected sequence member deletion should stage");
    write
        .put_many(
            HOT_ROW_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: replacement_key,
                    value: StoredValue { bytes: value },
                }],
            },
        )
        .await
        .expect("unrelated same-count member should stage");
    write
        .commit()
        .await
        .expect("same-count sequence substitution should commit");
}

pub async fn assert_missing_sequence_member_fails_closed<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("member-corrupt repository should open structurally");
    let error = lix
        .execute("SELECT lix_uuid_v7()", &[])
        .await
        .expect_err("missing selected sequence member must fail closed");
    assert!(
        error.message.contains("identity digest") && error.message.contains("canonical members"),
        "unexpected member-closure error: {error:?}"
    );
}

fn replace_subslice(haystack: &[u8], needle: &[u8], replacement: &[u8]) -> Vec<u8> {
    assert_eq!(needle.len(), replacement.len());
    let position = haystack
        .windows(needle.len())
        .position(|candidate| candidate == needle)
        .expect("sequence identity should be present in its physical key");
    assert!(
        haystack[position + needle.len()..]
            .windows(needle.len())
            .all(|candidate| candidate != needle),
        "sequence identity should occur exactly once in its physical key"
    );
    let mut replaced = haystack.to_vec();
    replaced[position..position + needle.len()].copy_from_slice(replacement);
    replaced
}

fn contains_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|candidate| candidate == needle)
}
