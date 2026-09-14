use super::*;

fn indexed_file(count: usize) -> (sdk::testing::Snapshot, ArenaRowIndex) {
    let bytes = b"a,b\n".repeat(count);
    let namespace = [0x31; 12];
    let import = ColdInitialImport::open(bytes.clone(), None).unwrap();
    let state = import.arena_state(namespace);
    let index = ArenaRowIndex::decode(&state).unwrap();
    let mut file = sdk::testing::Snapshot {
        file_id: "indexed".into(),
        path: "data.csv".into(),
        bytes,
        ..Default::default()
    };
    file.state
        .insert(ID_NAMESPACE_STATE.to_vec(), namespace.to_vec());
    let (header, pages) = split_csv_index(&state).unwrap();
    file.state.insert(CSV_INDEX_KEY.to_vec(), header.to_vec());
    for (page, bytes) in pages.into_iter().enumerate() {
        file.state
            .insert(csv_index_page_key(page as u32), bytes.to_vec());
    }
    (file, index)
}

fn edit(index: &ArenaRowIndex, ordinal: u32, cells: &[&str]) -> sdk::TypedRowChange {
    let mut change = index.row_change(ordinal, b"a,b\n".to_vec()).unwrap();
    change.row.as_mut().unwrap().insert(
        "cells",
        sdk::TypedValue::Jsonb(serde_json::json!(cells).into()),
    );
    sdk::TypedRowChange {
        schema_key: change.schema_key,
        schema_fingerprint: typed_schema(ROW_SCHEMA_KEY).unwrap().2,
        primary_key: change.row_pk,
        row: change.row,
        local_ref: None,
        effect: sdk::ChangeEffect::Content,
    }
}

#[test]
fn repeated_million_row_cell_edits_preserve_index_and_bounded_splices() {
    let (mut file, index) = indexed_file(1_000_000);
    let original_state = file.state.clone();
    let mut harness = sdk::testing::Harness::<CsvPlugin>::default();
    harness.max_batch_bytes = 2 * 1024 * 1024;
    for replacement in ["x", "y", "z"] {
        let result = harness
            .serialize_changes(
                &file,
                &[
                    edit(&index, 500_000, &[replacement, "b"]),
                    edit(&index, 900_000, &[replacement, "b"]),
                ],
            )
            .unwrap();
        assert_eq!(result.file_edits.len(), 2);
        assert_eq!(
            result
                .file_edits
                .iter()
                .map(|edit| edit.delete_len)
                .sum::<u64>(),
            8
        );
        assert_eq!(result.snapshot().state, original_state);
        file = result.into_snapshot();
    }
    assert_eq!(&file.bytes[2_000_000..2_000_004], b"z,b\n");
    assert_eq!(&file.bytes[3_600_000..3_600_004], b"z,b\n");
    // A subsequent byte edit must still use the dense parse path and preserve IDs.
    let result = harness
        .parse_changes(
            &file,
            &file.path,
            &[sdk::FileEdit {
                offset: 2_000_000,
                delete_len: 1,
                insert: b"q".to_vec(),
            }],
            None,
            sdk::CreateContext::from_namespace_bytes([0x32; 12]),
        )
        .unwrap();
    assert_eq!(result.row_changes.len(), 1);
    assert_eq!(
        result.row_changes[0].primary_key,
        edit(&index, 500_000, &["q", "b"]).primary_key
    );
}

#[test]
fn variable_length_batches_update_offsets_across_pages_and_reopen() {
    let (file, index) = indexed_file(300_000);
    let mut harness = sdk::testing::Harness::<CsvPlugin>::default();
    harness.max_batch_bytes = 2 * 1024 * 1024;
    let result = harness
        .serialize_changes(
            &file,
            &[
                edit(&index, 1, &["longer", "b"]),
                edit(&index, 262_145, &["", "b"]),
            ],
        )
        .unwrap();
    assert_eq!(result.file_edits.len(), 2);
    let file = result.into_snapshot();
    assert!(!file.state.contains_key(CSV_IDENTITIES_KEY));
    let mut state = file.state[CSV_INDEX_KEY].clone();
    for page in 0..2 {
        state.extend_from_slice(&file.state[&csv_index_page_key(page)]);
    }
    let next_index = ArenaRowIndex::decode(&state).unwrap();
    assert_eq!(next_index.file_len(), file.bytes.len() as u64);
    for ordinal in [0u32, 1, 2, 262_144, 262_145, 262_146, 299_999] {
        let expected = u64::from(ordinal) * 4 + if ordinal > 1 { 5 } else { 0 }
            - if ordinal > 262_145 { 1 } else { 0 };
        assert_eq!(
            next_index.row_range_for_edit(expected, 0).unwrap().0,
            ordinal
        );
    }
    let result = harness
        .serialize_changes(&file, &[edit(&next_index, 299_999, &["tail", "b"])])
        .unwrap();
    assert!(result.snapshot().bytes.ends_with(b"tail,b\n"));
    assert_eq!(result.file_edits.len(), 1);
}

#[test]
fn structural_checkpoint_streams_across_pages_and_preserves_order_overrides() {
    let (file, index) = indexed_file(300_000);
    let mut harness = sdk::testing::Harness::<CsvPlugin>::default();
    harness.max_batch_bytes = 2 * 1024 * 1024;
    let mut deletion = edit(&index, 0, &["a", "b"]);
    deletion.row = None;
    let file = harness
        .serialize_changes(&file, &[deletion])
        .unwrap()
        .into_snapshot();
    assert!(file.state.contains_key(CSV_IDENTITIES_KEY));
    assert!(!file.state.contains_key(CSV_INDEX_KEY));
    // The original 16-digit order keys no longer match the new row-count ranks.
    // Reopening must preserve them across checkpoint page/record boundaries.
    let result = harness
        .serialize_changes(&file, &[edit(&index, 262_144, &["changed", "b"])])
        .unwrap();
    let offset = (262_144 - 1) * 4;
    assert_eq!(
        &result.snapshot().bytes[offset..offset + 10],
        b"changed,b\n"
    );
    assert_eq!(result.file_edits.len(), 1);
    let next = harness
        .serialize_changes(result.snapshot(), &[edit(&index, 299_999, &["tail", "b"])])
        .unwrap();
    assert!(next.snapshot().bytes.ends_with(b"tail,b\n"));
}
