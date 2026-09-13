use super::*;
use sdk::testing::{Harness, Snapshot};
use std::time::{Duration, Instant};

thread_local! {
    static STRUCTURAL_WORK: std::cell::Cell<[usize; 3]> = const { std::cell::Cell::new([0; 3]) };
    static READS: std::cell::Cell<[usize; 3]> = const { std::cell::Cell::new([0; 3]) };
}

pub(super) fn record_read(kind: usize) {
    READS.with(|reads| {
        let mut value = reads.get();
        value[kind] += 1;
        reads.set(value);
    });
}

pub(super) fn record_structural_work(kind: usize, count: usize) {
    STRUCTURAL_WORK.with(|work| {
        let mut value = work.get();
        value[kind] += count;
        work.set(value);
    });
}

fn fixture(count: usize) -> (Snapshot, Vec<sdk::TypedRowChange>) {
    let bytes = format!(
        "{{{}}}",
        (0..count)
            .map(|i| format!("\"k{i:06}\":0"))
            .collect::<Vec<_>>()
            .join(",")
    )
    .into_bytes();
    let input = Snapshot {
        file_id: "perf".into(),
        path: "perf.json".into(),
        bytes,
        ..Snapshot::default()
    };
    let parsed = Harness::<JsonPlugin>::default()
        .parse(&input, sdk::CreateContext::from_namespace_bytes([0x51; 12]))
        .unwrap();
    let rows = parsed
        .row_changes
        .iter()
        .filter(|change| change.schema_key.as_ref() == OBJECT_MEMBER_SCHEMA_KEY)
        .map(|change| {
            let mut change = change.clone();
            change.row.as_mut().unwrap().insert(
                "scalar_json",
                sdk::TypedValue::Jsonb(serde_json::json!(123).into()),
            );
            change
        })
        .collect();
    (parsed.into_snapshot(), rows)
}

#[test]
fn identity_index_finds_scattered_rows_and_retains_shifted_offsets() {
    let (mut file, rows) = fixture(257);
    let harness = Harness::<JsonPlugin>::default();
    let selected = [rows[256].clone(), rows[0].clone(), rows[128].clone()];
    let changed = harness.serialize_changes(&file, &selected).unwrap();
    assert_eq!(changed.file_edits.len(), 3);
    file = changed.into_snapshot();
    let mut second = rows[128].clone();
    second.row.as_mut().unwrap().insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(7).into()),
    );
    file = harness
        .serialize_changes(&file, &[second])
        .unwrap()
        .into_snapshot();
    let value: serde_json::Value = serde_json::from_slice(&file.bytes).unwrap();
    assert_eq!(value["k000000"], 123);
    assert_eq!(value["k000128"], 7);
    assert_eq!(value["k000256"], 123);
    assert_eq!(value["k000127"], 0);
}

#[test]
fn sparse_sql_lookup_uses_logarithmic_reads_and_no_full_file_read() {
    let (file, changes) = fixture(8192);
    READS.with(|reads| reads.set([0; 3]));
    Harness::<JsonPlugin>::default()
        .serialize_changes(&file, &changes[8191..])
        .unwrap();
    let [full_file, identity_entries, scalar_entries] = READS.with(|reads| reads.get());
    assert_eq!(full_file, 0);
    assert!(identity_entries <= 15, "{identity_entries} identity reads");
    assert_eq!(scalar_entries, 1);
    READS.with(|reads| reads.set([0; 3]));
    Harness::<JsonPlugin>::default()
        .serialize_changes(&file, &[])
        .unwrap();
    assert_eq!(READS.with(|reads| reads.get()), [0; 3]);
}

#[test]
fn identity_lookup_crosses_pages_and_rejects_corrupt_state() {
    let (file, changes) = fixture(30_000);
    let harness = Harness::<JsonPlugin>::default();
    let identity_keys = file
        .state
        .keys()
        .filter(|key| key.starts_with(b"json/scalar-identity-page/"))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(identity_keys.len(), 2);
    // Select the first and final records by index order, guaranteeing both pages
    // are queried independently of the distribution of the identity hashes.
    let first = file.state.get(&identity_keys[0]).unwrap();
    let last = file.state.get(&identity_keys[1]).unwrap();
    let first_ordinal = u32::from_le_bytes(first[32..36].try_into().unwrap()) as usize;
    let last_ordinal = u32::from_le_bytes(last[last.len() - 4..].try_into().unwrap()) as usize;
    let batch = [
        changes[first_ordinal].clone(),
        changes[last_ordinal].clone(),
    ];
    let updated = harness.serialize_changes(&file, &batch).unwrap();
    assert_eq!(updated.file_edits.len(), 2);
    let actual: serde_json::Value = serde_json::from_slice(&updated.snapshot().bytes).unwrap();
    for ordinal in [first_ordinal, last_ordinal] {
        assert_eq!(actual[format!("k{ordinal:06}")], 123);
    }
    for corruption in 0..3 {
        let mut corrupt = file.clone();
        for key in &identity_keys {
            match corruption {
                0 => {
                    corrupt.state.remove(key);
                }
                1 => {
                    corrupt.state.insert(key.clone(), vec![0]);
                }
                _ => {
                    for entry in corrupt.state.get_mut(key).unwrap().chunks_exact_mut(36) {
                        entry[32..].copy_from_slice(&u32::MAX.to_le_bytes());
                    }
                }
            }
        }
        assert!(
            harness.serialize_changes(&corrupt, &batch).is_err(),
            "corruption {corruption}"
        );
    }
}

#[test]
fn duplicate_batch_with_later_noop_uses_last_row_value() {
    let (file, changes) = fixture(3);
    let mut noop = changes[2].clone();
    noop.row.as_mut().unwrap().insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(0).into()),
    );
    let updated = Harness::<JsonPlugin>::default()
        .serialize_changes(&file, &[changes[2].clone(), noop])
        .unwrap();
    assert_eq!(updated.snapshot().bytes, file.bytes);
}

#[test]
fn duplicate_object_occurrences_keep_independent_sparse_identities() {
    let input = Snapshot {
        file_id: "duplicates".into(),
        path: "duplicates.json".into(),
        bytes: br#"{"a":0,"a":1,"b":2,"a":3}"#.to_vec(),
        ..Snapshot::default()
    };
    let harness = Harness::<JsonPlugin>::default();
    let parsed = harness
        .parse(&input, sdk::CreateContext::from_namespace_bytes([0x41; 12]))
        .unwrap();
    let mut changes = parsed
        .row_changes
        .iter()
        .filter(|change| change.primary_key.get(1) == Some(&sdk::TypedValue::Text("a".into())))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(changes.len(), 3);
    for (index, change) in changes.iter_mut().enumerate() {
        assert_eq!(
            change.primary_key.get(2),
            Some(&sdk::TypedValue::Int8(index as i64))
        );
        change.row.as_mut().unwrap().insert(
            "scalar_json",
            sdk::TypedValue::Jsonb(serde_json::json!(100 + index).into()),
        );
    }
    let file = parsed.into_snapshot();
    let changed = harness.serialize_changes(&file, &changes).unwrap();
    assert_eq!(changed.file_edits.len(), 3);
    assert_eq!(
        changed.snapshot().bytes,
        br#"{"a":100,"a":101,"b":2,"a":102}"#
    );
    let mut second = changes[1].clone();
    second.row.as_mut().unwrap().insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(7).into()),
    );
    let changed = harness
        .serialize_changes(changed.snapshot(), &[second])
        .unwrap();
    assert_eq!(
        changed.snapshot().bytes,
        br#"{"a":100,"a":7,"b":2,"a":102}"#
    );
}

#[test]
fn shift_prefix_handles_negative_deltas_and_boundaries() {
    let shifts = [(2, 10), (5, -7), (8, 3)];
    let prefix = scalar_shift_prefix(&shifts).unwrap();
    for (ordinal, expected) in [(0, 100), (2, 100), (3, 110), (5, 110), (6, 103), (9, 106)] {
        assert_eq!(
            effective_scalar_start(100, ordinal, &prefix).unwrap(),
            expected
        );
    }
    assert_eq!(effective_scalar_length(20, 5, &shifts).unwrap(), 13);
    assert_eq!(effective_scalar_length(20, 4, &shifts).unwrap(), 20);
    assert!(scalar_shift_prefix(&[(0, i64::MAX), (1, 1)]).is_err());
}

#[test]
fn deep_structural_replacement_visits_each_matched_node_once() {
    let depth = 512;
    let before = format!("{}0{}", "[".repeat(depth), "]".repeat(depth)).into_bytes();
    let after = format!("{}1{}", "[".repeat(depth), "]".repeat(depth)).into_bytes();
    let (document, _) =
        Document::open_file(before.clone(), None, IdNamespace::from_halves(1, 2)).unwrap();
    STRUCTURAL_WORK.with(|work| work.set([0; 3]));
    let (updated, _) = document
        .file_changed(
            &[FileEdit {
                offset: 0,
                delete_len: before.len() as u64,
                insert: &after,
            }],
            IdNamespace::from_halves(3, 4),
        )
        .unwrap();
    assert_eq!(updated.bytes(), after);
    let [matched, rebased, hashed_bytes] = STRUCTURAL_WORK.with(|work| work.get());
    assert_eq!(hashed_bytes, before.len() + after.len());
    assert_eq!(matched, depth + 1);
    assert_eq!(
        rebased, 0,
        "matched subtrees must not be repeatedly rebased"
    );
}

#[test]
#[ignore = "manual scaling profile; run with --ignored --nocapture --test-threads=1"]
fn profile_json_adapter_scaling() {
    let harness = Harness::<JsonPlugin>::default();
    println!("lookup,rows,changed,parse_us,serialize_us,state_bytes,identity_reads,scalar_reads");
    for count in [128, 1024, 8192] {
        let started = Instant::now();
        let (file, changes) = fixture(count);
        let parse = started.elapsed();
        let state_bytes: usize = file.state.values().map(Vec::len).sum();
        for changed in [1, 16, 128] {
            let batch = (0..changed)
                .map(|i| changes[(i + 1) * count / changed - 1].clone())
                .collect::<Vec<_>>();
            let mut best = Duration::MAX;
            for _ in 0..5 {
                READS.with(|reads| reads.set([0; 3]));
                let started = Instant::now();
                let transition = harness.serialize_changes(&file, &batch).unwrap();
                best = best.min(started.elapsed());
                assert_eq!(transition.file_edits.len(), changed);
                std::hint::black_box(transition);
            }
            println!(
                "{},{count},{changed},{},{},{state_bytes},{},{}",
                "indexed",
                parse.as_micros(),
                best.as_micros(),
                READS.with(|reads| reads.get()[1]),
                READS.with(|reads| reads.get()[2])
            );
        }
    }
}

#[test]
#[ignore = "manual structural scaling profile; run with --ignored --nocapture --test-threads=1"]
fn profile_json_structural_scaling() {
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            println!("depth,bytes,replace_us,matched_nodes,rebased_nodes,hashed_bytes");
            for depth in [32, 128, 512] {
                let source = format!("{}0{}", "[".repeat(depth), ",0]".repeat(depth)).into_bytes();
                let after = format!("{}1{}", "[".repeat(depth), ",0]".repeat(depth)).into_bytes();
                let (document, _) =
                    Document::open_file(source.clone(), None, IdNamespace::from_halves(1, 2))
                        .unwrap();
                let mut best = Duration::MAX;
                for _ in 0..5 {
                    STRUCTURAL_WORK.with(|work| work.set([0; 3]));
                    let started = Instant::now();
                    let (updated, changes) = document
                        .file_changed(
                            &[FileEdit {
                                offset: 0,
                                delete_len: source.len() as u64,
                                insert: &after,
                            }],
                            IdNamespace::from_halves(3, 4),
                        )
                        .unwrap();
                    best = best.min(started.elapsed());
                    assert_eq!(updated.bytes(), after);
                    std::hint::black_box(changes);
                }
                let [matched, rebased, hashed] = STRUCTURAL_WORK.with(|work| work.get());
                println!(
                    "{depth},{},{},{matched},{rebased},{hashed}",
                    source.len(),
                    best.as_micros()
                );
            }
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn shift_overlay_pages_support_large_batches_and_cleanup() {
    const COUNT: usize = 90_000;
    let (file, changes) = fixture(COUNT);
    let harness = Harness::<JsonPlugin>::default();
    let changed = harness.serialize_changes(&file, &changes).unwrap();
    let mut file = changed.into_snapshot();
    assert!(
        file.state
            .keys()
            .filter(|key| key.starts_with(b"json/scalar-shift-page/"))
            .count()
            > 1
    );
    assert!(
        file.state
            .iter()
            .all(|(key, value)| key.len() + value.len() <= 1024 * 1024)
    );

    let mut middle = changes[COUNT / 2].clone();
    middle.row.as_mut().unwrap().insert(
        "scalar_json",
        sdk::TypedValue::Jsonb(serde_json::json!(7).into()),
    );
    file = harness
        .serialize_changes(&file, &[middle])
        .unwrap()
        .into_snapshot();
    let start = file
        .bytes
        .windows(b"\"k089999\":123".len())
        .position(|part| part == b"\"k089999\":123")
        .unwrap()
        + b"\"k089999\":".len();
    file = harness
        .parse_changes(
            &file,
            &file.path,
            &[sdk::FileEdit {
                offset: start as u64,
                delete_len: 3,
                insert: b"4567".to_vec(),
            }],
            None,
            sdk::CreateContext::from_namespace_bytes([0x52; 12]),
        )
        .unwrap()
        .into_snapshot();
    let value: serde_json::Value = serde_json::from_slice(&file.bytes).unwrap();
    assert_eq!(value["k045000"], 7);
    assert_eq!(value["k089999"], 4567);
    assert_eq!(value["k000000"], 123);

    let shrinking = changes[COUNT - 3_000..]
        .iter()
        .cloned()
        .map(|mut change| {
            change.row.as_mut().unwrap().insert(
                "scalar_json",
                sdk::TypedValue::Jsonb(serde_json::json!(0).into()),
            );
            change
        })
        .collect::<Vec<_>>();
    file = harness
        .serialize_changes(&file, &shrinking)
        .unwrap()
        .into_snapshot();
    assert_eq!(
        file.state
            .keys()
            .filter(|key| key.starts_with(SCALAR_SHIFT_PAGE_PREFIX))
            .count(),
        1
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&file.bytes).unwrap()["k089999"],
        0
    );

    // A structural source edit checkpoints current spans and removes all pages.
    file = harness
        .parse_changes(
            &file,
            &file.path,
            &[sdk::FileEdit {
                offset: (file.bytes.len() - 1) as u64,
                delete_len: 0,
                insert: br#", "new": 9"#.to_vec(),
            }],
            None,
            sdk::CreateContext::from_namespace_bytes([0x53; 12]),
        )
        .unwrap()
        .into_snapshot();
    assert!(!file.state.contains_key(SCALAR_SHIFTS_STATE));
    assert!(
        !file
            .state
            .keys()
            .any(|key| key.starts_with(b"json/scalar-shift-page/"))
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&file.bytes).unwrap()["new"],
        9
    );
}

#[test]
fn corrupt_shift_pages_are_rejected_without_panicking() {
    let (file, changes) = fixture(3);
    let harness = Harness::<JsonPlugin>::default();
    let file = harness
        .serialize_changes(&file, &changes)
        .unwrap()
        .into_snapshot();
    for corruption in 0..4 {
        let mut corrupt = file.clone();
        match corruption {
            0 => {
                corrupt.state.insert(SCALAR_SHIFTS_STATE.to_vec(), vec![0]);
            }
            1 => {
                corrupt.state.remove(&scalar_shift_page_key(0));
            }
            2 => {
                corrupt
                    .state
                    .insert(SCALAR_SHIFTS_STATE.to_vec(), 0_u64.to_le_bytes().to_vec());
            }
            _ => {
                corrupt
                    .state
                    .get_mut(&scalar_shift_page_key(0))
                    .unwrap()
                    .pop();
            }
        }
        assert!(harness.serialize_changes(&corrupt, &changes[..1]).is_err());
    }
}
