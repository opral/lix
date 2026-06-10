use super::types::{
    ChangeId, ChangeRecord, ChangeRecordRef, ChangeRecordView, CommitChangeRef,
    CommitChangeRefChunk, CommitChangeRefChunkRef, CommitChangeRefChunkView,
    CommitChangeRefEntryRef, CommitChangeRefEntryView, CommitId, CommitRecord, EntityPkRef,
};
use crate::common::LixError;
use crate::entity_pk::EntityPk;
use crate::storage_codec;

pub(crate) fn encode_commit_record(record: &CommitRecord) -> Result<Vec<u8>, LixError> {
    storage_codec::encode("commit record", record)
}

pub(crate) fn decode_commit_record(bytes: &[u8]) -> Result<CommitRecord, LixError> {
    storage_codec::decode("commit record", bytes)
}

pub(crate) fn encode_change_record(record: &ChangeRecord) -> Result<Vec<u8>, LixError> {
    // change_id is the storage key; the value intentionally omits it.
    storage_codec::encode(
        "change record",
        &ChangeRecordRef {
            format_version: record.format_version,
            schema_key: &record.schema_key,
            entity_pk: &record.entity_pk.parts,
            file_id: record.file_id.as_deref(),
            snapshot_ref: record.snapshot_ref.as_ref(),
            metadata_ref: record.metadata_ref.as_ref(),
            created_at: record.created_at,
        },
    )
}

pub(crate) fn decode_change_record(
    bytes: &[u8],
    change_id: ChangeId,
) -> Result<ChangeRecord, LixError> {
    let view: ChangeRecordView<'_> = storage_codec::decode("change record", bytes)?;
    Ok(ChangeRecord {
        format_version: view.format_version,
        change_id,
        schema_key: view.schema_key.to_string(),
        entity_pk: entity_pk_from_ref(view.entity_pk)?,
        file_id: view.file_id.map(str::to_string),
        snapshot_ref: view.snapshot_ref,
        metadata_ref: view.metadata_ref,
        created_at: view.created_at,
    })
}

pub(crate) fn encode_commit_change_ref_chunk(
    chunk: &CommitChangeRefChunk,
) -> Result<Vec<u8>, LixError> {
    let schema_keys = commit_change_ref_schema_dictionary(&chunk.entries);
    let file_ids = commit_change_ref_file_dictionary(&chunk.entries);
    // Entity pks are stored front-coded against the previous entry's encoded
    // pk; the chunker sorts entries, so consecutive pks share long prefixes.
    // Correctness does not depend on sortedness: unsorted input only loses
    // compression (shared prefix collapses toward zero).
    let encoded_pks = chunk
        .entries
        .iter()
        .map(|entry| encode_ref_entity_pk(&entry.entity_pk.parts))
        .collect::<Vec<_>>();
    let mut entries = Vec::with_capacity(chunk.entries.len());
    let mut previous_pk: &[u8] = &[];
    for (entry, pk_bytes) in chunk.entries.iter().zip(&encoded_pks) {
        let schema_index = dictionary_index(
            schema_keys.iter().copied(),
            entry.schema_key.as_str(),
            "commit change ref schema dictionary",
        )?;
        let file_index = optional_dictionary_index(
            file_ids.iter().copied(),
            entry.file_id.as_deref(),
            "commit change ref file dictionary",
        )?;
        let shared = shared_prefix_len(previous_pk, pk_bytes);
        entries.push(CommitChangeRefEntryRef {
            schema_index: u16_index(schema_index, "commit change ref schema index")?,
            file_index: u16_index(file_index, "commit change ref file index")?,
            pk_shared: u32::try_from(shared).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "commit change ref shared pk length exceeds u32",
                )
            })?,
            pk_suffix: &pk_bytes[shared..],
            change_id: entry.change_id,
        });
        previous_pk = pk_bytes;
    }
    storage_codec::encode(
        "commit change ref chunk",
        &CommitChangeRefChunkRef {
            format_version: chunk.format_version,
            schema_keys,
            file_ids,
            entries,
        },
    )
}

pub(crate) fn shared_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter().zip(right).take_while(|(l, r)| l == r).count()
}

/// Length-free entity-pk byte encoding for front-coding.
///
/// Length-prefixed encodings put a varying length byte ahead of the content,
/// which truncates prefix sharing between same-shaped pks of different
/// lengths. Instead, each part's bytes are emitted with `0x00` escaped as
/// `0x00 0xFF` and the part terminated by `0x00 0x01`, so byte-prefix
/// sharing equals content-prefix sharing.
pub(crate) fn encode_ref_entity_pk(parts: &[String]) -> Vec<u8> {
    // An empty parts list encodes to zero bytes, which decode rejects via
    // EntityPk validation; all constructors validate, this catches bypasses.
    debug_assert!(!parts.is_empty(), "entity pk parts must not be empty");
    let mut out = Vec::with_capacity(parts.iter().map(|part| part.len() + 2).sum());
    for part in parts {
        for &byte in part.as_bytes() {
            if byte == 0 {
                out.extend_from_slice(&[0x00, 0xFF]);
            } else {
                out.push(byte);
            }
        }
        out.extend_from_slice(&[0x00, 0x01]);
    }
    out
}

fn decode_ref_entity_pk(context: &str, bytes: &[u8]) -> Result<Vec<String>, LixError> {
    let mut parts = Vec::new();
    let mut current = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let byte = bytes[offset];
        offset += 1;
        if byte != 0 {
            current.push(byte);
            continue;
        }
        match bytes.get(offset) {
            Some(0xFF) => {
                current.push(0);
                offset += 1;
            }
            Some(0x01) => {
                offset += 1;
                let part = String::from_utf8(std::mem::take(&mut current)).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("changelog {context} entity pk part is not UTF-8"),
                    )
                })?;
                parts.push(part);
            }
            _ => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("changelog {context} entity pk has an invalid escape"),
                ));
            }
        }
    }
    if !current.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("changelog {context} entity pk has an unterminated part"),
        ));
    }
    Ok(parts)
}

pub(crate) fn decode_commit_change_ref_chunk(
    bytes: &[u8],
    commit_id: CommitId,
) -> Result<CommitChangeRefChunk, LixError> {
    let CommitChangeRefChunkView {
        format_version,
        schema_keys,
        file_ids,
        entries: storage_entries,
    } = storage_codec::decode("commit change ref chunk", bytes)?;
    let mut entries = Vec::with_capacity(storage_entries.len());
    let mut previous_pk: Vec<u8> = Vec::new();
    for (index, entry) in storage_entries.into_iter().enumerate() {
        let context = format!("commit change ref entries[{index}]");
        let CommitChangeRefEntryView {
            schema_index,
            file_index,
            pk_shared,
            pk_suffix,
            change_id,
        } = entry;
        let schema_index = schema_index as usize;
        let file_index = file_index as usize;
        let schema_key = *schema_keys.get(schema_index).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog {context}.schema_index is out of bounds"),
            )
        })?;
        let file_id = *file_ids.get(file_index).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog {context}.file_index is out of bounds"),
            )
        })?;
        let shared = pk_shared as usize;
        if shared > previous_pk.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog {context}.pk_shared exceeds the previous entity pk length"),
            ));
        }
        let mut pk_bytes = Vec::with_capacity(shared + pk_suffix.len());
        pk_bytes.extend_from_slice(&previous_pk[..shared]);
        pk_bytes.extend_from_slice(pk_suffix);
        let parts = decode_ref_entity_pk(&context, &pk_bytes)?;
        let entity_pk = EntityPk::from_parts(parts).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog {context} entity primary key is invalid: {error}"),
            )
        })?;
        entries.push(CommitChangeRef {
            schema_key: schema_key.to_string(),
            file_id: file_id.map(str::to_string),
            entity_pk,
            change_id,
        });
        previous_pk = pk_bytes;
    }
    Ok(CommitChangeRefChunk {
        format_version,
        commit_id,
        entries,
    })
}

fn commit_change_ref_schema_dictionary(entries: &[CommitChangeRef]) -> Vec<&str> {
    let mut values = Vec::new();
    for entry in entries {
        if !values.contains(&entry.schema_key.as_str()) {
            values.push(entry.schema_key.as_str());
        }
    }
    values
}

fn commit_change_ref_file_dictionary(entries: &[CommitChangeRef]) -> Vec<Option<&str>> {
    let mut values = Vec::new();
    for entry in entries {
        let value = entry.file_id.as_deref();
        if !values.contains(&value) {
            values.push(value);
        }
    }
    values
}

fn dictionary_index<'a>(
    mut values: impl Iterator<Item = &'a str>,
    needle: &str,
    context: &str,
) -> Result<usize, LixError> {
    values.position(|value| value == needle).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("changelog {context} is missing value '{needle}'"),
        )
    })
}

fn optional_dictionary_index<'a>(
    mut values: impl Iterator<Item = Option<&'a str>>,
    needle: Option<&str>,
    context: &str,
) -> Result<usize, LixError> {
    values.position(|value| value == needle).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("changelog {context} is missing optional value"),
        )
    })
}

fn u16_index(value: usize, context: &str) -> Result<u16, LixError> {
    u16::try_from(value).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("changelog {context} exceeds u16"),
        )
    })
}

fn entity_pk_from_ref(value: EntityPkRef<'_>) -> Result<EntityPk, LixError> {
    EntityPk::from_parts(value.parts.iter().map(|part| (*part).to_string()).collect()).map_err(
        |error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("changelog entity primary key is invalid: {error}"),
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::ChangeId;
    use crate::common::LixTimestamp;
    use crate::json_store::JsonRef;

    fn ref_chunk(entries: Vec<CommitChangeRef>) -> CommitChangeRefChunk {
        CommitChangeRefChunk {
            format_version: 1,
            commit_id: CommitId::for_test_label("ref-chunk-commit"),
            entries,
        }
    }

    fn ref_entry(
        schema: &str,
        file: Option<&str>,
        parts: &[&str],
        change: &str,
    ) -> CommitChangeRef {
        CommitChangeRef {
            schema_key: schema.to_string(),
            file_id: file.map(str::to_string),
            entity_pk: EntityPk::from_parts(parts.iter().map(ToString::to_string).collect())
                .expect("entity pk should build"),
            change_id: ChangeId::for_test_label(change),
        }
    }

    #[test]
    fn ref_chunk_round_trips_front_coded_entity_pks() {
        // Sorted, prefix-heavy single-part pks plus multi-part and unicode
        // entries; mixed file ids exercise both dictionaries.
        let mut entries = vec![
            ref_entry(
                "schema_a",
                Some("file-1"),
                &["/packages/0001/version"],
                "c1",
            ),
            ref_entry(
                "schema_a",
                Some("file-1"),
                &["/packages/0002/version"],
                "c2",
            ),
            ref_entry(
                "schema_a",
                Some("file-2"),
                &["/packages/0002/version"],
                "c3",
            ),
            ref_entry("schema_b", None, &["ns", "42"], "c4"),
            ref_entry("schema_b", None, &["ns", "43"], "c5"),
            ref_entry("schema_b", None, &["sch\u{00e9}ma-\u{4e2d}"], "c6"),
        ];
        entries.sort_by(|l, r| {
            (l.schema_key.as_str(), l.file_id.as_deref(), &l.entity_pk).cmp(&(
                r.schema_key.as_str(),
                r.file_id.as_deref(),
                &r.entity_pk,
            ))
        });
        let chunk = ref_chunk(entries);
        let encoded = encode_commit_change_ref_chunk(&chunk).expect("chunk should encode");
        let decoded =
            decode_commit_change_ref_chunk(&encoded, chunk.commit_id).expect("chunk should decode");
        assert_eq!(decoded, chunk);
    }

    #[test]
    fn ref_chunk_front_coding_compresses_sorted_prefix_heavy_pks() {
        let entries = (0..500usize)
            .map(|index| {
                ref_entry(
                    "json_pointer",
                    Some("pnpm-lock"),
                    &[&format!("/packages/{index:04}/resolution/integrity")],
                    &format!("change-{index:04}"),
                )
            })
            .collect::<Vec<_>>();
        let chunk = ref_chunk(entries);
        let encoded = encode_commit_change_ref_chunk(&chunk).expect("chunk should encode");
        // Each pk encodes to 37 bytes (35 content + terminator); adjacent
        // sorted pks share a ~13-byte head (the identical tail is not
        // prefix-sharable), so the front-coded form must shed at least 7
        // bytes per entry against the ~55-byte verbatim entry footprint.
        let decoded =
            decode_commit_change_ref_chunk(&encoded, chunk.commit_id).expect("chunk should decode");
        assert_eq!(decoded, chunk);
        assert!(
            encoded.len() < 500 * 48,
            "front coding must compress sorted pks: encoded={}",
            encoded.len()
        );
    }

    #[test]
    fn ref_chunk_round_trips_unsorted_entries_without_compression() {
        // Correctness must not depend on sortedness.
        let chunk = ref_chunk(vec![
            ref_entry("schema_b", None, &["zzz"], "c1"),
            ref_entry("schema_a", Some("file-1"), &["aaa"], "c2"),
        ]);
        let encoded = encode_commit_change_ref_chunk(&chunk).expect("chunk should encode");
        let decoded =
            decode_commit_change_ref_chunk(&encoded, chunk.commit_id).expect("chunk should decode");
        assert_eq!(decoded, chunk);
    }

    /// Pins the stored entry layout. Drift in the front-coding (e.g. a
    /// different pk byte encoding) silently changes every persisted chunk.
    #[test]
    fn ref_chunk_wire_format_is_pinned() {
        let chunk = ref_chunk(vec![
            ref_entry("s", None, &["abc"], "pin-1"),
            ref_entry("s", None, &["abd"], "pin-2"),
        ]);
        let encoded = encode_commit_change_ref_chunk(&chunk).expect("chunk should encode");
        let change_1 = ChangeId::for_test_label("pin-1");
        let change_2 = ChangeId::for_test_label("pin-2");
        let mut expected = vec![
            1, // format_version
            1, 1, b's', // schema dictionary: ["s"]
            1, 0, // file dictionary: [None]
            2, // entry count
            // entry 0: schema 0, file 0, pk_shared 0,
            // suffix "abc" + part terminator 0x00 0x01
            0, 0, 0, 5, b'a', b'b', b'c', 0x00, 0x01,
        ];
        expected.extend_from_slice(change_1.as_uuid().as_bytes());
        // entry 1: schema 0, file 0, pk_shared 2 ("ab"), suffix "d" + terminator
        expected.extend_from_slice(&[0, 0, 2, 3, b'd', 0x00, 0x01]);
        expected.extend_from_slice(change_2.as_uuid().as_bytes());
        assert_eq!(encoded, expected, "ref chunk wire bytes must stay stable");
    }

    #[test]
    fn ref_chunk_round_trips_nul_bytes_in_pk_parts() {
        // Embedded 0x00 exercises the escape; an empty part exercises the
        // bare terminator.
        let chunk = ref_chunk(vec![ref_entry(
            "s",
            None,
            &["a\u{0}b", "", "plain"],
            "nul-change",
        )]);
        let encoded = encode_commit_change_ref_chunk(&chunk).expect("chunk should encode");
        let decoded =
            decode_commit_change_ref_chunk(&encoded, chunk.commit_id).expect("chunk should decode");
        assert_eq!(decoded, chunk);
    }

    #[test]
    fn ref_chunk_rejects_truncated_pk_escape() {
        // A pk ending mid-escape (bare trailing 0x00) must be rejected.
        let bogus = CommitChangeRefChunkRef {
            format_version: 1,
            schema_keys: vec!["s"],
            file_ids: vec![None],
            entries: vec![CommitChangeRefEntryRef {
                schema_index: 0,
                file_index: 0,
                pk_shared: 0,
                pk_suffix: &[b'a', 0x00],
                change_id: ChangeId::for_test_label("bogus"),
            }],
        };
        let bytes = storage_codec::encode("test chunk", &bogus).expect("should encode");
        let error =
            decode_commit_change_ref_chunk(&bytes, CommitId::for_test_label("ref-chunk-commit"))
                .expect_err("truncated escape must reject");
        assert!(error.message.contains("invalid escape"));
    }

    /// Pins the escape bytes themselves: 0x00 content escapes to 0x00 0xFF.
    #[test]
    fn ref_chunk_escape_wire_format_is_pinned() {
        let chunk = ref_chunk(vec![ref_entry("s", None, &["a\u{0}b"], "esc-pin")]);
        let encoded = encode_commit_change_ref_chunk(&chunk).expect("chunk should encode");
        let change = ChangeId::for_test_label("esc-pin");
        let mut expected = vec![
            1, // format_version
            1, 1, b's', // schema dictionary
            1, 0, // file dictionary
            1, // entry count
            // pk_shared 0, suffix: 'a', escaped NUL, 'b', part terminator
            0, 0, 0, 6, b'a', 0x00, 0xFF, b'b', 0x00, 0x01,
        ];
        expected.extend_from_slice(change.as_uuid().as_bytes());
        assert_eq!(encoded, expected, "escape wire bytes must stay stable");
    }

    #[test]
    fn ref_chunk_round_trips_escape_edge_cases() {
        // Part "\0" alone, trailing NUL, consecutive NULs, and 0x01 as plain
        // content; the x-entries' shared prefix splits the previous pk's
        // escape pair between 0x00 and 0xFF.
        let chunk = ref_chunk(vec![
            ref_entry("s", None, &["\u{0}"], "e1"),
            ref_entry("s", None, &["\u{0}\u{0}"], "e2"),
            ref_entry("s", None, &["a\u{0}"], "e3"),
            ref_entry("s", None, &["a\u{1}b"], "e4"),
            ref_entry("s", None, &["x\u{0}"], "e5"),
            ref_entry("s", None, &["x"], "e6"),
        ]);
        let encoded = encode_commit_change_ref_chunk(&chunk).expect("chunk should encode");
        let decoded =
            decode_commit_change_ref_chunk(&encoded, chunk.commit_id).expect("chunk should decode");
        assert_eq!(decoded, chunk);
    }

    #[test]
    fn ref_chunk_rejects_non_utf8_pk_part() {
        // 0xFF as plain content can never come from the encoder (parts are
        // UTF-8 strings); a crafted suffix must fail the UTF-8 check.
        let bogus = CommitChangeRefChunkRef {
            format_version: 1,
            schema_keys: vec!["s"],
            file_ids: vec![None],
            entries: vec![CommitChangeRefEntryRef {
                schema_index: 0,
                file_index: 0,
                pk_shared: 0,
                pk_suffix: &[0xFF, 0x00, 0x01],
                change_id: ChangeId::for_test_label("bogus"),
            }],
        };
        let bytes = storage_codec::encode("test chunk", &bogus).expect("should encode");
        let error =
            decode_commit_change_ref_chunk(&bytes, CommitId::for_test_label("ref-chunk-commit"))
                .expect_err("non-UTF-8 part must reject");
        assert!(error.message.contains("is not UTF-8"));
    }

    #[test]
    fn ref_chunk_rejects_out_of_bounds_dictionary_indices() {
        for (schema_index, file_index) in [(7u16, 0u16), (0, 7)] {
            let bogus = CommitChangeRefChunkRef {
                format_version: 1,
                schema_keys: vec!["s"],
                file_ids: vec![None],
                entries: vec![CommitChangeRefEntryRef {
                    schema_index,
                    file_index,
                    pk_shared: 0,
                    pk_suffix: &[b'k', 0x00, 0x01],
                    change_id: ChangeId::for_test_label("bogus"),
                }],
            };
            let bytes = storage_codec::encode("test chunk", &bogus).expect("should encode");
            let error = decode_commit_change_ref_chunk(
                &bytes,
                CommitId::for_test_label("ref-chunk-commit"),
            )
            .expect_err("out-of-bounds dictionary index must reject");
            assert!(error.message.contains("is out of bounds"));
        }
    }

    #[test]
    fn ref_chunk_rejects_adversarial_extreme_shared_len() {
        // The over-share guard must fire before any allocation at u32::MAX.
        let extreme = CommitChangeRefChunkRef {
            format_version: 1,
            schema_keys: vec!["s"],
            file_ids: vec![None],
            entries: vec![CommitChangeRefEntryRef {
                schema_index: 0,
                file_index: 0,
                pk_shared: u32::MAX,
                pk_suffix: b"",
                change_id: ChangeId::for_test_label("bogus"),
            }],
        };
        let bytes = storage_codec::encode("test chunk", &extreme).expect("should encode");
        assert!(
            decode_commit_change_ref_chunk(&bytes, CommitId::for_test_label("ref-chunk-commit"))
                .is_err()
        );
    }

    #[test]
    fn ref_chunk_rejects_empty_pk() {
        // A zero-byte pk decodes to an empty parts list, which EntityPk
        // validation must reject.
        let bogus = CommitChangeRefChunkRef {
            format_version: 1,
            schema_keys: vec!["s"],
            file_ids: vec![None],
            entries: vec![CommitChangeRefEntryRef {
                schema_index: 0,
                file_index: 0,
                pk_shared: 0,
                pk_suffix: b"",
                change_id: ChangeId::for_test_label("bogus"),
            }],
        };
        let bytes = storage_codec::encode("test chunk", &bogus).expect("should encode");
        let error =
            decode_commit_change_ref_chunk(&bytes, CommitId::for_test_label("ref-chunk-commit"))
                .expect_err("empty pk must reject");
        assert!(error.message.contains("entity primary key is invalid"));
    }

    #[test]
    fn ref_chunk_rejects_over_shared_pk() {
        // First entry cannot share bytes with a non-existent predecessor.
        let bogus = CommitChangeRefChunkRef {
            format_version: 1,
            schema_keys: vec!["s"],
            file_ids: vec![None],
            entries: vec![CommitChangeRefEntryRef {
                schema_index: 0,
                file_index: 0,
                pk_shared: 4,
                pk_suffix: b"",
                change_id: ChangeId::for_test_label("bogus"),
            }],
        };
        let bytes = storage_codec::encode("test chunk", &bogus).expect("should encode");
        let error =
            decode_commit_change_ref_chunk(&bytes, CommitId::for_test_label("ref-chunk-commit"))
                .expect_err("over-shared pk must reject");
        assert!(error.message.contains("pk_shared exceeds"));
    }

    fn full_record() -> ChangeRecord {
        ChangeRecord {
            format_version: 1,
            change_id: ChangeId::for_test_label("roundtrip-change"),
            schema_key: "schema-\u{00e9}\u{4e2d}".to_string(),
            entity_pk: EntityPk::from_parts(vec!["part-a".to_string(), "part-b".to_string()])
                .expect("entity pk should build"),
            file_id: Some("file-1".to_string()),
            snapshot_ref: Some(JsonRef::for_content(b"snapshot")),
            metadata_ref: Some(JsonRef::for_content(b"metadata")),
            created_at: LixTimestamp::expect_parse("created_at", "2026-06-10T00:00:00.000Z"),
        }
    }

    #[test]
    fn change_record_round_trips_fully_populated() {
        let record = full_record();
        let encoded = encode_change_record(&record).expect("record should encode");
        let decoded =
            decode_change_record(&encoded, record.change_id).expect("record should decode");
        assert_eq!(decoded, record);
    }

    #[test]
    fn change_record_round_trips_with_empty_options() {
        let record = ChangeRecord {
            file_id: None,
            snapshot_ref: None,
            metadata_ref: None,
            ..full_record()
        };
        let encoded = encode_change_record(&record).expect("record should encode");
        let decoded =
            decode_change_record(&encoded, record.change_id).expect("record should decode");
        assert_eq!(decoded, record);
    }

    #[test]
    fn change_record_takes_identity_from_the_decode_argument() {
        // The stored value omits change_id; whatever id the key supplies is
        // what the decoded record carries.
        let record = full_record();
        let encoded = encode_change_record(&record).expect("record should encode");
        let other_id = ChangeId::for_test_label("other-change");
        let decoded = decode_change_record(&encoded, other_id).expect("record should decode");
        assert_eq!(decoded.change_id, other_id);
    }
}
