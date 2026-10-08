//! Byte admission is part of every provider's storage contract.
use super::{ConformanceReport, ConformanceResult, StorageFactory, open_storage};
use crate::storage::*;
use bytes::Bytes;
use std::ops::Bound;
const SPACE: StorageSpace = StorageSpace::mutable(SpaceId(700), "conformance.bounded");

pub(super) async fn register<F: StorageFactory>(report: &mut ConformanceReport, factory: &F) {
    report
        .run(
            "bounded::exact_results_and_ordered_prefixes",
            check(factory),
        )
        .await;
}
async fn check<F: StorageFactory>(factory: &F) -> ConformanceResult {
    let storage = open_storage(factory).await;
    let mut write = storage
        .begin_write(Default::default())
        .await
        .map_err(|e| e.to_string())?;
    write
        .put_many(
            SPACE,
            PutBatch {
                entries: (0..3)
                    .map(|index| PutEntry {
                        key: Key(Bytes::from(vec![index])),
                        value: StoredValue {
                            bytes: Bytes::from(vec![index; 128]),
                        },
                    })
                    .collect(),
            },
        )
        .await
        .map_err(|e| e.to_string())?;
    write.commit().await.map_err(|e| e.to_string())?;
    let read = storage
        .begin_read(Default::default())
        .await
        .map_err(|e| e.to_string())?;
    let keys = [
        Key(Bytes::from_static(&[2])),
        Key(Bytes::from_static(&[9])),
        Key(Bytes::from_static(&[0])),
        Key(Bytes::from_static(&[2])),
    ];
    let requests = [GetManyRequest {
        space: SPACE,
        keys: &keys,
        opts: Default::default(),
    }];
    let budget = ReadBudget {
        max_result_bytes: 384,
        max_single_value_bytes: 128,
    };
    let result = read
        .get_many_bounded(&requests, budget)
        .await
        .map_err(|e| e.to_string())?;
    if result.values.len() != 4
        || result.values[1].is_some()
        || result.values[0] != result.values[3]
    {
        return Err("bounded point reads changed ordering, duplicates or absence".into());
    }
    if !matches!(
        read.get_many_bounded(
            &requests,
            ReadBudget {
                max_result_bytes: 383,
                ..budget
            }
        )
        .await,
        Err(StorageError::ReadBudgetExceeded { singleton: false })
    ) {
        return Err("duplicate result slots did not count against aggregate bytes".into());
    }
    if !matches!(
        read.get_many_bounded(
            &requests[..1],
            ReadBudget {
                max_single_value_bytes: 127,
                ..budget
            }
        )
        .await,
        Err(StorageError::ReadBudgetExceeded { singleton: true })
    ) {
        return Err("single value codec cap was not enforced".into());
    }
    let prefix_budget = ReadBudget {
        max_result_bytes: 255,
        ..budget
    };
    let mut offset = 0;
    let mut values = Vec::new();
    loop {
        let page = read
            .get_many_bounded_prefix(&requests, offset, 32, prefix_budget)
            .await
            .map_err(|e| e.to_string())?;
        if page.values.is_empty() && page.next_offset.is_some() {
            return Err("empty point prefix made no progress".into());
        }
        prefix_budget
            .validate_result(&page.values)
            .map_err(|e| e.to_string())?;
        let next = offset + page.values.len();
        if page.next_offset != (next < keys.len()).then_some(next) {
            return Err("point prefix offset did not count missing slots".into());
        }
        values.extend(page.values);
        match page.next_offset {
            Some(next) => offset = next,
            None => break,
        }
    }
    if values != result.values {
        return Err("point prefix changed order, duplicates or absence".into());
    }
    let range = KeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    };
    for order in [ScanOrder::Ascending, ScanOrder::Descending] {
        let mut cursor = match read
            .begin_scan(
                SPACE,
                range.clone(),
                BeginScanOptions {
                    order,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(cursor) => cursor,
            Err(StorageError::Unsupported(Capability::ReverseScan))
                if order == ScanOrder::Descending =>
            {
                continue;
            }
            Err(error) => return Err(error.to_string()),
        };
        let mut seen = Vec::new();
        loop {
            let (rows, more) = cursor
                .next_page_bounded(
                    100,
                    ReadBudget {
                        max_result_bytes: 255,
                        ..budget
                    },
                )
                .await
                .map_err(|e| e.to_string())?
                .into_parts();
            if rows.len() > 1 || (more && rows.is_empty()) {
                return Err(
                    "scan byte limit did not produce a nonempty bounded ordered prefix".into(),
                );
            }
            seen.extend(rows.into_iter().map(|row| row.key.0[0]));
            if !more {
                break;
            }
        }
        let expected = if order == ScanOrder::Ascending {
            vec![0, 1, 2]
        } else {
            vec![2, 1, 0]
        };
        if seen != expected {
            return Err("byte-limited scan skipped or reordered a row".into());
        }
    }
    let mut cursor = read
        .begin_scan(SPACE, range.clone(), Default::default())
        .await
        .map_err(|e| e.to_string())?;
    let (rows, _) = cursor
        .next_page_bounded(
            10,
            ReadBudget {
                max_result_bytes: 64,
                ..budget
            },
        )
        .await
        .map_err(|e| e.to_string())?
        .into_parts();
    if rows.len() != 1 {
        return Err("explicit codec singleton should fit one byte-limited page".into());
    }
    let mut cursor = read
        .begin_scan(SPACE, range, Default::default())
        .await
        .map_err(|e| e.to_string())?;
    if !matches!(
        cursor
            .next_page_bounded(
                10,
                ReadBudget {
                    max_single_value_bytes: 127,
                    ..budget
                }
            )
            .await,
        Err(StorageError::ReadBudgetExceeded { singleton: true })
    ) {
        return Err("oversized first scan value was not a typed refusal".into());
    }
    Ok(())
}
