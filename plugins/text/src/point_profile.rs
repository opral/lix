//! Matched native adapter probe; also runs unchanged against the PR baseline.
use crate::{TextPlugin, sdk};
use sdk::testing::{Harness, Snapshot};
use std::time::Instant;

#[test]
#[ignore = "matched native point edit profile"]
fn text_matched_point_profile() {
    println!("rows,import_ms,point_p50_ms,point_p95_ms");
    for count in [1_000, 10_000, 100_000] {
        let mut driver = Harness::<TextPlugin>::default();
        driver.max_batch_bytes = 2 * 1024 * 1024;
        let source = Snapshot {
            file_id: "profile".into(),
            path: "/profile.txt".into(),
            bytes: b"original text line\n".repeat(count),
            ..Snapshot::default()
        };
        let start = Instant::now();
        let parsed = driver
            .parse(&source, sdk::CreateContext::from_namespace_bytes([1; 12]))
            .unwrap();
        let import = start.elapsed().as_secs_f64() * 1000.0;
        let mut change = parsed.row_changes[count / 2].clone();
        change.primary_key = vec![change.row.as_ref().unwrap().get("id").unwrap().clone()];
        change.local_ref = None;
        let mut snapshot = parsed.into_snapshot();
        let mut samples = Vec::new();
        for iteration in 0..26 {
            let content = if iteration % 2 == 0 {
                "short"
            } else {
                "a longer replacement for the original line"
            };
            change
                .row
                .as_mut()
                .unwrap()
                .insert("content", sdk::TypedValue::Text(content.into()));
            let start = Instant::now();
            let result = driver
                .serialize_changes(&snapshot, &[change.clone()])
                .unwrap();
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;
            if iteration >= 5 {
                samples.push(elapsed);
            }
            snapshot = result.into_snapshot();
            let mut expected = b"original text line\n".repeat(count / 2);
            expected.extend_from_slice(content.as_bytes());
            expected.push(b'\n');
            expected.extend_from_slice(&b"original text line\n".repeat(count - count / 2 - 1));
            assert_eq!(snapshot.bytes, expected);
        }
        samples.sort_by(f64::total_cmp);
        println!("{count},{import:.3},{:.3},{:.3}", samples[10], samples[19]);
    }
}
