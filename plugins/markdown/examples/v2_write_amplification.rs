use plugin_md_v2::exports::lix::plugin::api::{EntityState, Guest};
use plugin_md_v2::{DetectedChange, File, MarkdownPlugin};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

fn main() {
    println!(
        "| scenario | logical items | active rows | changed rows | changed KiB | detect median | render median |"
    );
    println!("| --- | ---: | ---: | ---: | ---: | ---: | ---: |");
    for size in [100, 1_000, 5_000] {
        benchmark("paragraph", size, paragraph_sources(size));
        benchmark("list item", size, list_sources(size));
    }
    for size in [100, 1_000] {
        benchmark("table cell", size, table_sources(size));
    }
}

fn benchmark(name: &str, size: usize, (before, after): (String, String)) {
    let state = project(&before);
    let sample = MarkdownPlugin::detect_changes(state.clone(), file(&after)).unwrap();
    let changed_rows = sample.len();
    let changed_bytes = sample
        .iter()
        .filter_map(|change| change.snapshot_content.as_ref())
        .map(String::len)
        .sum::<usize>();
    let detect = median_duration((0..9).map(|_| {
        let start = Instant::now();
        let _ = MarkdownPlugin::detect_changes(state.clone(), file(&after)).unwrap();
        start.elapsed()
    }));
    let render = median_duration((0..9).map(|_| {
        let start = Instant::now();
        let _ = MarkdownPlugin::render(state.clone()).unwrap();
        start.elapsed()
    }));
    let changed_kib = changed_bytes / 1024;
    let changed_kib_hundredths = (changed_bytes % 1024) * 100 / 1024;
    println!(
        "| {name} | {size} | {} | {changed_rows} | {changed_kib}.{changed_kib_hundredths:02} | {:.2?} | {:.2?} |",
        state.len(),
        detect,
        render,
    );
}

fn project(source: &str) -> Vec<EntityState> {
    apply(
        Vec::new(),
        MarkdownPlugin::detect_changes(Vec::new(), file(source)).unwrap(),
    )
}

fn apply(state: Vec<EntityState>, changes: Vec<DetectedChange>) -> Vec<EntityState> {
    let mut rows = state
        .into_iter()
        .map(|row| ((row.schema_key.clone(), row.entity_pk.clone()), row))
        .collect::<BTreeMap<_, _>>();
    for change in changes {
        let key = (change.schema_key.clone(), change.entity_pk.clone());
        if let Some(snapshot_content) = change.snapshot_content {
            rows.insert(
                key,
                EntityState {
                    entity_pk: change.entity_pk,
                    schema_key: change.schema_key,
                    snapshot_content,
                    metadata: change.metadata,
                },
            );
        } else {
            rows.remove(&key);
        }
    }
    rows.into_values().collect()
}

fn file(source: &str) -> File {
    File {
        filename: Some("benchmark.md".to_string()),
        data: source.as_bytes().to_vec(),
    }
}

fn median_duration(samples: impl IntoIterator<Item = Duration>) -> Duration {
    let mut samples = samples.into_iter().collect::<Vec<_>>();
    samples.sort();
    samples[samples.len() / 2]
}

fn paragraph_sources(size: usize) -> (String, String) {
    let before = (0..size)
        .map(|index| format!("Paragraph {index:05}"))
        .collect::<Vec<_>>()
        .join("\n\n")
        + "\n";
    let after = before.replace(
        &format!("Paragraph {:05}", size / 2),
        &format!("Paragraph {:05} edited", size / 2),
    );
    (before, after)
}

fn list_sources(size: usize) -> (String, String) {
    let before = (0..size)
        .map(|index| format!("- Item {index:05}"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let after = before.replace(
        &format!("Item {:05}", size / 2),
        &format!("Item {:05} edited", size / 2),
    );
    (before, after)
}

fn table_sources(size: usize) -> (String, String) {
    let body = (0..size)
        .map(|index| format!("| Row {index:05} | Value {index:05} |"))
        .collect::<Vec<_>>()
        .join("\n");
    let before = format!("| Name | Value |\n| --- | --- |\n{body}\n");
    let after = before.replace(
        &format!("Value {:05}", size / 2),
        &format!("Value {:05} edited", size / 2),
    );
    (before, after)
}
