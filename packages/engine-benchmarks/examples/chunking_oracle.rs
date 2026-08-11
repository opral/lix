//! Compares candidate binary-CAS chunking policies over a real-file corpus.
//!
//! `cas_sharing` measures what the engine stores today. This oracle answers the
//! next question without changing the engine: for the same real file pairs, how
//! much would each candidate policy have shared, how many chunk rows would it
//! have produced, and how fast does the boundary search itself run?
//!
//! ```sh
//! cargo run -p lix_benchmarks --release --features storage-benches,slatedb \
//!   --example chunking_oracle -- <corpus-dir>
//! ```
//!
//! Sharing is computed exactly the way the CAS computes it: chunk the base,
//! chunk the variant, and charge the variant for every chunk whose content hash
//! is not already in the base's chunk set.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// The resumable upload path accepts 16 MiB parts and completes up to four of
/// them out of order, so a shipped CDC has to force a boundary at every part
/// edge to keep parts independently chunkable. `anchor_bytes` models that.
#[derive(Clone, Copy, Debug)]
enum Policy {
    Fixed {
        chunk_bytes: usize,
    },
    Cdc {
        avg_bytes: usize,
        anchor_bytes: Option<usize>,
        /// Divisor for the minimum chunk size. FastCDC skips `min` bytes
        /// without hashing at the start of every chunk, so a larger minimum is
        /// directly less CPU per written byte.
        min_divisor: usize,
    },
}

impl Policy {
    fn label(&self) -> String {
        match self {
            Self::Fixed { chunk_bytes } => format!("fixed/{}", human(*chunk_bytes)),
            Self::Cdc {
                avg_bytes,
                anchor_bytes: None,
                min_divisor,
            } => format!("cdc/{}/min{}", human(*avg_bytes), min_divisor),
            Self::Cdc {
                avg_bytes,
                anchor_bytes: Some(anchor),
                min_divisor,
            } => format!(
                "cdc/{}@{}/min{}",
                human(*avg_bytes),
                human(*anchor),
                min_divisor
            ),
        }
    }

    fn ranges(&self, data: &[u8]) -> Vec<(usize, usize)> {
        match self {
            Self::Fixed { chunk_bytes } => (0..data.len())
                .step_by(*chunk_bytes)
                .map(|start| (start, start.saturating_add(*chunk_bytes).min(data.len())))
                .collect(),
            Self::Cdc {
                avg_bytes,
                anchor_bytes,
                min_divisor,
            } => {
                let span = anchor_bytes.unwrap_or(usize::MAX);
                let mut out = Vec::new();
                let mut base = 0usize;
                while base < data.len() {
                    let end = base.saturating_add(span).min(data.len());
                    out.extend(
                        cdc_ranges(&data[base..end], *avg_bytes, *min_divisor)
                            .into_iter()
                            .map(|(start, stop)| {
                                (base.saturating_add(start), base.saturating_add(stop))
                            }),
                    );
                    base = end;
                }
                out
            }
        }
    }
}

fn cdc_ranges(data: &[u8], avg_bytes: usize, min_divisor: usize) -> Vec<(usize, usize)> {
    if data.is_empty() {
        return Vec::new();
    }
    let min = (avg_bytes * 1 / min_divisor).max(64) as u32;
    let avg = avg_bytes as u32;
    let max = (avg_bytes * 4) as u32;
    fastcdc::v2020::FastCDC::new(data, min, avg, max)
        .map(|chunk| (chunk.offset, chunk.offset + chunk.length))
        .collect()
}

fn human(bytes: usize) -> String {
    if bytes >= 1 << 20 {
        format!("{}m", bytes >> 20)
    } else {
        format!("{}k", bytes >> 10)
    }
}

/// A scenario names the policy that chunked v1 and the policy that chunks v2.
/// They differ only for `mixed/*`, which models gating the chunker on write
/// context: v1 arrives as an initial write, v2 as a rewrite.
struct Scenario {
    label: &'static str,
    v1: Policy,
    v2: Policy,
}

const FIXED_1M: Policy = Policy::Fixed {
    chunk_bytes: 1 << 20,
};
const CDC_1M: Policy = Policy::Cdc {
    avg_bytes: 1 << 20,
    anchor_bytes: Some(16 << 20),
    min_divisor: 4,
};
const CDC_1M_MIN2: Policy = Policy::Cdc {
    avg_bytes: 1 << 20,
    anchor_bytes: Some(16 << 20),
    min_divisor: 2,
};
const CDC_1M_MIN1_33: Policy = Policy::Cdc {
    avg_bytes: 1 << 20,
    anchor_bytes: Some(16 << 20),
    min_divisor: 1,
};

const SCENARIOS: &[Scenario] = &[
    Scenario {
        label: "fixed/1m",
        v1: FIXED_1M,
        v2: FIXED_1M,
    },
    Scenario {
        label: "cdc/1m@16m/min4",
        v1: CDC_1M,
        v2: CDC_1M,
    },
    Scenario {
        label: "cdc/1m@16m/min2",
        v1: CDC_1M_MIN2,
        v2: CDC_1M_MIN2,
    },
    Scenario {
        label: "cdc/1m@16m/min1",
        v1: CDC_1M_MIN1_33,
        v2: CDC_1M_MIN1_33,
    },
    // The proposed gate: fixed chunking on the initial write, CDC on rewrites.
    Scenario {
        label: "mixed/fixed-v1_cdc-v2",
        v1: FIXED_1M,
        v2: CDC_1M,
    },
];

const THROUGHPUT_POLICIES: &[Policy] = &[FIXED_1M, CDC_1M, CDC_1M_MIN2, CDC_1M_MIN1_33];

const EDIT_BYTES: usize = 4 << 10;

fn main() {
    let corpus = PathBuf::from(
        std::env::args()
            .nth(1)
            .expect("usage: chunking_oracle <corpus-dir>"),
    );
    let mut cases = fs::read_dir(&corpus)
        .expect("read corpus directory")
        .map(|entry| entry.expect("read corpus entry").path())
        .filter(|path| path.is_dir())
        .collect::<Vec<_>>();
    cases.sort();

    for case in cases {
        let name = case
            .file_name()
            .expect("case directory name")
            .to_string_lossy()
            .into_owned();
        let base = fs::read(case.join("base.bin")).expect("read case base.bin");
        for (shape, v2) in shapes(&case, &base) {
            for scenario in SCENARIOS {
                report(&name, &shape, scenario, &base, &v2);
            }
        }
    }

    // Boundary-search throughput, measured on the largest corpus file so the
    // cost of the rolling hash is separated from any storage work.
    let mut largest = Vec::new();
    for case in fs::read_dir(&corpus).expect("read corpus directory") {
        let path = case.expect("read corpus entry").path();
        if !path.is_dir() {
            continue;
        }
        let data = fs::read(path.join("base.bin")).expect("read case base.bin");
        if data.len() > largest.len() {
            largest = data;
        }
    }
    for policy in THROUGHPUT_POLICIES {
        let mut best = f64::MAX;
        for _ in 0..9 {
            let started = Instant::now();
            let ranges = policy.ranges(&largest);
            let elapsed = started.elapsed().as_secs_f64();
            std::hint::black_box(ranges.len());
            best = best.min(elapsed);
        }
        println!(
            "chunking_throughput,policy={},bytes={},best_ms={:.3},mib_per_s={:.1}",
            policy.label(),
            largest.len(),
            best * 1_000.0,
            (largest.len() as f64 / (1 << 20) as f64) / best,
        );
    }
}

fn shapes(case: &Path, base: &[u8]) -> Vec<(String, Vec<u8>)> {
    let mut supplied = fs::read_dir(case)
        .expect("read case directory")
        .map(|entry| entry.expect("read case entry").path())
        .filter(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value.starts_with("v2_") && value.ends_with(".bin"))
        })
        .collect::<Vec<_>>();
    supplied.sort();

    let mut shapes = supplied
        .iter()
        .map(|path| {
            let label = path
                .file_name()
                .and_then(|value| value.to_str())
                .expect("supplied variant name")
                .trim_start_matches("v2_")
                .trim_end_matches(".bin");
            (
                format!("supplied/{label}"),
                fs::read(path).expect("read supplied variant"),
            )
        })
        .collect::<Vec<_>>();

    let mut overwrite = base.to_vec();
    let len = EDIT_BYTES.min(overwrite.len());
    let start = (overwrite.len() - len) / 2;
    fill(&mut overwrite[start..start + len], 0x5a17_5a17_5a17_5a17);
    shapes.push(("derived/overwrite_4k_mid".to_owned(), overwrite));

    let at = base.len() / 100;
    let mut inserted = Vec::with_capacity(base.len() + EDIT_BYTES);
    inserted.extend_from_slice(&base[..at]);
    let mut patch = vec![0u8; EDIT_BYTES];
    fill(&mut patch, 0x1234_5678_9abc_def0);
    inserted.extend_from_slice(&patch);
    inserted.extend_from_slice(&base[at..]);
    shapes.push(("derived/insert_4k_at_1pct".to_owned(), inserted));

    let end = (at + EDIT_BYTES).min(base.len());
    let mut deleted = Vec::with_capacity(base.len() - (end - at));
    deleted.extend_from_slice(&base[..at]);
    deleted.extend_from_slice(&base[end..]);
    shapes.push(("derived/delete_4k_at_1pct".to_owned(), deleted));

    let mut appended = base.to_vec();
    let mut tail = vec![0u8; 1 << 20];
    fill(&mut tail, 0x0fed_cba9_8765_4321);
    appended.extend_from_slice(&tail);
    shapes.push(("derived/append_1m".to_owned(), appended));

    shapes
}

fn report(case: &str, shape: &str, scenario: &Scenario, base: &[u8], v2: &[u8]) {
    let base_ranges = scenario.v1.ranges(base);
    let v2_ranges = scenario.v2.ranges(v2);
    let known = base_ranges
        .iter()
        .map(|&(start, end)| *blake3::hash(&base[start..end]).as_bytes())
        .collect::<HashSet<_>>();
    let mut new_bytes = 0u64;
    let mut new_chunks = 0u64;
    let mut seen = HashSet::new();
    for &(start, end) in &v2_ranges {
        let hash = *blake3::hash(&v2[start..end]).as_bytes();
        if known.contains(&hash) || !seen.insert(hash) {
            continue;
        }
        new_bytes += (end - start) as u64;
        new_chunks += 1;
    }
    let shared = (v2.len() as u64).saturating_sub(new_bytes);
    println!(
        "chunking_oracle,case={case},shape={shape},policy={},\
         v1_bytes={},v2_bytes={},v1_chunks={},v2_chunks={},\
         new_chunks={new_chunks},new_bytes={new_bytes},shared_bytes={shared},\
         sharing_ratio={:.4},mean_chunk_bytes={}",
        scenario.label,
        base.len(),
        v2.len(),
        base_ranges.len(),
        v2_ranges.len(),
        shared as f64 / v2.len() as f64,
        base.len() / base_ranges.len().max(1),
    );
}

fn fill(bytes: &mut [u8], seed: u64) {
    let mut state = seed ^ 0xd1b5_4a32_d192_ed03;
    for chunk in bytes.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let generated = state.to_le_bytes();
        chunk.copy_from_slice(&generated[..chunk.len()]);
    }
}
