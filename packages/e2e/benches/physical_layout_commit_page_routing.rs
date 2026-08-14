//! EXP-COMMIT-PAGE-ROUTING: smallest authenticated routing-directory crossover.
//!
//! This models immutable V3 page geometry without changing production. A
//! valid range directory requires non-overlapping canonical StateKey
//! intervals. V3 pages preserve commit/result ordinal order, so the model
//! measures both real ordinal ordering and the hypothetical key-sorted order
//! required by such a directory.

use std::collections::BTreeSet;

const PAGE_MEMBERS: usize = 270;
const PAGE_BYTES: usize = 64 * 1024;
const DIRECTORY_FIXED_BYTES: usize = 32;
const DIRECTORY_ENTRY_FIXED_BYTES: usize = 32 + 4 + 32;

#[derive(Clone)]
struct Page {
    keys: Vec<Vec<u8>>,
}

impl Page {
    fn bounds(&self) -> (&[u8], &[u8]) {
        let lower = self.keys.iter().min().expect("nonempty page");
        let upper = self.keys.iter().max().expect("nonempty page");
        (lower, upper)
    }
}

fn key(index: usize, width: usize) -> Vec<u8> {
    let mut out = format!("schema/entity/{index:020}").into_bytes();
    out.resize(width.max(out.len()), b'x');
    out
}

fn shuffled_indices(len: usize) -> Vec<usize> {
    let mut values = (0..len).collect::<Vec<_>>();
    let mut state = 0x9e37_79b9_7f4a_7c15_u64 ^ len as u64;
    for index in (1..len).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        values.swap(index, state as usize % (index + 1));
    }
    values
}

fn pages(keys: Vec<Vec<u8>>) -> Vec<Page> {
    keys.chunks(PAGE_MEMBERS)
        .map(|chunk| Page {
            keys: chunk.to_vec(),
        })
        .collect()
}

fn valid_nonoverlapping_ranges(pages: &[Page]) -> bool {
    let mut ranges = pages.iter().map(Page::bounds).collect::<Vec<_>>();
    ranges.sort_unstable_by(|left, right| left.0.cmp(right.0));
    ranges.windows(2).all(|pair| pair[0].1 < pair[1].0)
}

fn selected_pages(pages: &[Page], target: &[u8]) -> usize {
    pages
        .iter()
        .filter(|page| {
            let (lower, upper) = page.bounds();
            lower <= target && target <= upper
        })
        .count()
}

fn directory_bytes(pages: &[Page]) -> usize {
    DIRECTORY_FIXED_BYTES
        + pages
            .iter()
            .map(|page| {
                let (lower, _) = page.bounds();
                DIRECTORY_ENTRY_FIXED_BYTES + lower.len()
            })
            .sum::<usize>()
}

fn assert_corruption_controls() {
    let sorted = pages((0..600).map(|index| key(index, 40)).collect());
    assert!(valid_nonoverlapping_ranges(&sorted));

    let mut duplicate = sorted.clone();
    duplicate[1].keys[0] = duplicate[0].keys[0].clone();
    assert!(!valid_nonoverlapping_ranges(&duplicate));

    let mut substituted = sorted.clone();
    substituted[1].keys[0] = key(1, 40);
    assert!(!valid_nonoverlapping_ranges(&substituted));

    let truncated = &sorted[..sorted.len() - 1];
    let covered = truncated
        .iter()
        .flat_map(|page| page.keys.iter())
        .collect::<BTreeSet<_>>();
    assert!(covered.len() < 600);

    let interleaved = pages(
        shuffled_indices(600)
            .into_iter()
            .map(|index| key(index, 40))
            .collect(),
    );
    assert!(!valid_nonoverlapping_ranges(&interleaved));
}

fn main() {
    assert_corruption_controls();
    println!(
        "N,D,H,key_bytes,pages,ordinal_valid,ordinal_point_pages,sorted_point_pages,baseline_point_bytes,ordinal_directory_point_bytes,sorted_directory_point_bytes,directory_bytes,full_history_byte_overhead_pct,publication_byte_overhead_pct,diff_page_savings_pct,count_dir_baseline_pages,count_dir_selected_pages,count_dir_page_savings_pct,count_dir_root_overhead_pct"
    );
    for n in [1_000_usize, 10_000, 50_000, 100_000] {
        for d in [1_usize, 10, (n / 100).max(1)] {
            for h in [10_usize, 100, 1_000, 10_000] {
                for width in [32_usize, 64] {
                    let shuffled = shuffled_indices(d)
                        .into_iter()
                        .map(|index| key(index, width))
                        .collect::<Vec<_>>();
                    let ordinal_pages = pages(shuffled);
                    let sorted_pages = pages((0..d).map(|index| key(index, width)).collect());
                    let page_count = ordinal_pages.len();
                    let target = key(d / 2, width);
                    let ordinal_selected = selected_pages(&ordinal_pages, &target);
                    let sorted_selected = selected_pages(&sorted_pages, &target);
                    let dir_bytes = directory_bytes(&sorted_pages);
                    let baseline_point_bytes = page_count * PAGE_BYTES * h;
                    let ordinal_directory_point_bytes =
                        (dir_bytes + ordinal_selected * PAGE_BYTES) * h;
                    let sorted_directory_point_bytes =
                        (dir_bytes + sorted_selected * PAGE_BYTES) * h;
                    let overhead = dir_bytes as f64 / (page_count * PAGE_BYTES) as f64 * 100.0;
                    // Sparse diff/merge discovers D keys through hash-pruned
                    // state roots and exact current-pack page back-edges. The
                    // current ordinal proof decodes every page prefix through
                    // the greatest selected page. An authenticated count next
                    // to each existing page ObjectId makes the start ordinal
                    // derivable from the commit envelope and loads only the
                    // distinct selected pages. Model deterministic uniformly
                    // distributed changed keys in a source commit of N rows.
                    let source_pages = n.div_ceil(PAGE_MEMBERS);
                    let selected_source_pages = (0..d)
                        .map(|slot| {
                            slot.saturating_mul(2)
                                .saturating_add(1)
                                .saturating_mul(source_pages)
                                / d.max(1).saturating_mul(2)
                        })
                        .collect::<BTreeSet<_>>();
                    let greatest_selected = selected_source_pages
                        .iter()
                        .next_back()
                        .copied()
                        .unwrap_or_default();
                    let baseline_prefix_pages = (greatest_selected + 2).min(source_pages);
                    let selected_page_count = selected_source_pages.len();
                    let count_savings =
                        (1.0 - selected_page_count as f64 / baseline_prefix_pages as f64) * 100.0;
                    let count_root_overhead =
                        (source_pages * 4) as f64 / (source_pages * PAGE_BYTES) as f64 * 100.0;
                    println!(
                        "{n},{d},{h},{width},{page_count},{},{ordinal_selected},{sorted_selected},{baseline_point_bytes},{ordinal_directory_point_bytes},{sorted_directory_point_bytes},{dir_bytes},{overhead:.4},{overhead:.4},0.0,{baseline_prefix_pages},{selected_page_count},{count_savings:.4},{count_root_overhead:.4}",
                        valid_nonoverlapping_ranges(&ordinal_pages),
                    );
                }
            }
        }
    }
}
