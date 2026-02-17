# plugin-text-lines Optimization Trials

This log records each optimization trial with baseline vs candidate benchmark results.

## Trial 1: Remove `after_split.clone()` + canonical-after reparse in `detect_changes`

- Status: Kept
- Scope: `detect_changes` (`parse_after_lines_with_histogram_matching`)
- Gate: keep only if >=10% improvement with criterion statistical significance (`p < 0.05`) on lockfile-heavy scenarios.

### Baseline (`opt2_trial1_before`)

- `detect_changes/small_single_line_edit`: `[4.6213 µs 4.6882 µs 4.8055 µs]`
- `detect_changes/lockfile_large_create`: `[4.3500 ms 4.3744 ms 4.4013 ms]`
- `detect_changes/lockfile_large_patch`: `[15.261 ms 16.380 ms 18.123 ms]`
- `detect_changes/lockfile_large_block_move_and_patch`: `[18.856 ms 19.021 ms 19.197 ms]`

### Candidate (after change, compared with `--baseline opt2_trial1_before`)

- `detect_changes/small_single_line_edit`: `[3.9326 µs 3.9489 µs 3.9684 µs]`, change `[-20.312% -17.931% -15.898%]`, `p=0.00`
- `detect_changes/lockfile_large_create`: `[4.3072 ms 4.3346 ms 4.3697 ms]`, change `[-2.2613% -1.2321% -0.2218%]`, `p=0.03` (noise)
- `detect_changes/lockfile_large_patch`: `[12.521 ms 12.711 ms 12.886 ms]`, change `[-23.337% -18.851% -15.612%]`, `p=0.00`
- `detect_changes/lockfile_large_block_move_and_patch`: `[16.044 ms 16.150 ms 16.260 ms]`, change `[-15.385% -14.138% -12.784%]`, `p=0.00`

### Decision

- Keep. This clears the >10% statistically significant threshold on the two lockfile-heavy update scenarios.
