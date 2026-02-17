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

## Trial 2: Replace tree-based containers with hash-based containers in hot detect/apply paths

- Status: Kept
- Scope:
  - `detect_changes`: `before_id_set`, `after_id_set`, removed-id dedupe, `used_ids`
  - `apply_changes`: `line_by_id`, `seen_line_change_ids`, document id dedupe set
- Gate: keep only if >=10% improvement with criterion statistical significance (`p < 0.05`) on heavy scenarios.

### Baseline (`opt2_trial2_before_detect`, `opt2_trial2_before_apply`)

- `detect_changes/small_single_line_edit`: `[4.0766 µs 4.1076 µs 4.1410 µs]`
- `detect_changes/lockfile_large_create`: `[4.4598 ms 4.4912 ms 4.5352 ms]`
- `detect_changes/lockfile_large_patch`: `[13.118 ms 13.278 ms 13.426 ms]`
- `detect_changes/lockfile_large_block_move_and_patch`: `[16.487 ms 16.698 ms 16.944 ms]`
- `apply_changes/small_projection_from_empty`: `[1.6292 µs 1.6379 µs 1.6469 µs]`
- `apply_changes/small_delta_on_base`: `[2.3637 µs 2.3848 µs 2.4126 µs]`
- `apply_changes/lockfile_projection_from_empty`: `[8.5040 ms 8.7429 ms 8.9437 ms]`
- `apply_changes/lockfile_delta_patch_on_base`: `[7.7328 ms 7.8221 ms 7.9248 ms]`
- `apply_changes/lockfile_delta_move_patch_on_base`: `[10.179 ms 10.362 ms 10.581 ms]`

### Candidate (after change, compared with baseline)

- `detect_changes/small_single_line_edit`: `[4.2534 µs 4.2937 µs 4.3411 µs]`, change `[+2.2862% +3.6716% +4.9566%]`, `p=0.00` (regression)
- `detect_changes/lockfile_large_create`: `[4.2217 ms 4.2425 ms 4.2666 ms]`, change `[-6.6074% -5.5571% -4.4974%]`, `p=0.00`
- `detect_changes/lockfile_large_patch`: `[9.5969 ms 9.6828 ms 9.7666 ms]`, change `[-26.555% -24.679% -22.342%]`, `p=0.00`
- `detect_changes/lockfile_large_block_move_and_patch`: `[12.228 ms 12.400 ms 12.633 ms]`, change `[-26.816% -25.622% -24.450%]`, `p=0.00`
- `apply_changes/small_projection_from_empty`: `[1.7634 µs 1.7834 µs 1.8033 µs]`, change `[+7.4681% +8.8452% +10.355%]`, `p=0.00` (regression)
- `apply_changes/small_delta_on_base`: `[2.4915 µs 2.5153 µs 2.5415 µs]`, change `[+1.3229% +3.1558% +4.9414%]`, `p=0.00` (regression)
- `apply_changes/lockfile_projection_from_empty`: `[5.8853 ms 5.9453 ms 6.0276 ms]`, change `[-32.204% -30.584% -28.808%]`, `p=0.00`
- `apply_changes/lockfile_delta_patch_on_base`: `[6.3841 ms 6.6080 ms 7.0324 ms]`, change `[-21.667% -16.846% -9.6793%]`, `p=0.00`
- `apply_changes/lockfile_delta_move_patch_on_base`: `[8.1249 ms 8.2146 ms 8.3025 ms]`, change `[-22.323% -21.005% -19.561%]`, `p=0.00`

### Decision

- Keep. Heavy lockfile scenarios improved by ~20–32% with strong significance; small-input regressions are not representative for target workload.
