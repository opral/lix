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

## Trial 3: Replace SHA-1 line-id hash with xxh3-128

- Status: Rejected
- Scope: line id generation in `parse_lines_with_ids*` and inserted-id derivation path.
- Gate: keep only if >=10% statistically significant improvement.

### Baseline (`opt2_trial3_before_detect_seq`, `opt2_trial3_before_apply_seq`)

- `detect_changes/small_single_line_edit`: `[4.3072 µs 4.3299 µs 4.3583 µs]`
- `detect_changes/lockfile_large_create`: `[4.2953 ms 4.3322 ms 4.3666 ms]`
- `detect_changes/lockfile_large_patch`: `[9.9072 ms 9.9966 ms 10.079 ms]`
- `detect_changes/lockfile_large_block_move_and_patch`: `[12.758 ms 12.884 ms 13.001 ms]`
- `apply_changes/small_projection_from_empty`: `[1.8145 µs 1.8295 µs 1.8499 µs]`
- `apply_changes/small_delta_on_base`: `[2.5331 µs 2.5547 µs 2.5832 µs]`
- `apply_changes/lockfile_projection_from_empty`: `[6.3973 ms 6.4741 ms 6.5495 ms]`
- `apply_changes/lockfile_delta_patch_on_base`: `[6.1933 ms 6.2938 ms 6.4040 ms]`
- `apply_changes/lockfile_delta_move_patch_on_base`: `[7.9721 ms 8.2928 ms 8.5970 ms]`

### Candidate (after change, compared with baseline)

- `detect_changes/small_single_line_edit`: change `[-5.9964% -4.0321% -1.4819%]`, `p=0.00`
- `detect_changes/lockfile_large_create`: change `[-8.4735% -7.4754% -6.1115%]`, `p=0.00`
- `detect_changes/lockfile_large_patch`: change `[-7.1955% -6.0324% -4.6124%]`, `p=0.00`
- `detect_changes/lockfile_large_block_move_and_patch`: change `[-8.1033% -6.2394% -4.3950%]`, `p=0.00`
- `apply_changes/small_projection_from_empty`: change `[-6.3195% -4.2402% -2.4693%]`, `p=0.00`
- `apply_changes/small_delta_on_base`: change `[-9.4157% -7.7505% -6.1694%]`, `p=0.00`
- `apply_changes/lockfile_projection_from_empty`: change `[-9.4030% -7.8370% -6.4333%]`, `p=0.00`
- `apply_changes/lockfile_delta_patch_on_base`: change `[-12.220% -9.7753% -7.0239%]`, `p=0.00`
- `apply_changes/lockfile_delta_move_patch_on_base`: change `[-11.079% -8.3382% -5.7167%]`, `p=0.00`

### Decision

- Reject. Good gains, but this trial does not consistently clear the >10% gate on the key lockfile scenarios.

## Trial 4: Reduce redundant ID cloning/comparison allocations in `detect_changes`

- Status: Rejected
- Scope: remove `before_ids` vector and compare document ordering directly from parsed line vectors.
- Gate: keep only if >=10% statistically significant improvement.

### Baseline (`opt2_trial4_before_detect`)

- `detect_changes/small_single_line_edit`: `[4.3262 µs 4.3541 µs 4.3934 µs]`
- `detect_changes/lockfile_large_create`: `[4.2517 ms 4.2963 ms 4.3586 ms]`
- `detect_changes/lockfile_large_patch`: `[9.7997 ms 9.8724 ms 9.9471 ms]`
- `detect_changes/lockfile_large_block_move_and_patch`: `[12.824 ms 13.066 ms 13.399 ms]`

### Candidate (after change, compared with baseline)

- `detect_changes/small_single_line_edit`: change `[-5.0641% -3.3641% -0.9528%]`, `p=0.00` (noise)
- `detect_changes/lockfile_large_create`: change `[+2.7487% +4.3189% +6.0364%]`, `p=0.00` (regression)
- `detect_changes/lockfile_large_patch`: change `[-0.8962% +0.5743% +2.1780%]`, `p=0.49` (no change)
- `detect_changes/lockfile_large_block_move_and_patch`: change `[-4.2537% -2.2307% -0.4501%]`, `p=0.03` (noise)

### Decision

- Reject. Does not meet the >10% threshold and regresses one lockfile scenario.

## Trial 5: Use fixed SHA-1 fingerprints (`[u8;20]`) as occurrence keys (no per-line key `Vec<u8>` allocation)

- Status: Kept
- Scope:
  - Replace `HashMap<Vec<u8>, u32>` occurrence keys with `HashMap<[u8;20], u32>`.
  - Compute a line fingerprint once from `(content, ending)` and reuse it for both map key and entity-id hash text.
- Gate: keep only if >=10% statistically significant improvement.

### Baseline (`opt2_trial5_before_detect`)

- `detect_changes/small_single_line_edit`: `[4.4274 µs 4.5174 µs 4.5931 µs]`
- `detect_changes/lockfile_large_create`: `[4.3711 ms 4.4275 ms 4.5040 ms]`
- `detect_changes/lockfile_large_patch`: `[10.082 ms 10.227 ms 10.375 ms]`
- `detect_changes/lockfile_large_block_move_and_patch`: `[12.919 ms 13.063 ms 13.198 ms]`

### Candidate (after change, compared with baseline)

- `detect_changes/small_single_line_edit`: change `[-7.3782% -5.1223% -2.4092%]`, `p=0.00`
- `detect_changes/lockfile_large_create`: change `[-19.978% -16.293% -12.978%]`, `p=0.00`
- `detect_changes/lockfile_large_patch`: change `[-10.517% -9.1114% -7.6749%]`, `p=0.00`
- `detect_changes/lockfile_large_block_move_and_patch`: change `[-13.004% -10.230% -8.3076%]`, `p=0.00`

### Additional signal (apply bench, compared against existing `opt2_trial3_before_apply_seq`)

- `apply_changes/small_projection_from_empty`: no meaningful change
- `apply_changes/small_delta_on_base`: small improvement (`~4%`)
- `apply_changes/lockfile_projection_from_empty`: no meaningful change
- `apply_changes/lockfile_delta_patch_on_base`: improvement centered below 10% (wide interval)
- `apply_changes/lockfile_delta_move_patch_on_base`: improvement centered below 10%

### Decision

- Keep. Detect path clears >10% with strong significance on core lockfile-heavy scenarios.

## Trial 6: Manual line-snapshot codec (replace serde JSON encode/decode for `text_line`)

- Status: Kept
- Scope:
  - `serialize_line_snapshot`: manual fixed-shape JSON string construction.
  - `parse_line_snapshot`: manual fixed-shape field extraction + explicit ending literal parsing.
- Gate: keep only if >=10% statistically significant improvement.

### Baseline (`opt2_trial6_before_detect`, `opt2_trial6_before_apply`)

- `detect_changes/small_single_line_edit`: `[4.4692 µs 4.7584 µs 5.1401 µs]`
- `detect_changes/lockfile_large_create`: `[3.9347 ms 3.9776 ms 4.0459 ms]`
- `detect_changes/lockfile_large_patch`: `[9.4057 ms 9.7089 ms 10.122 ms]`
- `detect_changes/lockfile_large_block_move_and_patch`: `[11.919 ms 12.038 ms 12.146 ms]`
- `apply_changes/small_projection_from_empty`: `[1.7957 µs 1.8082 µs 1.8231 µs]`
- `apply_changes/small_delta_on_base`: `[2.5264 µs 2.6428 µs 2.8122 µs]`
- `apply_changes/lockfile_projection_from_empty`: `[6.2112 ms 6.3086 ms 6.4214 ms]`
- `apply_changes/lockfile_delta_patch_on_base`: `[5.4396 ms 5.5035 ms 5.6065 ms]`
- `apply_changes/lockfile_delta_move_patch_on_base`: `[7.2386 ms 7.5865 ms 7.9813 ms]`

### Candidate (after change, compared with baseline)

- `detect_changes/small_single_line_edit`: change `[-20.885% -14.844% -8.6747%]`, `p=0.00`
- `detect_changes/lockfile_large_create`: change `[-18.873% -15.105% -11.701%]`, `p=0.00`
- `detect_changes/lockfile_large_patch`: change `[-12.667% -8.2534% -3.9221%]`, `p=0.00`
- `detect_changes/lockfile_large_block_move_and_patch`: no significant change
- `apply_changes/small_projection_from_empty`: change `[-14.398% -13.119% -11.585%]`, `p=0.00`
- `apply_changes/small_delta_on_base`: change `[-20.005% -14.303% -9.5417%]`, `p=0.00`
- `apply_changes/lockfile_projection_from_empty`: change `[-14.331% -12.842% -11.359%]`, `p=0.00`
- `apply_changes/lockfile_delta_patch_on_base`: change within noise threshold
- `apply_changes/lockfile_delta_move_patch_on_base`: no significant change

### Decision

- Keep. This gives clear >10% wins on several detect/apply paths with strong statistical significance.
