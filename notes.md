# plugin-md-v2 benchmark baseline

Run timestamp: 2026-02-17 21:32:54 PST  
Command: `cargo bench -p plugin_md_v2 --bench detect_changes`

| Benchmark | Lower | Median | Upper |
| --- | --- | --- | --- |
| `detect_changes/small` | 35.613 µs | 36.104 µs | 36.632 µs |
| `detect_changes/medium` | 4.3458 ms | 4.3703 ms | 4.3904 ms |
| `detect_changes/large` | 65.325 ms | 66.236 ms | 67.255 ms |
| `detect_changes_with_state_context/medium` | 2.6057 ms | 2.6400 ms | 2.6745 ms |
| `detect_changes_with_state_context/large` | 38.411 ms | 38.629 ms | 38.895 ms |

## after imara-diff localization

Run timestamp: 2026-02-17 21:37:16 PST  
Command: `cargo bench -p plugin_md_v2 --bench detect_changes`

Change implemented: replaced global fuzzy candidate scan with `imara-diff` hunk-localized candidate ranges on unmatched `node_type` sequences, with global fallback.

| Benchmark | Lower | Median | Upper |
| --- | --- | --- | --- |
| `detect_changes/small` | 34.906 µs | 35.124 µs | 35.311 µs |
| `detect_changes/medium` | 4.0354 ms | 4.0843 ms | 4.1452 ms |
| `detect_changes/large` | 65.634 ms | 67.319 ms | 69.253 ms |
| `detect_changes_with_state_context/medium` | 2.4658 ms | 2.5023 ms | 2.5320 ms |
| `detect_changes_with_state_context/large` | 35.740 ms | 36.232 ms | 36.709 ms |

| Benchmark | Baseline median | After median | Delta |
| --- | --- | --- | --- |
| `detect_changes/small` | 36.104 µs | 35.124 µs | -2.71% |
| `detect_changes/medium` | 4.3703 ms | 4.0843 ms | -6.54% |
| `detect_changes/large` | 66.236 ms | 67.319 ms | +1.64% (not significant in Criterion output) |
| `detect_changes_with_state_context/medium` | 2.6400 ms | 2.5023 ms | -5.22% |
| `detect_changes_with_state_context/large` | 38.629 ms | 36.232 ms | -6.21% |

## optimization 1 (rejected)

Run timestamp: 2026-02-17 21:43:44 PST  
Change attempted: early exit after full assignment, lazy text normalization caches, remove per-candidate full sort (top-2 scan).  
Result: statistically significant improvements, but no benchmark exceeded the required >10% speedup threshold with significance.

| Benchmark | Lower | Median | Upper | Criterion change vs previous |
| --- | --- | --- | --- | --- |
| `detect_changes/small` | 33.233 µs | 33.779 µs | 34.297 µs | improved, -3.7474% median |
| `detect_changes/medium` | 3.9599 ms | 4.0072 ms | 4.0644 ms | improved, -3.2726% median |
| `detect_changes/large` | 62.339 ms | 63.130 ms | 63.971 ms | improved, -3.1065% median |
| `detect_changes_with_state_context/medium` | 2.3598 ms | 2.3836 ms | 2.4119 ms | improved, -9.3053% median |
| `detect_changes_with_state_context/large` | 36.360 ms | 36.807 ms | 37.208 ms | improved, -4.9732% median |

## optimization 2 (kept)

Run timestamp: 2026-02-17 (after optimization 1)  
Change implemented: switched hot-path matching fingerprints from AST JSON serialization to normalized block-markdown fingerprints; added semantic AST compare only for changed `paragraph`/`code` blocks to preserve normalization-sensitive behavior.

| Benchmark | Lower | Median | Upper | Criterion change vs previous |
| --- | --- | --- | --- | --- |
| `detect_changes/small` | 32.257 µs | 32.602 µs | 32.977 µs | improved, -2.7730% median |
| `detect_changes/medium` | 3.9847 ms | 4.0565 ms | 4.1350 ms | no significant change |
| `detect_changes/large` | 54.455 ms | 55.347 ms | 56.178 ms | improved, -12.328% median |
| `detect_changes_with_state_context/medium` | 2.4109 ms | 2.4649 ms | 2.5134 ms | regressed, +3.3819% median |
| `detect_changes_with_state_context/large` | 32.125 ms | 32.601 ms | 32.995 ms | improved, -12.562% median |

Result: kept. This optimization produced statistically significant >10% speedups on:
- `detect_changes/large` (CI entirely below -10%)
- `detect_changes_with_state_context/large` (CI entirely below -10%)

## optimization 3 (rejected)

Run timestamp: 2026-02-17 21:49:19 PST  
Change attempted: semantic AST compare for `paragraph` only when markdown looked normalization/escape-sensitive (CRLF, non-ASCII, backslash, hard-break spaces).

| Benchmark | Lower | Median | Upper | Criterion change vs previous |
| --- | --- | --- | --- | --- |
| `detect_changes/small` | 30.861 µs | 31.445 µs | 32.007 µs | improved, -4.6637% median |
| `detect_changes/medium` | 3.6087 ms | 3.6504 ms | 3.7064 ms | improved, -7.8256% median |
| `detect_changes/large` | 53.758 ms | 54.894 ms | 56.114 ms | no significant change |
| `detect_changes_with_state_context/medium` | 2.2273 ms | 2.2501 ms | 2.2708 ms | improved, -8.7009% median |
| `detect_changes_with_state_context/large` | 30.545 ms | 31.283 ms | 31.782 ms | no significant change |

Result: rejected. No benchmark showed a statistically significant >10% speedup.

## optimization 4 (kept)

Run timestamp: 2026-02-17 21:55:04 PST  
Baseline method: `cargo bench -p plugin_md_v2 --bench detect_changes -- --save-baseline opt2`, then compared with `--baseline opt2`.  
Change implemented: fast-path normalization in `normalize_text_for_fingerprint` to skip expensive Unicode normalization when text is already ASCII/NFC and has no CR.

| Benchmark | Lower | Median | Upper | Criterion change vs `opt2` |
| --- | --- | --- | --- | --- |
| `detect_changes/small` | 28.729 µs | 29.051 µs | 29.350 µs | improved, -11.147% median |
| `detect_changes/medium` | 3.4444 ms | 3.4684 ms | 3.4938 ms | improved, -11.253% median |
| `detect_changes/large` | 51.596 ms | 52.310 ms | 53.081 ms | improved, -5.0069% median |
| `detect_changes_with_state_context/medium` | 2.0292 ms | 2.0583 ms | 2.0884 ms | improved, -16.693% median |
| `detect_changes_with_state_context/large` | 28.934 ms | 29.442 ms | 29.793 ms | improved, -11.142% median |

Result: kept. Benchmarks with confidence interval entirely beyond 10% improvement:
- `detect_changes/medium` (CI: -12.213% .. -10.310%)
- `detect_changes_with_state_context/medium` (CI: -17.992% .. -15.359%)
