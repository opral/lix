# vscode-docs `d5badf` Markdown memory profile

Measured on 2026-07-29 with the production Wasm Component v2 Markdown plugin
and RocksDB replay storage.

## Workload

- Repository: `microsoft/vscode-docs`
- Transition:
  `15faa92b6970dc11001cded47db900aa890ca05b..d5badf95f8ab16c4deb91199dc696f2293d93554`
- Changed paths: 151
- Changed blob bytes submitted by replay: 3,276,550
- Largest changed file: `api/references/vscode-api.md`, 1,237,840 bytes
- Change in that file: one insertion and one deletion
- Wasm guest memory limit: 134,217,728 bytes (128 MiB)

The parent tree was bootstrapped before the timed transition. The replay ran
with all production semantic plugins and verified both the commit state and
the final Git tree manifest.

## Result

| Build | Peak guest linear memory | Replay result |
| --- | ---: | --- |
| Baseline | greater than 134,217,728 bytes | OOM trap |
| Borrowed-view prototype, run 1 | 102,367,232 bytes (97.63 MiB) | success |
| Borrowed-view prototype, run 2 | 102,367,232 bytes (97.63 MiB) | success |

The successful build leaves 31,850,496 bytes (30.38 MiB, 23.7%) free under
the guest limit. The two measured transition execution times were 8.95 s and
9.22 s. Timing is secondary here because the baseline cannot complete.

The baseline trap backtrace ends in UUID formatting called from
`allocate_generated_ids`, reached while processing `Document::file_changed`.

## Prototype cut

The full-parse fallback previously held or created these owned structures:

1. a materialized clone of the persistent tree;
2. an owned projection containing a clone of every snapshot;
3. a second tree rebuilt from that projection for reconciliation;
4. the parsed successor tree;
5. an owned flattened clone of the reconciled successor;
6. another projection/tree rebuild to retain the successor.

The prototype changes that pipeline to:

1. materialize the persistent tree once;
2. create a sorted identity index containing borrowed `&str` and
   `&NodeSnapshot` references;
3. parse one owned successor tree using compact transition-local identities;
4. reconcile and diff through borrowed views;
5. retain that same reconciled successor tree.

It also replaces parse-only UUIDv7 strings with one collision-free per-parse
ordinal namespace (`~0`, `~1`, ... in hexadecimal). Durable identities remain
canonical host-namespaced UUIDs; only identities that cannot cross the
component boundary use the compact representation.

This is an arena-shaped ownership cut without introducing a general-purpose
arena allocator: phases share one owned tree through borrowed indexes, and the
parsed successor becomes persistent state instead of being flattened and
rebuilt.

## Reproduction

Hydrate the target commit and its parent before measuring so partial-clone
network fetches do not contaminate the replay:

```sh
git clone --filter=blob:none --no-checkout \
  https://github.com/microsoft/vscode-docs.git /tmp/vscode-docs
git -C /tmp/vscode-docs fetch --filter=blob:limit=5m origin \
  15faa92b6970dc11001cded47db900aa890ca05b \
  d5badf95f8ab16c4deb91199dc696f2293d93554
```

Build and run the exact transition:

```sh
cargo run --release -p lix_cli -- exp git-replay \
  --repo-path /tmp/vscode-docs \
  --output-path /tmp/vscode-d5badf-replay \
  --storage rocksdb \
  --plugins all \
  --branch d5badf95f8ab16c4deb91199dc696f2293d93554 \
  --from-commit d5badf95f8ab16c4deb91199dc696f2293d93554 \
  --num-commits 1 \
  --force \
  --profile-json /tmp/vscode-d5badf-profile.json
```

Read the peak from:

```sh
jq '.commits[0].plugin_counters.guest_linear_memory_high_water_bytes' \
  /tmp/vscode-d5badf-profile.json
```

## Architectural conclusion

A full parser/semantic/storage arena is not justified by this result alone.
The immediate problem was ownership topology, not allocation API selection.
Borrowed views and direct successor retention recover more than 30 MiB of
headroom while preserving the existing semantic model.

The next arena cut should be conditional on a new profile showing that the
remaining peak is dominated by `NodeSnapshot`/`serde_json::Value`
materialization. If so, use typed arena nodes with compact `NodeId` references
and defer JSON wire serialization to changed rows only. Do not put source
bytes, parser AST, semantic nodes, and durable snapshots into one universal
arena: their lifetimes and mutation patterns differ, and coupling them would
make sparse persistent successors harder to share.
