# Production v2 TSV plugin task

Use these two values verbatim with the AX-eval harness. The harness combines
them into its canonical single-line prompt; do not include this file as docs.

```text
task: Implement and test a Wasm Component v2 plugin that round-trips a two-column TSV file and emits one sparse entity upsert for a localized row edit
tool: the production Lix plugin API in this repository
```

An independent judge should require an executable new plugin crate rather than
edits to the v2 contract or the CSV reference. The submission succeeds when:

1. Its manifest selects `wasm-component-v2` API `2.0.0`, text `*.tsv` files,
   and valid schemas, including a host-allocated stable row identity.
2. Opening `key\tvalue\nalpha\tone\nbeta\ttwo\n` emits complete table/row state
   with distinct stable row IDs.
3. Changing only `two` to `TWO` preserves the `beta` row ID and emits exactly
   one complete row upsert, with no unrelated table or `alpha` change.
4. Applying that semantic change renders the exact expected TSV bytes.
5. The plugin implements cold open, warm file and entity transitions, cheap
   immutable fork, bounded permanent-EOF cursors, and retry-stable new IDs.
6. Native tests pass and the crate builds as a `wasm32-wasip2` component.
