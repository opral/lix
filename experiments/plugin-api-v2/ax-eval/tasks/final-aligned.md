# Final-aligned paging and pre-call validation task

Implement and test a minimal line/CSV-like plugin against the frozen refined
Candidate B facade. Do not modify the API facade or WIT contract.

Acceptance requirements:

1. `open_file` must return `EntityChangeOutput::Paged` for an input containing
   at least 200,000 rows. Its `ChangePageReader` generates complete row upserts
   incrementally from compact source/index state; it must not first collect all
   entities or merge groups. Drain it page by page, validate every page with one
   host-global `ChangeDrainValidator`, and prove exactly 200,000 unique upserts.
2. Pages must be non-empty and honor their advertised group/encoded-byte caps.
   A coupled two-change `MergeGroup` must remain intact in one page; never split
   one semantic group between pages.
3. Add a deliberately faulty two-page change reader whose second page repeats
   a key from its first page. Prove page-local validation passes but the single
   transition-wide `ChangeDrainValidator` rejects page two. Prove advancing
   after accepted EOF is also rejected.
4. `open_entities` must consume the new mutable, stateful `EntitySource` until
   its first `None`, reconstruct canonical bytes, and not require a caller-owned
   cursor. The host reader mock must make EOF permanent and reject/record any
   illegal progress behavior.
5. `entities_changed` must consume merge-resolved changes through the stateful
   `EntityChangeSource`, not an eager `EntityChanges`. Prove a broad input can
   be processed page by page. Its `current_entities` fallback must represent
   the transaction-local prospective state after applying those changes and
   before commit; test that a simple full renderer produces the prospective,
   not accepted-before, bytes.
6. Drain paged renderer edits through one `EditDrainValidator`. Prove two pages
   that are individually valid but overlap in accepted-base coordinates are
   rejected, and prove progress after accepted EOF is rejected.
7. Build a small host pre-call wrapper for warm `file_changed`. It must call
   `validate_warm_plugin_selection` and `validate_input_splices` before invoking
   plugin logic. Instrument the document so tests prove all of these are
   rejected while the plugin call count stays zero: excessive splice count,
   excessive aggregate inline bytes, overlapping/unsorted edits, before-source
   out-of-range deletion, and after-source out-of-range `AfterRange`.
8. A rename-only update with the same plugin key and generation must reach the
   plugin. Changing either the plugin key or generation must be rejected before
   plugin logic. Include both reselection cases.
9. Use complete upserts and stable primary keys. Run formatting and tests. Keep
   implementation changes inside the isolated task workspace.

The 200,000-row assertion is about bounded semantic output construction, not
just putting a prebuilt `Vec<EntityChange>` behind a cursor. Tests should expose
the reader's maximum live/generated page size so this is directly observable.
