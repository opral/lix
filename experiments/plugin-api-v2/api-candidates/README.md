# Plugin API AX candidates

This crate contains five compileable SDK facades for an API-usability
evaluation. It is not a production SDK and has no runtime implementation.

- `current_v1`: current stateless complete state/blob control.
- `candidate_a`: immutable persistent document, complete blob input/output.
- `candidate_b`: immutable persistent document, splice input/patch output, with
  complete-source fallbacks.
- `candidate_b_refined`: candidate B after AX and correctness review: explicit
  file/entity cold constructors; before/after descriptors for rename and media
  type changes; merge groups; no guest `Send`/`Sync`; and
  `Upsert(complete entity, typed effect) | Delete(entity key)`. Durable entities
  are exposed through bounded host-backed pages, while input and output byte
  ranges and oversized entity snapshots can stay lazy. The allocator takes an
  explicit schema, composite-PK scope, and deterministic ordinal, so retry
  behavior does not depend on call order. Semantic change output is inline for
  ordinary sparse edits and bounded/paged for broad edits or initial import, so
  a large file never has to accumulate every complete upsert in guest memory.
  Entity input is one stateful permanent-EOF cursor; the host validates change
  keys across every output page, caps inline edits before guest lowering, and
  permits warm descriptor changes only when plugin key/generation are unchanged.
  Renderer deltas use a separate stateful `EntityChangeSource`; its complete
  fallback is the transaction-local prospective after-state. Paged renderer
  edits are validated across page boundaries against one accepted base.
- `candidate_c`: pure reducer with a copied opaque checkpoint.
- `candidate_d`: host-owned transactional private KV context.

The first five candidates use the shared evaluation types in `types.rs`; the
refined candidate deliberately makes the post-review contracts explicit in its
own facade. Its WIT lowering additionally binds every source/output page to one
aggregate transition budget: record, page, page-count, total-byte, and deadline
limits apply across the top-level call and cursor draining and are never renewed
per `read`/`next`. Evaluation tasks live in `../ax-eval/tasks`. Implementations
must run as Wasm in the performance prototype; this facade crate only isolates
author comprehension.
