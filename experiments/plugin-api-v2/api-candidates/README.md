# Plugin API AX candidates

This crate contains five compileable SDK facades for an API-usability
evaluation. It is not a production SDK and has no runtime implementation.

- `current_v1`: current stateless complete state/blob control.
- `candidate_a`: immutable persistent document, complete blob input/output.
- `candidate_b`: immutable persistent document, splice input/patch output, with
  complete-source fallbacks.
- `candidate_b_refined`: candidate B with the AX-driven semantic change model:
  `Upsert(complete entity, typed effect) | Delete(entity key)`. This prevents
  order changes from disappearing into optional generic metadata.
- `candidate_c`: pure reducer with a copied opaque checkpoint.
- `candidate_d`: host-owned transactional private KV context.

All candidates use the same entities, changes, retry-stable ID allocator, and
base-relative splice rules from `types.rs`. Evaluation tasks live in
`../ax-eval/tasks`. Implementations must run as Wasm in the performance
prototype; this facade crate only isolates author comprehension.
