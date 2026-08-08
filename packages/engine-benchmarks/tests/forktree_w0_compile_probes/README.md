# W0 compile/source probes

These probes are deliberately outside the production crate. They are
acceptance fixtures for the W0 hard cut and must be run against a candidate
after the static residue verifier is green. The runner is
`scripts/forktree_w0_compile_probes.sh`; it creates temporary dependency
crates and never edits this repository.

* `positive_descriptor.rs` is the positive Rust boundary probe. It imports the
  actual public `StorageSpace`/`ValueSemantics` types and inspects a descriptor;
  it does not forge one. The benchmark test's `--no-run` is the additional
  positive compile gate for the internal opaque ForkTree boundary.
* `negative_raw_space.rs` must fail because raw `SpaceId` and `StorageSpace`
  constructors are not public or available to an adapter.
* `negative_columnar_owner.rs` imports the real public
  `StorageAdapterRead`/`StorageSpace` surface and must fail with the expected
  missing `load_columnar_row_group` diagnostic; the deleted columnar physical
  owner and its storage space are absent.
* `negative_tracked_changelog.rs` imports the real public
  `StorageAdapterRead` surface and must fail with the expected missing
  `load_commit_state_manifest`, `load_tracked_state`, and
  `load_branch_head_control` diagnostics; legacy tracked/changelog and
  selector readers and physical spaces are absent.
* `negative_binary_cas_owner.rs` imports the real public storage/read surface
  and must fail with the expected missing `load_binary_cas_manifest`
  diagnostic; the deleted binary-CAS owner is not a consumer API.
* `negative_native_exports.ts` imports the actual JS SDK `LocalFilesystem` and
  `LixBinding` declarations and must fail when removed filesystem members are
  named. It does not declare those members itself.

The negative probes are source/compile-fail assertions, not compatibility
shims. The runner validates both the expected diagnostic code and the
attempted removed symbol, so an unrelated nonzero exit is not a pass. A
candidate that makes one compile, or emits a different diagnostic, is
rejected. Native registration is checked by the runner and full-workspace residue verifier over the actual
`packages/js-sdk/native/napi.rs` source and by the negative TypeScript probe;
no alternate registry is permitted.
