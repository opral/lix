# W0 compile/source probes

These probes are deliberately outside the production crate. They are acceptance
fixtures for the W0 hard cut and must be run against a candidate after the
static residue verifier is green.

* `positive_descriptor.rs` is the positive Rust boundary probe. It must compile
  using only the engine-declared descriptor and opaque ForkTree object-domain
  types.
* `negative_raw_space.rs` must fail to compile because raw `SpaceId` and
  `StorageSpace` constructors are not public or available to an adapter.
* `negative_columnar_owner.rs` must fail because the deleted columnar physical
  owner and its storage space are absent.
* `negative_tracked_changelog.rs` must fail because legacy tracked/changelog
  readers and physical spaces are absent.
* `negative_native_exports.ts` must fail type-checking when the removed native
  filesystem/space import methods are named.

The negative probes are source/compile-fail assertions, not compatibility
shims. A candidate that makes one compile is rejected. Native registration is
checked by the same residue verifier over the binding source and by the
negative TypeScript probe; no alternate registry is permitted.
