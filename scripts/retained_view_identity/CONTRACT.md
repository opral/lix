# Retained-view identity oracle

This package is test/report-only. It does not add a production token, cache,
reader, or fallback. Its purpose is to make the next R2/R3 immutable source
review deterministic.

## Required identity

The candidate must derive one stable logical token from all of these
authenticated values:

1. the storage read epoch/version (the lifetime of the retained read);
2. repository identity;
3. raw global selector bytes and its authenticated root;
4. raw branch selector bytes and its authenticated root;
5. selected snapshot/commit identity.

The token is a value identity. Temporary wrapper addresses, `Arc` allocation,
reader concrete type, or facade clone identity must not participate.

One operation-owned retained read may construct a raw control reader, packed
hot reader, and history reader. Those readers must carry the same token and
must not call `begin_read`, refresh, detach, extract, or silently reacquire a
view. A cross-read, reopen, repository, global selector/root, branch
selector/root, or snapshot/commit mismatch must fail closed.

Unknown object/domain/space values must return a typed error. They may not fall
through to a raw object-space reader or install an index. A failed proof must
leave the operation-local token/index absent. At most one pack decode and one
closure proof are permitted per retained view; subsequent member hits are
lookups under that token. No cross-view, process-global, reopen, or persisted
cache is accepted.

## Gate roles

- `model.rs` is a dependency-free positive/negative executable model. It
  covers same-view raw/packed/history clones, pointer-independent wrappers,
  every identity component mismatch, reopen/epoch mismatch, unknown domain,
  and no-install-on-failure.
- `source_gate.py` is a candidate-parametric structural pre-filter. It takes
  arbitrary base/candidate refs, checks the whole changed-path allowlist,
  searches the candidate ForkTree source for all identity components, checks
  one shared raw+packed operation owner, rejects reader-local acquisition and
  fallback/cache/compatibility paths, and checks validate-before-install.
  `GREEN` is necessary but not sufficient for manual call-graph review.
- `gate.sh` compiles/runs the model with warnings denied, then runs the source
  gate. It accepts `expected-source=red|green`; the immutable current-main
  oracle is expected RED, while a future candidate is admitted only with
  `green` and independent backend/corruption evidence.

The source gate deliberately limits candidate production changes to
`packages/lix/src/forktree/`; the future reviewer may tighten this allowlist,
never widen it silently.
