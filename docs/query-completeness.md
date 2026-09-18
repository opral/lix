# Query completeness in partial replicas

An SQL read may return an empty result only after Lix has proved that the
requested serving scope is complete for the pinned branch basis. Resident rows,
an index, a read interest, or a successful DataFusion plan are not proofs: all
of them can describe a sparse local view.

The Rust hot-state reader owns this invariant. A partial read first resolves
the admitted branch control. If the lazy current-base projection is present,
the existing indexed path remains the fast path. If it is absent, the reader
traverses the authenticated branch head. That traversal either produces the
complete rows for the pinned basis or returns an existing typed native
dependency demand. The SQL retry loop can then hydrate and rerun the same
statement. JavaScript only receives the result after this loop succeeds.

This preserves the three meaningful states for a partial scope:

- **covered:** local reads are served with no network round trip;
- **unknown:** the read remains pending behind a typed native demand;
- **proven empty:** an authenticated traversal found no matching row.

Missing branch admission is also fail-closed. A requested branch that is not
present in the serving snapshot cannot be projected away and interpreted as an
empty relation. The read returns the existing partial-scope error instead.

The fallback is deliberately a serving-reader concern rather than a UI patch.
It applies to path indexes, exact lookups, scans, joins, and aggregates because
all of those routes consume the same hot-state reader. Coverage is tied to the
branch control and its immutable head; a publication or branch change selects
a new basis and causes the normal coherent-read retry.

Performance is measured with existing native-read and SQL profile counters.
Warm covered reads must retain their zero-demand fast path. Cold reads are
allowed to pay only for the native objects needed to prove the requested scope;
tests must assert demand counts and correctness rather than wall-clock limits.
