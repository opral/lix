# Mixed tracked/untracked public-SQL discriminator

Status: test/report-only. This package is based directly on rejected immutable
`11442c1e0023e20307a7231d88cd557bc704fd13` and makes no production change.

## Contract

An ordinary combined entity request (`untracked == None`) must resolve one
coherent visibility result containing tracked and untracked rows. It must
preserve global-to-branch overlay precedence, overlapping identities,
explicit `NULL`, local tombstones, typed primary-key filtering, canonical
typed ordering, and `LIMIT` after identity/tombstone resolution.

The rejected 11442 direct path scans only ForkTree global/local tracked roots.
The corrected architecture must call exactly one canonical
`LiveStateReader::scan_batch`, then derive snapshot bytes or primary keys from
that returned batch. No terminal projection may acquire a second view, use a
raw adapter, or call a ForkTree-specific entity range helper.

## Frozen base and commands

- base/head: `11442c1e0023e20307a7231d88cd557bc704fd13`
- base tree: `dd4f29df5020b85359ecf1c0320880fe3b6d6fb7`
- parent: `e1666edd0b4d814a88d985086ecc5a477b5d32e6`

The frozen oracle commit is a direct test/report-only descendant of the
rejected head. Its exact immutable commit/tree/diff identity is supplied in
the qualification handoff so this report does not self-reference a commit
hash that would change when the report is amended.

Run the source gate first:

```sh
node scripts/forktree_mixed_tracked_untracked_residue_verify.mjs --root "$PWD"
```

It is expected RED on 11442 because direct entity snapshot/PK methods and the
ForkTree entity range route remain. The same gate is GREEN only after the
corrected canonical reader owner is wired.

Run the model independently of the compiler-red production crate:

```sh
rustc --edition=2021 --test -D warnings \
  packages/engine-benchmarks/tests/forktree_mixed_tracked_untracked_oracle.rs \
  -o /tmp/forktree-mixed-tracked-untracked-oracle
/tmp/forktree-mixed-tracked-untracked-oracle --nocapture --test-threads=1
```

No Cargo, adapter, or broad runtime claim is made while the candidate's
inherited compiler frontier is red.

## Discriminator behavior

The pure model has four tests. The rejected route loses the untracked
replacement for `a`, the untracked branch row for integer key `2`, the global
untracked row `e`, and the local untracked row `f`; it also returns stale
tracked winners for overlapping identities. The canonical route returns the
same expected rows for snapshot and typed-PK projections, calls its reader
exactly once per operation, preserves explicit `NULL`, hides the tombstoned
`d` by default, and includes `d` only when tombstones are requested.

The source gate rejects `ForkTreeReadFacade::scan_entity_rows`, direct entity
snapshot/PK methods, the SQL-boundary `EntitySnapshotReader`/raw adapter
owner, and the provider's second entity reader. Generic `StorageAdapterRead`
ownership and ForkTree may remain below the one canonical `LiveStateReader`;
the gate is scoped so it does not reject unrelated engine readers.

## Frozen red/green calibration

On exact rejected `11442c1e`, the initial frozen gate (v1) reported:

- source gate output SHA: `de6436835246fffd8d2c69a22d3867691b17c141ab79ee39b476cafcb8aafbf1`
- required canonical tokens missing: none
- forbidden alternate-read residues: `25`
- model binary SHA: `dd92c713dca3578c294dacc306a6257406cc2f4adadeecd3cceda87209f61fd7`
- model log SHA: `494b9b2da1db1eaa4bd67e9040c21a0fe8f43b77b467a05f00e7e2d4169bea4d`
- model result: **4/4 PASS**
- `cargo fmt --all -- --check`: PASS
- `git diff --check`: PASS

The red source result is intentional calibration: the rejected head still
contains the alternate direct entity reader. The first corrected successor
must invert this gate to GREEN while retaining the same 4/4 model result.

The scoped v2 gate used for the successor retains only the canonical projection
requirements. Its exact rejected-control output was RED with two missing
projection tokens and nine direct-path residues; output SHA
`c1f72e6a239919505e183188b11a8bccb69aa71223dd152dc2f2dfcba278b872`.

## Successor qualification update

The corrected immutable successor `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d`
removes the direct ForkTree entity-range seam. Its entity capability remains
only a terminal wrapper: each snapshot/PK operation calls the canonical
`LiveStateReader::scan_batch` once and maps the returned batch. The scoped
successor source gate is GREEN:

- output SHA: `8c68664b212ef941d941296b699110df11b898c2b7444aebde67947525cc5de7`
- required canonical tokens missing: none
- forbidden alternate-read residues: `0`

The older frozen public-SQL residue script also ran on the successor and was
RED because it requires the now-deleted `scan_direct_entity_*` and
`direct_entity_snapshot_scope` names and still scans writer/GC columnar
owners outside this read cut (3 missing tokens, 27 residues; output SHA
`9b65a658eb2789bee0b8be9658f7a047ab1885397ad39e15996f74740621a459`). This
is a stale comparator gate, not evidence of a surviving alternate SQL read
owner; the scoped mixed-domain gate above is the applicable correction gate.
