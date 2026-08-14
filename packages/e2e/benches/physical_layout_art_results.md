# EXP-ART-01 — authenticated crit-bit versus C2 slotted pages

Verdict: **correctness-qualified NO-WIN**. The crit-bit candidate does not pass the
lexicographic OLTP gate. Its 54.3% update-one and 78.9% sparse-diff reductions do
not compensate for critical range, scan, missing-point, memory, and physical-byte
regressions. VCS cannot promote a candidate with material OLTP regressions.

## Immutable comparator and scope

- Approved C2 parent: `aecf821658644f95724f22e3d29deda04573fdf1`
  (tree `5e504b7ecf2e0d080dd0c79f407ea72387c8279b`).
- Both geometries use identical canonical typed PK bytes and identical Schema-v1
  non-PK tuple bytes. PK bytes occur only in traversal keys.
- Integer, UUID, text, and composite PK distributions were measured at 1K and
  10K. Integer, UUID, and text additionally completed 50K and 100K. The
  coordinator stopped redundant composite 50K/100K cells after the result was
  decisive.
- Each qualified model cell has 20 measured repetitions. Every process had a
  1,200-second cap. C2 insertion is honestly marked as a full-rebuild model and
  excluded from OLTP aggregation.
- RocksDB and SlateDB cold-reopen closure ran at 1K for every PK and both
  geometries. No production code, compatibility reader, fallback, cache, or
  second authority was added.

## Correctness

- Logical result digests match between C2 and crit-bit for every common operation
  and qualified cell.
- Both geometries reject missing root/child, ObjectId substitution, schema
  mismatch, compression-tag corruption, oversized declared length, parent-edge
  corruption, wrong domain, and branch-root-link corruption (nine controls per
  geometry/PK).
- Mutation is checked against an independently rebuilt canonical authenticated
  root. Crit-bit decision bits are canonical and parent edges authenticate bounds,
  row counts, and children.
- RocksDB and SlateDB persist, flush, drop, reopen, and reproduce exact roots and
  logical digests for all 1K closure cells.

## Performance scorecard

Ratios are crit-bit / C2. Values below 1.0 favor crit-bit. Aggregate ratios are
geometric means over 14 common PK/size cells; RSS is the worst observed ratio.

| Operation | Cells | p50 wall | p95 wall | p50 CPU | max RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| point | 14 | 1.076x | 1.049x | 1.071x | 1.734x |
| missing point | 14 | 1.811x | 1.732x | 1.806x | 1.734x |
| update one | 14 | 0.457x | 0.461x | 0.456x | 1.734x |
| mutate 1% | 14 | 1.158x | 1.143x | 1.158x | 1.734x |
| range 100 | 14 | 5.200x | 4.937x | 5.187x | 1.734x |
| full scan | 14 | 6.289x | 6.243x | 6.289x | 1.734x |
| hash diff D=1 | 14 | 0.211x | 0.220x | 0.210x | 1.734x |
| hash diff D=10 | 14 | 0.293x | 0.302x | 0.293x | 1.734x |
| hash diff D=1% | 14 | 0.565x | 0.581x | 0.565x | 1.734x |

The selected point/missing/update/1%-mutation OLTP aggregate is 1.008x
(+0.8%), but that neutral aggregate hides disqualifying 5.2x range and 6.3x scan
regressions. The VCS aggregate is 0.327x (-67.3%), but VCS is considered only
when OLTP has no material regression.

Crit-bit uses 21.10x as many authenticated objects, 3.48x serialized object bytes,
and 4.54x tree height. At 1K, settled bytes are 2.91–3.17x C2 on RocksDB and
4.65–4.97x on SlateDB; cold-reopen wall is also materially worse. Byte savings
therefore do not exist and cannot influence the verdict.

## Evidence

- Qualified model CSV SHA-256:
  `ca81fed55df51996bf6497d3aa42024b48e82b717d9ca9486a56c8ff93feeb3f`
- Summary CSV SHA-256:
  `13f1b5816c6da6ba0e6ff336da0fb54bfa606951e841abb962b737e735d35bc7`
- Rocks/Slate closure CSV SHA-256:
  `673f7dfc038ca8ed9aeed260ebf82c7da8643c57c50fe66cef9dd832618dd4bd`
- Benchmark source SHA-256:
  `493c9fd0d8de3e1469d4e57bf33e1957889026349e57238dd1be2f26c5aed4f3`
- Analyzer source SHA-256:
  `7c4af7aecad720d801f6402455aea65455884b5388653c8bf8e1df3b8fcac019`
- Release executable SHA-256:
  `fe5f79792b39193c203a50e23bc85b5bb9bc6b62d24a98741800f1d7987a7183`

The first independent source review found five blockers. After correction, the
same reviewer approved the sole-authority decode path, C2 size policy, canonical
crit-bit verification, deletion-aware diff, and corruption controls. The final
performance review is recorded separately in the external evidence directory.
