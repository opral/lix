# ForkTree W5/R7 GC + reachability oracle

Status: TEST/REPORT ONLY. This package rebinds prior package head
`6487170dfa11b24411dbbd73e3c003439072df09` onto exact approved head
`705440f55eccba9e2d55c0951d6a684737005d76`. It contains no production source,
Cargo wiring, adapter behavior, runtime, or PR mutation.

## Contract

GC/reachability progress must use one authenticated coherent read, one epoch
fence, one progress update, and one owner-selector CAS in the existing
transaction publication batch. No raw `StorageSpace` forge, legacy
GC/reachability reader/writer, alternate durable authority, fallback,
compatibility path, or independent publication commit is allowed.

The focused matrix covers exact 65-entry processing as 64 plus suffix,
blocked-head one-token debt with no retry spin and release cadence,
publication-first and GC-first races, poisoned cursor plus fresh exclusive-key
restart, upload completion/abort, branch/history/diff, checkpoint, shared and
final roots, missing/malformed/wrong-object/cyclic edges, cold reopen, and
explicit legacy-space/symbol negatives.

The standalone model is intentionally un-wired. A future compile-green
candidate must bind the same assertions to one transaction plan on Memory,
RocksDB, and SlateDB, then qualify cold reopen and settled physical effects.

## Source RED calibration

The exact-head/tree-guarded verifier scans only production Rust/TS/JS/C++ roots
for old GC/reachability spaces, codecs, readers, writers, raw storage-space
construction, and independent publication seams:

```text
node test-reports/w5-r7-gc-reachability-705/verify_source_contract.mjs \
  --root "$PWD"
```

Against exact 705 it exits `1` with 168 forbidden residues. Captured output
SHA-256: `cf15d78de7bd894a3e5dabffc0614aae8780300d3959c8bd1700efcf72c5f1f0`.
This is the expected compiler-red source result; no runtime or build was run.
