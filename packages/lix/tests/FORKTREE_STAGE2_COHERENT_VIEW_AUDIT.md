# ForkTree Stage 2 coherent-view and deletion source audit

Status: test/report-only static gate. It executes no Lix code and changes no production source.

## Purpose

The gate reviews the corrected topology reader first, then the blob reader. It proves:

1. `open_coherent_view` performs exactly one `begin_read` at the logical read boundary.
2. `open_coherent_view_on_read` performs no refresh and stores the exact caller-owned read in `CoherentView<R>`.
3. State, catalog, commit, change, tree, value, and blob authentication remain on that view.
4. Commit topology performs no nested `begin_read`, loads no Change/member payload objects, and de-duplicates parent object loads.
5. Semantic history still loads authenticated Change objects and validates catalog owner/ordinal back-edges.
6. No deleted legacy owner module or legacy space definition reappears relative to cbe.
7. No benchmark/model implementation is copied into production.

The scanner reads Git objects directly and masks comments/literals before item-level call analysis. It supports a non-failing calibration mode and a strict candidate mode.

## Immutable calibration inputs

- Current semantic/cursor control: `a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3`, tree `9a705d36392e88d8f5f363b2b23d373deec3321d`.
- Stage 2 milestone 2: `cbe48835f6f07a21e0babf1ba16652a0c6b8a214`, tree `36ffe0ff867cd31bf52263675de2d16fc54e9b4f`.
- cbe is intentionally non-runnable; this audit never treats compilation as a source-approval substitute.

## Commands

```sh
python3 packages/lix/tests/forktree_stage2_coherent_view_audit.py \
  --repo . --baseline a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3 \
  --target a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3 \
  --profile topology --calibrate

python3 packages/lix/tests/forktree_stage2_coherent_view_audit.py \
  --repo . --baseline a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3 \
  --target cbe48835f6f07a21e0babf1ba16652a0c6b8a214 \
  --profile topology --calibrate

python3 packages/lix/tests/forktree_stage2_coherent_view_audit.py \
  --repo . --baseline cbe48835f6f07a21e0babf1ba16652a0c6b8a214 \
  --target <immutable-topology-head> --profile topology --strict

python3 packages/lix/tests/forktree_stage2_coherent_view_audit.py \
  --repo . --baseline <approved-topology-head> \
  --target <immutable-blob-head> --profile blob --strict
```

## Manual source checklist

Static automation is necessary but not sufficient. The reviewer also inspects the exact changed function bodies and call graph:

- The public logical reader acquires one `StorageRead`; no wrapper, context constructor, topology helper, history helper, or blob helper calls `begin_read` again.
- Every selector, catalog, commit, parent, state tree, value, manifest, and chunk load receives the same `CoherentView` or its borrowed `read()`.
- Topology may decode authenticated Commit objects and parent/generation facts. It must not call `load_change`, hydrate `member_change_object_ids`, validate member payloads, or load a parent twice after the batch/cache has resolved it.
- The topology regression is non-vacuous: it observes actual commit-object reads, asserts zero Change/member payload reads, and asserts one physical load per unique parent.
- History follows selected commit members through authenticated Change objects and checks ChangeCatalog owner, commit object, ordinal, and reverse member edge before output.
- Blob reads resolve a visible authenticated state edge to one manifest, then authenticate manifest domain/root, ordered chunk identity/length, and bytes on the same view. There is no BlobId-to-manifest directory or alternate binary-CAS reader.
- Missing/malformed selector, catalog, commit, parent, member, manifest, chunk, owner, chronology, or hash fails closed; absence cannot trigger a legacy fallback.
- Deleted module files/declarations, old durable spaces, old codecs, tracked/changelog/branch/head/working-diff/upload/GC owners, and benchmark models remain absent or monotonically decrease from cbe.

## Decision rule

`APPROVE` requires every strict scanner check plus the manual checklist. A scanner reduction caused by restored old modules, raw spaces, fallback readers, a second snapshot, or model code is a blocker. The corrected topology head is reviewed independently before the blob successor; evidence is not combined across mutable heads.

## Frozen calibration identities

- Scanner SHA-256: `fd4894f6a71606ea732f944e297e51a0eaadbeff6811518516b973d01795a4ec`.
- a12 calibration file SHA-256: `4a4620f7a57f8e66cb352a43b5101d9a63f33ff2b16ab7d24f85d41b397b9d1d`; canonical evidence digest inside the file: `a531382472c5f2dec2a4f2b266d48877a992c2e919a3278439d5f5b2e6713187`.
- cbe calibration file SHA-256: `98f1a8dea0114e74cce2d7511cfe4ffc2dbb68e991032eeca6c01baae33a0b29`; canonical evidence digest inside the file: `ef97c00b02f89ab4b0f21cca953c8e3581919af0343300c5d881f07b7a12732c`.
- a12 calibration observes 22 legacy physical-owner modules and all 42 legacy spaces; the cursor hard cut had already removed `storage_adapter/scan.rs`.
- cbe calibration observes zero deleted modules and nine still-unconverted writer-last legacy spaces. Its expected topology blockers are raw legacy changelog/tracked-state reads, no `CoherentView` threading through commit graph, and no ForkTree member/back-edge history path.
