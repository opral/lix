# Frozen ForkTree Stage2 multimedia acceptance package

Verdict: **READY TO COMPILE ON THE FIRST RUNNABLE IMMUTABLE STAGE2 HEAD; NO
STAGE2 HEAD HAS BEEN EXECUTED.** This package is test-only and contains no Lix
production edit, branch, commit or PR mutation.

## Frozen identity

- Harness source SHA-256:
  `cc0d3cfb14b562b7821ca124c67cbb8ead0da7287f9e0125ba39738304a4a09e`
- Standalone Cargo manifest SHA-256:
  `652b79169275e25180acb43b52602c546209f112e97ea9059a81ed71cab8fa27`
- Compile/run manifest SHA-256:
  `2463197613022e9321e24f09f12211faae3cc85c5cb156ee6c9c7bb1667f2b4d`
- Expected-gates SHA-256:
  `bac266cdcb648b4df04cff18c088a5b757455bd8aa41abcd207c5e0134398fc4`
- Residue/format oracle SHA-256:
  `b689ad247523ee74491ea69a7f586c2831b8a63556615e6be80658af35cc9869`

The frozen residue/format oracle is green. Compilation remains deliberately
unattempted until the exact facade symbols land on a runnable immutable head.

## Comparator and model binding

- Exact comparator commit:
  `a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3`
- Exact comparator tree:
  `9a705d36392e88d8f5f363b2b23d373deec3321d`
- Comparator report SHA-256:
  `7bd123c1ea7d39bf8ecb315d21dbcb30c41235367df203b386134c02fe63d0de`
- Accepted bounded-GC model report SHA-256:
  `ad35a8fc07c51d1f93578ab05e27b47aaf54f3ddf0e60ff3156058b5528b2d77`
- Accepted mixed-lifecycle report SHA-256:
  `84968abad5ff84eccbbd3d952f6033444148b2335286cfcacb7224c323df1414`

The a12 row/encoded-byte figures are comparison facts only. The ForkTree
harness does not assume current-layout CAS spaces, a presence row, fixed tree
packing, or model object counts.

## Frozen operation order

The unchanged harness covers deterministic 64 MiB/1% image and audio plus 512
MiB/10% archive and video profiles on RocksDB and SlateDB. It performs public
multipart upload, exact/range reads, checkpoint, no-edit branch, authenticated
edit, diff, merge, checkpoint, hot undo/redo, merged-branch retirement, file
delete, retained-history branch, 64 recovery-interval checkpoints, retained
full GC, flush/drop/cold reopen, cold diff/undo/redo, final branch release, full
GC, flush/drop/final cold reopen.

It emits exact typed inventory, GC summary, phase wall/CPU/allocation/RSS,
adapter calls/bytes, Slate physical I/O and disk. It asserts zero duplicate blob
growth across branch, merge and checkpoint, retained-root survival, final exact
blob reclamation, and no upload receipt/part residue.

## Authority boundary

Only `lix::storage_bench::forktree` may observe the layout. The facade is
required to traverse authenticated typed owners/objects. Raw object/selector
spaces, `SpaceId`, internal `lix::forktree`, external presence rows, object
indexes, compatibility readers and model substitution are rejected.

## Launch state

Ryzen-V reported no runnable immutable Stage2 head while this package was
frozen. The historical `c238` head and current compiler-red milestone
`cbe48835f` / tree `36ffe0ff867cd31bf52263675de2d16fc54e9b4f` are explicitly excluded.

When the first runnable immutable head arrives, the exact order is:

1. verify advertised ref/head/tree and clean detached worktree;
2. run this package's residue oracle and warnings-clean compile probe;
3. run only `image-64-1` on RocksDB under 20 minutes;
4. stop and attribute any failure; widen only after that gate is green.
