# File layout experiment E

## Immutable anchors

- pull request 1469 head: `ff5795c93fc70dd0c6e77fc08e6a311727603c7b`
  (tree `1630ea3836e5c95237140d6d54e28b3bf5b2d425`)
- `origin/main`: `6085ac656baf1634dd152c5e23da03589c2edea9`
  (tree `71e6479df9b0c056c0db79177e169fec988f84eb`)

The binary-CAS, filesystem, and storage-adapter implementation at the pull
request anchor is byte-identical to its merge base.  The experiment therefore
uses that anchor for the E layouts and the exact-main `cas_sharing` example as
the production-layout control.

## Layouts

- Current: JSON file-descriptor and BlobRef rows plus the shipping manifest,
  manifest-chunk, and chunk spaces.
- E1: native typed metadata and an authenticated chunk descriptor embedded in
  the row. Payload bytes remain external content-addressed chunks.
- E2: native typed metadata and a fixed 32-byte pointer to an independently
  authenticated, shared descriptor object. Payload bytes remain external
  content-addressed chunks.
- E3: payload bytes embedded in the typed row below a threshold. This is
  rejected because it amplifies copy-on-write carrier pages.

All E encodings are canonical binary values and every descriptor/chunk read is
BLAKE3-256 authenticated. There is no raw-hash read, fallback decoder, second
writer, or cache authority.

## Result

Use one explicit canonical descriptor union:

```text
ContentDescriptorV1 =
  Inline { descriptor_id: ObjectId, bytes: <= 128 bytes }
  External { descriptor_id: ObjectId }
```

The tag is decoded directly; readers never probe another representation. The
descriptor ID authenticates the same canonical descriptor bytes in both cases
and can be retained independently by history. Payload bytes are never stored
in the typed row.

The 128-byte boundary is based only on immutable encoded descriptor size. With
the current CDC policy it admits at most three `(length, chunk_id)` entries:
`4 + 36*N <= 128`. Measured base descriptors were 76 bytes at 1 MiB, 148 bytes
at 4 MiB, 472 bytes at 16 MiB, and 1,840 bytes at 64 MiB.

Sensitivity:

- one retained row: E1 avoids the external-object overhead;
- five retained rows: E2 reaches physical parity around the 4 MiB/148-byte
  descriptor and wins clearly by 16 MiB;
- 64 shared references: E2 wins at 4 MiB on RocksDB and by 16 MiB on both
  adapters, while its fixed row pointer avoids about 2.26 MiB of modeled
  64-row carrier rewrites at 16 MiB.

At 1 KiB on SlateDB, E1 used about 16.9 KiB settled versus 32.1 KiB for the
current shape and 22.5 KiB for E2. E3 reduced object reads but increased the
modeled carrier from 36.5 KiB (pointer) to 100.5 KiB at a 1 KiB cutoff and to
389 KiB when all retained payloads were inline.

At 256 MiB/current CDC/32-byte metadata on SlateDB, E2 used 303,490,512
settled bytes versus 303,500,158 current and 303,556,874 E1. Its modeled row
carrier was 36,480 bytes versus 2,718,592 for E1.

Five-sample repeated latency cells show that E1/E2 are not universal speedups:
small reads improve materially, while 64-256 MiB hashing/chunk I/O dominates
and differences are mostly within a few percent. The durable decision is driven
by authenticated object count, fixed row width, range-read shape, and retained
history/branch amplification rather than a claim of broad latency gains.

## Raw evidence

- threshold matrix: SHA-256
  `5a8cbbb40187f4c460b617aa64476281cf168b0c1271ccca4c39b835016f7d7d`
- large matrix: SHA-256
  `6e96d27e6eed2932be3e6951ed21f84ed171393719dc6843d860871141a6d7de`
- E1/E2 size crossover: SHA-256
  `cf7f6fbf6b137774fb0793369b72675398ab28c0be7abe62360effa6069bb667`
- E1/E2 sharing crossover: SHA-256
  `ade3aa09a1c9e08f661a219bd05f20a2a50e045c8a987020ce634f02d5e6f39c`
- five-sample representative matrix: SHA-256
  `b73c3a07c698df44cd79feb7b6a7d9e356a08a2db69168e0cd41f3f57cbe0401`
- descriptor-size cases: SHA-256
  `be4863ac309a1e4319a1a5e36c676213d2102ea7053b6245969781eead5830af`
- exact-main 64 MiB current layout: SHA-256
  `8df59ee7da813b14809c1e73017471666dcddde24f7e8f1859f1ba386efa8478`
- exact-main 256 MiB current layout: SHA-256
  `95e02fece812f72ce408a7b7a86c2195e2637d0d4cd6e48f348680394e6cffc4`

