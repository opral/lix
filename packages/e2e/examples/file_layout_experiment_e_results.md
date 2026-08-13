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

- Current comparator: JSON file-descriptor and BlobRef rows plus the shipping
  manifest, manifest-chunk, and chunk spaces. This is measured legacy state,
  not an accepted representation in the candidate.
- E1: native typed metadata and an authenticated chunk descriptor embedded in
  the row. Payload bytes remain external content-addressed chunks.
- E2: native typed metadata and a fixed descriptor ID/length cell pointing to
  an independently authenticated, shared descriptor object. Payload bytes
  remain external content-addressed chunks.
- Selected: the canonical tagged E1/E2 union below. The earlier E3 row-inline
  payload experiment is rejected because it amplifies copy-on-write carrier
  pages; its decoder tag is now explicitly forbidden.

All E encodings are canonical binary values and every descriptor/chunk read is
BLAKE3-256 authenticated. There is no raw-hash read, fallback decoder, second
writer, or cache authority.

## Result

Use one explicit canonical descriptor union:

```text
ContentDescriptorV1 =
  Inline { descriptor_id: ObjectId, bytes: <= 128 bytes }
  External { descriptor_id: ObjectId, canonical_len: u32 (> 128) }
```

The tag is decoded directly; readers never probe another representation. The
descriptor ID authenticates the same canonical descriptor bytes in both cases
and can be retained independently by history. Payload bytes are never stored
in the typed row. File metadata is decomposed into native scalar columns; this
typed binary descriptor cell is not JSON/JSONB. JSONB remains available only
when it is the user's declared metadata-column type.

Codec validation rejects an 80-byte descriptor encoded as `External`, a
152-byte descriptor encoded as `Inline`, a mismatched inline descriptor ID,
and the retired row-inline payload tag. External reads authenticate both the
descriptor ID and declared canonical length before decoding chunk references;
there is no representation probe or fallback.

The 128-byte boundary is based only on immutable encoded descriptor size. With
the current CDC policy it admits at most three `(length, chunk_id)` entries:
`8 + 36*N <= 128`. Measured base descriptors were 80 bytes at 1 MiB, 152 bytes
at 4 MiB, 476 bytes at 16 MiB, and 1,844 bytes at 64 MiB. (The earlier report
omitted the canonical four-byte reference-count field from these figures.)

Sensitivity:

- one retained row: E1 avoids the external-object overhead;
- five retained rows: E2 reaches physical parity around the 4 MiB/152-byte
  descriptor and wins clearly by 16 MiB;
- 64 shared references: E2 wins at 4 MiB on RocksDB and by 16 MiB on both
  adapters, while its fixed row descriptor cell avoids about 2.26 MiB of modeled
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

## Pinned PostgreSQL-schema-v1 rebind

The model/codec candidate was rebound without following a branch name to
commit `2cf539744e7864f79bf1994e002f47cfd3281dc0`, tree
`89a6e9a0623483268cb7841f757446c5e29559dd`. Its public canonical scalar
types are `text`, `uuid`, `int8`, `float8`, `boolean`, `jsonb`, and
`timestamptz`. File identity/path/size metadata therefore remains native scalar
columns. `ContentDescriptorV1` is a typed binary cell; it is not JSONB.

The final model codec serializes file ID and directory ID as native UUID-width
fields, name as canonical UTF-8 text, and payload size as native `u64`; its
metadata-width sweep varies the text field rather than an opaque row blob. It
uses one descriptor tag and the same canonical descriptor content
ID for both classes. The inline tag carries the ID and descriptor bytes; the
external tag carries the ID and canonical length and resolves exactly that
content-addressed object. Unit gates are 4/4 green for the 128-byte class,
noncanonical tags, descriptor-ID substitution, and same-size chunk
substitution.

Required representative cells are green, including chunk corruption rejection
and cold reopen:

- 1 MiB / 80-byte inline descriptor, RocksDB and SlateDB, no shared copies:
  log SHA-256
  `199290f8aaa408df4c62b0d727ae60239689150c57468b9e48bea0e4165a75ca`.
  After GC the selected layout has zero external descriptor objects and zero
  row-inline payload bytes.
- 16 MiB / 548-byte external descriptor, RocksDB and SlateDB, 64 additional
  shared references (67 retained rows after version operations): log SHA-256
  `c3cb0adcef5ed96624b68fd9d4558e7dc6e2ee104f577c156a76140f02237c7d`.
  After GC both adapters retain one 548-byte authenticated descriptor object,
  15 unique chunks, and 67x logical sharing; row-inline payload bytes remain
  zero.
- release benchmark binary SHA-256
  `cadbc417a0e0c170eb2af0ff70f8a3a53e249f3870fcc2ad526c007eef0ddb9f`.

Each representative cell additionally rereads the retained payload after GC.
The corruption gate covers missing and same-size substituted chunks, plus
external descriptor ID/bytes and declared-length substitution. When an adapter
refuses immutable same-ID replacement before serving (SlateDB), that rejection
is itself fail-closed; RocksDB accepts the physically replaced test object and
the model's digest check rejects it on read.

This child changes only benchmark/report paths. The cells above qualify the
format model and real RocksDB/SlateDB storage behavior; they do **not** prove
that public `lix_file` SQL currently exercises this carrier. A baseline public
test target compiled, but the exact attempted filter selected zero tests; a
second legacy plugin-runtime file test fails in an inherited fixture while
reading plugin schema metadata. Neither result is claimed as E1/E2 public-path
coverage. Production cutover still requires replacing the current file-row
content reference with this tagged binary cell as the sole authority, followed
by public SQL, corruption, GC, and reopen qualification.
