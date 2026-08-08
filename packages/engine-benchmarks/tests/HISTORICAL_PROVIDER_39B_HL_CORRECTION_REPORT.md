# Comprehensive H-L historical-provider correction oracle

## Immutable lineage

This direct test/report-only successor is based on frozen H/I head
`b175f184a53747ad81f6ce236400e17760aeae02`, tree
`2eaffd168bb32f9308102575707f0c270ccaf796`, whose production parent is the
blocked 47957 head
`47957d30ae7c16c89c3c523feea23e2f98461fed`.

The H/I parent-to-head full-index diff is
`2bde0fc58aa4f205729ca21e98cacec0daa74827530a2b92facd3591b1fd288e`, with
stable patch-id `72068c4b988222500164e66b2ef5659d2bf27ccd`.

The original H/I artifact bytes are unchanged and are bound here:

| Artifact | SHA-256 |
| --- | --- |
| H/I manifest | `b1cc8ff6ee16ab6c9b8804c11e36a367d7acd59b83e8df611e393e1cb43984c5` |
| H/I report | `6c75e6de728d634ea6e717f15295a4b830c166af8b792b32bd226525a13f7e16` |
| H/I model source | `c0ff0dc243b41f2e570c627cc7dd539e6aa09b77630920a52ef948662f811084` |
| H/I source gate | `104837254768265c334869cbf4f76f409e5ef64e30f503134369e4cc2af34533` |

The J-L source/model commit is `44194d18`; it changes only two new
test/report-side source files. The final report commit and complete successor
identity are supplied in the immutable handoff.

## J-L contract

J requires authenticated required file, directory, and blob payloads to reject
missing, NULL, and tombstone states. K requires a certified row whose
`commit_id` differs from the walked commit to return a typed error rather than
being silently skipped. L requires a filesystem path with a missing parent or
cycle to return a typed error rather than `None`.

The comprehensive model imports the frozen H/I module and adds four J-L tests:

- required file/directory/blob payload state rejection;
- certified commit identity mismatch rejection;
- missing filesystem parent typed failure;
- filesystem parent-cycle typed failure.

## Bounded results

The model was compiled and run without Cargo or production runtime:

```text
rustfmt --edition 2021 --check packages/engine-benchmarks/tests/historical_provider_39b_h_l_oracle.rs
rustc --edition=2021 -D warnings --test packages/engine-benchmarks/tests/historical_provider_39b_h_l_oracle.rs -o /tmp/historical-provider-39b-h-l-test
/tmp/historical-provider-39b-h-l-test --test-threads=1
```

Result: `10 passed; 0 failed` (six inherited H/I plus four J-L tests).

| Artifact | SHA-256 |
| --- | --- |
| H-L model source | `3e677b8435f1ee91934e838a41164df26555a46736cab59e5c147138242f9726` |
| H-L model binary | `6739705d9d0f34249e096f7ce4c8e5d8a77456fe468ebea33ade49c1f4f9bb58` |
| H-L model output | `465a67658c8f0d357250574c55de9d95c72cb91189422ec0817ea4058fba0cab` |
| J-L source gate | `96655eb592f4d5d018997a6375a5bc771d767c8c4fd5455131dadd8689dd5275` |
| J-L gate output | `43ee52c586129339777afeb527ea50bfc909d9eb29d60f60f1194c8ac98106bd` |
| warnings-denied compile log | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |

The J-L source gate was run against exact 47957 and correctly returned RED,
exit 1, with these findings:

```text
inherited H/I source gate is RED
required file/directory/blob entry payload missing-NULL-tombstone path
required file/directory/blob observed payload missing-NULL-tombstone path
certified-row commit_id mismatch is skipped instead of errored
filesystem path resolver returns Option instead of typed error
filesystem path cycle becomes None instead of typed failure
filesystem path missing parent propagates None instead of typed failure
```

`git diff --check` and standalone rustfmt passed. No production files,
Cargo metadata, runtime, adapter, or benchmark paths were modified.
