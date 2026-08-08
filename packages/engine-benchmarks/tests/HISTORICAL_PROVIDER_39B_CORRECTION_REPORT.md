# Historical provider 39b correction oracle report

## Immutable anchor

The oracle is anchored to blocked production head
`47957d30ae7c16c89c3c523feea23e2f98461fed`, tree
`b2e0c8a355fcee64d24cd5fcf77d2351d6fe4170`, parent
`39b12568f86d02ec81327cb672b7ef5f7e936448`.

The anchor has parent-to-head full-index diff SHA-256
`90385cc0d009a1c858e79769288183dec2d5e1e29fd036df709d6695a83e7438` and
stable patch-id `d40a2dda07bc83d1a5478636652a0b8d65177df3`. This oracle adds
only test/report artifacts; it does not alter production source or register a
runtime acceptance path.

## Required semantics

- Omitting `lixcol_commit_created_at` from projection and filters must not omit
  authenticated certified event/plugin rows.
- Missing or malformed commit topology and authenticated payload-key identity
  mismatches must fail closed.
- Checkpoint chronology and filesystem working-diff baseline must share one
  retained ForkTree chronology view. A root is implicit, and only
  `marker == walked_commit_id` selects a non-root checkpoint.
- The source gate rejects projection-dependent reachable-node construction,
  explicit typed deferrals, TrackedState/certified-reader fallback, raw or
  second read acquisition, and missing ForkTree ownership of the two providers.

## Bounded execution

The pure model was compiled without Cargo or production dependencies:

```text
rustc --edition=2021 -D warnings --test packages/engine-benchmarks/tests/historical_provider_39b_correction_oracle.rs -o /tmp/historical-provider-39b-correction-oracle-test
/tmp/historical-provider-39b-correction-oracle-test --test-threads=1
```

Result: `6 passed; 0 failed`; model binary SHA-256
`4e505995cdcfde1f13ad7a71a092e8ab23cd2f6c62232cef553f232204678d51`.
Model source SHA-256:
`c0ff0dc243b41f2e570c627cc7dd539e6aa09b77630920a52ef948662f811084`.
The model covers metadata omission, projection equivalence, missing/malformed
topology, payload identity, exact marker/root behavior, and one retained
chronology view.

The static source gate was run as:

```text
bash packages/engine-benchmarks/tests/historical_provider_39b_source_gate.sh /tmp/historical-provider-39b-correction-oracle
```

Expected anchor result: `SOURCE_GATE=RED`, exit 1, with exactly these findings:

```text
projection-dependent reachable_nodes
typed checkpoint chronology deferral
typed filesystem working-diff deferral
missing ForkTree chronology owner: packages/lix/src/sql2/providers/checkpoint.rs
```

Source-gate script SHA-256:
`104837254768265c334869cbf4f76f409e5ef64e30f503134369e4cc2af34533`.
Captured gate output SHA-256:
`2ef5ed413c61af6a3111d5f45e480c871210d7d12dcce659cf672cae75962f3e`.
Captured model output SHA-256:
`7fb93a64eea819c17521b0b0b069785fd0f8850a743d9a3d2891c9b1ccf6957a`.
The empty warnings-denied compile log SHA-256 is
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.

## Verdict

`BLOCKED_ANCHOR_CONFIRMED`. The oracle is ready for R5's correction child:
the source gate must turn GREEN only after the authenticated topology is kept
independent of metadata projection, both providers use one ForkTree chronology
owner/view, and no legacy or second-read path is introduced.
