# Candidate-aware BranchRef source-only readiness calibration

Expected result: **RED**, exit status 1. This is a deliberate pre-migration
calibration and makes no production/runtime claim. The gate is not an
approval: it is a direct successor package for independent review.

Exact source anchor:

~~~text
e1af471b9ab0f598dafa7c2ddec7867667c81740
bfa0d271a723da8250ab76ada16fda90926f1099
~~~

The corrected verifier was invoked with explicit base and candidate identities:

~~~text
python3 verify_selector_readiness.py \
  /tmp/lix-branch-ref-readiness-e1af \
  e1af471b9ab0f598dafa7c2ddec7867667c81740 \
  /tmp/lix-branch-ref-readiness-e1af \
  e1af471b9ab0f598dafa7c2ddec7867667c81740

base_commit=e1af471b9ab0f598dafa7c2ddec7867667c81740
candidate_commit=e1af471b9ab0f598dafa7c2ddec7867667c81740
base_tree=bfa0d271a723da8250ab76ada16fda90926f1099
candidate_tree=bfa0d271a723da8250ab76ada16fda90926f1099
legacy.branch_head_control.base=80 candidate=80 delta=0
legacy.branch_head_cache.base=45 candidate=45 delta=0
legacy.branch_ref_reader.base=196 candidate=196 delta=0
legacy.branch_ref_stage.base=38 candidate=38 delta=0
legacy.mutation_revision.base=48 candidate=48 delta=0
legacy.tracked_generation.base=48 candidate=48 delta=0
legacy.raw_authority.base=0 candidate=0 delta=0
lix_branch_ref_occurrence_files=15
non_derived_lix_branch_ref_files=4
SOURCE_GATE=RED
~~~

The complete calibration log SHA-256 is
54b5e706bfb460bb8e5b206c5fff56f9e77fc6a0e18772bf7b3bd985df7948d9.
The wrapper classifies the remaining legacy ownership as RED and exits 1.
The wrapper is source-only and does not run Cargo or adapters.

Argument-driven behavior was independently probed with the prior readiness
head as both explicit base and candidate (f5eba0dc7a253581421950c43b68a49cd31422fc),
not e1af. It also returned the expected RED and reported that exact candidate
identity/tree; log SHA-256:

~~~text
bf1a4174d9d0e20d66bf8839c28bcb213f468a190f66a4d5cfc6b6ab928e1bbc
~~~

The RED is caused by known e1af residues: BranchHeadControl/TrackedHead/current
generation, mutation/revision spaces and writers, legacy BranchRef readers,
flat-row/schema paths, caches/fallback spellings, and independent GC/control
consumers. The future GREEN contract is one ForkTree selector/read/publication
authority with exact CAS and no compatibility path. The approved v4 identity
and 15/15 semantic model are bindings, not a waiver for these source residues.

The package's independent standalone model was compiled with `rustc
--edition=2021 --test -D warnings` and ran 7/7 tests. This is model evidence,
not production or adapter runtime evidence:

~~~text
binary SHA-256 04c749d9d90f8134a791e8b095e8adcf5f3b153b9e324ec8cf44682b0a891f57
log SHA-256    ff7ec3a355b21a730a261525c30f223bd7dc4ab2c7410cda88da39dee0ea9789
~~~

The candidate-aware gate also compiles and runs the five-test selector
fixture with `-D warnings`; this replaces the old uncompiled source snippets
and is not a production/runtime claim:

~~~text
source SHA-256  b0155133ee0abcb3ca14b74befb1e266ac083ed76539b796c7f559f1039d5356
binary SHA-256  5c35b9813baa76f151f2ad0570554e0d58250a8e664218779e71ed275ec777e6
log SHA-256     0515dc138bd33d334c539f21556f18d9be74ad6c5666ac247f50376bd515e569
result          5/5 PASS
~~~
