# e1af source-only readiness calibration

Expected result: **RED**, exit status 1. This is a deliberate pre-migration
calibration and makes no production/runtime claim.

Exact source anchor:

~~~text
e1af471b9ab0f598dafa7c2ddec7867667c81740
bfa0d271a723da8250ab76ada16fda90926f1099
~~~

The inherited source calibration command produced:

~~~text
legacy_control_generation  58
checkpoint_history         1139
snapshot_pin               16
selector_epoch             770
mutation_revision          24
PASS e1af_source_calibration
~~~

The inherited calibration log SHA-256 is
ef6077659dca998b3a4030f19d61434fb4bb97c0f491c738851f4bdfad553c9e.
The readiness wrapper then classifies the remaining legacy ownership as RED
and exits 1. The wrapper is source-only and does not run Cargo or adapters.

The RED is caused by known e1af residues: BranchHeadControl/TrackedHead/current
generation, mutation/revision spaces and writers, legacy BranchRef readers,
flat-row/schema paths, caches/fallback spellings, and independent GC/control
consumers. The future GREEN contract is one ForkTree selector/read/publication
authority with exact CAS and no compatibility path.
