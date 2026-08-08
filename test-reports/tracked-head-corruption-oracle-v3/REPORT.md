# TrackedHead whole-module corruption oracle v3

This is a test/report-only successor of oracle v2 at
1d9c47728377c6ec7d2646704d51f3aadb11c773. It closes the v2 review gap without
touching production source.

The model has six independently authenticated closure members:
GlobalSelector, BranchSelector, StateRoot, CommitCatalog, ChangeCatalog, and
CheckpointRoot. For each member it applies Malformed, Missing, WrongKind, and
IdentitySubstitution corruption before one coherent open. Every failure must
show exactly one retained read and one retained view, with zero plans,
prepared writes, commits, and selector rotations. A separate test replaces
each valid member after opening and requires the pinned view to fail stale
without durable work.

The model therefore contains 24 per-domain corruption cases plus six stale-view
cases. It is symbolic and does not claim production codec compatibility,
adapter behavior, or runtime qualification.

## Independent R4 binding

The package also binds R4's independently frozen discriminator:

| object | value |
|---|---|
| R4 head | 7ff277c297e93eba83da09bf12f83d6485a8458b |
| R4 tree | a0e6be2c9029144497b75e4f9dcd6b001d71fec9 |
| R4 parent | 1d9c47728377c6ec7d2646704d51f3aadb11c773 |
| R4 parent-to-head full-index diff | 55d018ea5389898414dbf7844053c5339b316bf36652574b86983c1c8cb43b4b |
| R4 stable patch ID | 1ad9a7cc93f5386920032d3d1b1cebc8febaa43d |
| R4 model | 20 cases over five authority slots |

R4 is retained as an independent red discriminator reference. This package
supersedes its five-slot shape with separate CommitCatalog and ChangeCatalog
members, yielding six domains and 24 corruption cases while preserving the
same one-read/zero-work requirement.

## Local focused evidence

Warnings-denied standalone binary SHA-256:
c690d4aad11e7423d30d64ca93cf334b28a2068dc7b0cbee1136cd94718c0497.
Binary output SHA-256:
99d37a26d21d8ee05abf9f6683768dcb9da32c7870304218f205d2aa4e531635.
Warnings-denied standalone test binary SHA-256:
7689e072cab5fff5aa24c5b2950efe66cc4ad82ac66136540d5563a1982ec4a9.
Test output SHA-256:
9c9619d97053b998dd916ac0c370e34c919ce8543d7c53d135554f8c6c3ce850.
The executable reported 24/24 and 0 failures with digest
55329c3b11586602; the test binary reported 3/3.

## Dormant commands

    rustc --edition=2021 -D warnings \
      test-reports/tracked-head-corruption-oracle-v3/selector_domain_contract_model.rs \
      -o /root/repos/evidence/tracked-head-whole-module-oracle-b59-v3/selector-domain-v3
    /root/repos/evidence/tracked-head-whole-module-oracle-b59-v3/selector-domain-v3
    rustc --edition=2021 -D warnings --test \
      test-reports/tracked-head-corruption-oracle-v3/selector_domain_contract_model.rs \
      -o /root/repos/evidence/tracked-head-whole-module-oracle-b59-v3/selector-domain-v3-tests
    /root/repos/evidence/tracked-head-whole-module-oracle-b59-v3/selector-domain-v3-tests

No Cargo, Lix, adapter, benchmark, or production build is part of this
package. The source gate must run before either standalone rustc command.
