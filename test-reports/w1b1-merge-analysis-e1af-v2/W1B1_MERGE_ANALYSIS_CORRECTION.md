# W1b-1 correction contract

## What was blocked

The first oracle had a useful pure merge model and five exact e1af RED
predicates, but its source gate only looked for strings. It could not prove
that a future merge implementation passed one retained operation reader
through the exact facade alias into analysis, or that an alternate reader,
cache, fallback, or detached authority was absent.

## Corrected proof

structural_gate.py performs balanced item-scoped extraction of the merge
functions. It checks the retained-read factory, every merge-analysis caller,
the exact facade alias at every analysis call, the typed analysis parameter,
and the facade-to-CoherentView path. Forbidden construction/acquisition and
legacy authority names are checked in code rather than comments or string
literals. Positive and negative source fixtures execute through the same
checker, proving genuine GREEN capability and discrimination.

The standalone model is independent of Lix and has explicit operation/view
identity, authenticated base/source/target commit identity and ancestry, owner
rows, conflict groups, deterministic row output, exact result digest, and zero
publication steps. Missing, malformed, wrong-kind, identity-substituted,
duplicate, and conflicting owner inputs fail before any result is returned.
Publication/prepare/commit is not modeled as a reader operation and any
nonzero publication step is rejected.

## Preserved boundary

The exact e1af five-RED calibration remains the source baseline. This package
does not approve or wire production, does not invoke adapters, and does not
expand into W1a, W3, W4, or W5.

