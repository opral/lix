# OLAP corruption and valid-absence matrix

This is a dormant, test/report-only contract for the exact b59 ForkTree side.
It is not a b59 runtime result. Every case runs against a clean fixture and
must use one retained authenticated read/view.

| target domain | corruption | required typed outcome | required invariants |
|---|---|---|---|
| global selector | malformed | `MalformedGlobalSelector` | unchanged authority fingerprint; zero writes/publication/CAS |
| global selector | missing | `MissingGlobalSelector` | unchanged authority fingerprint; zero writes/publication/CAS |
| global selector | wrong kind | `WrongGlobalSelectorKind` | unchanged authority fingerprint; zero writes/publication/CAS |
| global selector | identity substitution | `GlobalSelectorIdentityMismatch` | unchanged authority fingerprint; zero writes/publication/CAS |
| branch selector | malformed | `MalformedBranchSelector` | unchanged authority fingerprint; zero writes/publication/CAS |
| branch selector | missing | `MissingBranchSelector` | unchanged authority fingerprint; zero writes/publication/CAS |
| branch selector | wrong kind | `WrongBranchSelectorKind` | unchanged authority fingerprint; zero writes/publication/CAS |
| branch selector | identity substitution | `BranchSelectorIdentityMismatch` | unchanged authority fingerprint; zero writes/publication/CAS |
| state/root object | malformed | `MalformedStateRoot` | unchanged authority fingerprint; zero writes/publication/CAS |
| state/root object | missing | `MissingStateRoot` | unchanged authority fingerprint; zero writes/publication/CAS |
| state/root object | wrong kind | `WrongStateRootKind` | unchanged authority fingerprint; zero writes/publication/CAS |
| state/root object | identity substitution | `StateRootIdentityMismatch` | unchanged authority fingerprint; zero writes/publication/CAS |
| catalog root | malformed | `MalformedCatalogRoot` | unchanged authority fingerprint; zero writes/publication/CAS |
| catalog root | missing | `MissingCatalogRoot` | unchanged authority fingerprint; zero writes/publication/CAS |
| catalog root | wrong kind | `WrongCatalogRootKind` | unchanged authority fingerprint; zero writes/publication/CAS |
| catalog root | identity substitution | `CatalogRootIdentityMismatch` | unchanged authority fingerprint; zero writes/publication/CAS |
| checkpoint root | malformed | `MalformedCheckpointRoot` | unchanged authority fingerprint; zero writes/publication/CAS |
| checkpoint root | missing | `MissingCheckpointRoot` | unchanged authority fingerprint; zero writes/publication/CAS |
| checkpoint root | wrong kind | `WrongCheckpointRootKind` | unchanged authority fingerprint; zero writes/publication/CAS |
| checkpoint root | identity substitution | `CheckpointRootIdentityMismatch` | unchanged authority fingerprint; zero writes/publication/CAS |

The pure model executes all 20 typed cases across the five authenticated
domains. The future adapter harness must preserve the same domain/corruption
cross-product, including catalog and checkpoint roots.

Valid absence is a separate control: an optional query object that is
authenticated as absent returns `ValidAbsence`, not a missing-root error. It
must still consume one coherent read, perform zero writes/publication/selector
CAS/epoch CAS, and leave the authority fingerprint byte-identical.

For every failure, capture before/after authority fingerprints, typed error,
read/view count, publication count, selector/epoch CAS count, backend writes,
and result digest. Any changed fingerprint, partial write, fallback, or
`ValidAbsence` result for a required object is a blocker.
