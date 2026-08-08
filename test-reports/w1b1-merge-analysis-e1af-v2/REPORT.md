# W1b-1 merge-analysis correction v2 report

Status: frozen test/report-only successor; no approval is claimed.

## Scope and provenance

The package is a direct successor to the immutable e1af W1b-1 package:

- anchor: e1af471b9ab0f598dafa7c2ddec7867667c81740
- parent package head: 6f8cb08305342b2e218aacdb036ae64790cf2338
- parent tree: 10e9f90b8d60134505326f02106bf4d81256db64
- parent-to-successor package paths are confined below
  test-reports/w1b1-merge-analysis-e1af-v2/

## Gates run

The structural capability self-test accepted the positive fixture and rejected
all five negatives: second read, alias mismatch, fresh graph construction,
fallback/cache, and alternate JsonStore authority.

The exact e1af source command returned status 1 and preserved five RED
predicates. Its source log SHA-256 is:

  28b6a714d5076b97adc7abad3cd7676883f0089cfd836ff25bf4dde8fd0cfa4f

The standalone model compiled with rustc --edition=2024 --test -D warnings
and six tests passed. The frozen binary SHA-256 is:

  8a12bf24d5e1fb38a54cabeed44c9a8955d9aec6aa3e64829233148474d630c8

The model has explicit operation/view identity, authenticated base/source/
target identity and ancestry, owner rows, conflict groups, deterministic
result digest, duplicate/identity/corruption rejection, no partial result on
conflict, and zero publication steps.

The package checksum manifest covers every package file except itself. The
source gate remains candidate-parametric; only the e1af anchor is fixed for
the preserved five-RED calibration.

No production source, adapter, runtime matrix, PR, or merge was changed.

