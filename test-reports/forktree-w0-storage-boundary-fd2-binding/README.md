# W0 v3 ↔ fd2 storage-boundary binding

This is a narrow, immutable TEST/REPORT-only binding package. It connects the
SOURCE/REPORT-approved W0 v3 storage-boundary oracle at `6a91df3f…` to the
exact fd2 source head `fd2be256…` and its report-only package `e2503fd1…`.

Run:

```sh
python3 test-reports/forktree-w0-storage-boundary-fd2-binding/verify_binding.py .
sha256sum -c test-reports/forktree-w0-storage-boundary-fd2-binding/SHA256SUMS
```

The verifier is integrity/source-contract-only. It checks W0 provenance,
607/598/955 evidence, the five Rust negative probes, the actual-source
TypeScript/native diagnostics, wrong-domain reopen, the exact fd2 fallback
diagnostic, and report-only scope. It does not compile, run adapters, mutate
source, or claim Memory/RocksDB/SlateDB qualification.
