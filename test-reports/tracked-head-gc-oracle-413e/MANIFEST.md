# TrackedHead GC/current-generation migration oracle

Test/report-only package anchored at exact 413e:

- head `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d`
- tree `820fe560da3bbd2b00b788b0b1759c409048cd6e`
- parent `11442c1e0023e20307a7231d88cd557bc704fd13`
- required whole-module gate `0b9ab47f7ac7da600b215d0b8aa2ab04db6bd36d`

The package is not wired into Cargo, production, adapters, current-main
benchmarks, or a runtime matrix. It contains the GC ownership/epoch/drain
model, path-aware source verifier, and future Memory -> RocksDB -> SlateDB
command order.
