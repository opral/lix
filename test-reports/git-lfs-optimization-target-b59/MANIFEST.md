# Immutable package manifest

Package identity is finalized by the Git commit containing this directory.
The package is based on exact b59 source:

```text
base= b59e1f11a51153e0a787a81f0f25bf104d150aaf
base_tree=700fd04d21bc40c05425c9fc9e10d65c9e1eda24
```

Before this manifest was added, the package file SHA-256 values were:

```text
README.md                 c8bb7253de1655c7711dbcd0548e7b6360c099a02118f11165563a9d7b2e9c3d
REFERENCE_RESULTS.csv     a7e341aabea27a6d31a01aa60a3208d9a7fad40f2230243919ed74fe88e614c7
TARGET_METADATA.md        09231d53d3454bccd24583adbe50e0e824eb9371360cb13535db2ec8ed78b6c2
TRACE_MANIFEST.md         4364173c5fa4a535b4b2b55b69cd4b8dcc276044c56ee95acbb745675295a3d2
WORKLOAD_CONTRACT.md      77b390e7dedaa0f056c1a728d683abfc8211d04462cdb620cc2f4a598abead79
trace_git_lfs_workload.sh 476d2f6253e77aa2a3e463089273c45f4650248b77128e9e5bef601bfa36f03d
```

The script is executable. There are no production, Cargo, Lix, ForkTree, PR,
or current-main benchmark changes in this package. The raw host-local traces
are hash-bound by `TARGET_METADATA.md` and the compact rows are committed in
`REFERENCE_RESULTS.csv`.
