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
TARGET_METADATA.md        eb0f5516d9f927f21af052b807cfa1bdaea9cd60cb24a7b755bd3676ae45326f
TRACE_MANIFEST.md         d65b2d3ebbf5e9d00324483f86d7287bbe455cf5b2c8482aedd85c24c1c09087
WORKLOAD_CONTRACT.md      77b390e7dedaa0f056c1a728d683abfc8211d04462cdb620cc2f4a598abead79
trace_git_lfs_workload.sh 476d2f6253e77aa2a3e463089273c45f4650248b77128e9e5bef601bfa36f03d
```

The script is executable. There are no production, Cargo, Lix, ForkTree, PR,
or current-main benchmark changes in this package. The raw host-local traces
are hash-bound by `TARGET_METADATA.md` and the compact rows are committed in
`REFERENCE_RESULTS.csv`.
