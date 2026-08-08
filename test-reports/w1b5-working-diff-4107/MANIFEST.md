# W1b-5 working-diff provider readiness package

Test/report-only package anchored to exact approved 4107bef177c00694574b4fc65d6bb209239ee877.
No production source, adapter, benchmark, runtime, Cargo, PR, or merge change
is contained here.

The package maps SQL working-diff, filesystem file/directory working-diff, and
checkpoint-baseline consumers. It separates the already ForkTree-owned
historical row APIs from remaining TrackedState/TrackedHead/current-layout
readers and writers. Its structural gate is candidate-parametric: it accepts a
future candidate commit/ref as an argument, enforces the entire W1b-5
production path allowlist, extracts balanced function bodies, and checks
operation-owned facade/graph arguments rather than token presence alone.

The model is standalone and warnings-denied. It covers checkpoint-to-ordinary
history, branch/global overlay, tracked/untracked visibility, file/blob rows,
projection/order/LIMIT, NULL/tombstone, marker/root chronology, corruption,
reopen, exact base/head identities, deterministic digest, and no partial
output.
