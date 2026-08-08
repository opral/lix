# TrackedHead oracle v3 authority corruption discriminator

TEST/REPORT-ONLY extension anchored to blocked v2:

```text
v2 head   1d9c47728377c6ec7d2646704d51f3aadb11c773
v2 tree   df2a373a1c0e7917f4abbd167c7659efd1c3e6a1
v2 parent 422319cca0dad82525ab840d157aba5be49b09f0
```

The v2 model validates five authority domains but its corruption helper
mutates only `state_root`. `verify_discriminator.sh` deliberately records
that as a RED calibration. A future v3 successor is GREEN only when it
independently mutates every `GlobalSelector`, `BranchSelector`, `StateRoot`,
`CatalogRoot`, and `CheckpointRoot` fixture with `Malformed`, `Missing`,
`WrongKind`, and `IdentitySubstitution` corruption.

The model's 20 cases each require exactly one retained read and one coherent
view, followed by zero plans, prepared writes, commits, and selector
rotations. Healthy authentication has the same one-read/one-view and zero
durable-work counters. All failures are typed before publication.

## Bounded checks

```sh
bash verify_discriminator.sh "$PWD" <v3-head> <v3-tree>
rustfmt --edition 2021 --check authority_corruption_matrix_model.rs
rustc --edition=2021 --test authority_corruption_matrix_model.rs -o /tmp/tracked-head-v3-model
/tmp/tracked-head-v3-model
git diff --check
```

These are static/standalone-model checks only. They do not build Lix, run an
adapter, execute production, or modify any production path. The verifier
rejects v2's state-root-only matrix and rejects any successor diff outside
 this v3 report/model/verifier directory.
