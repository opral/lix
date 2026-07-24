## Lix Engine

- During development, `cargo test -p lix_engine` runs the fast base simulation.
- Before committing, run `cargo test -p lix_engine --features all-simulations`
  to exercise both the base and tracked-state-rebuild simulations.
