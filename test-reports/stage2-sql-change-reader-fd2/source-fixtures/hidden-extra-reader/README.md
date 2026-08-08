# Structural negative fixture: hidden transitive reader acquisition

This candidate-shaped fixture has valid constructor fields and provider call
arguments, but the provider plan reaches a concrete helper in the ForkTree
closure that constructs `ForkTreeReadFacade::new(other_store)`. The verifier
must traverse that helper and reject the hidden acquisition, not only direct
constructor-body extras.
