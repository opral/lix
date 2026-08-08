# Structural negative fixture: extra reader acquisition

This candidate-shaped fixture has the valid `forktree_reader` field and exact
provider call arguments, but also constructs
`ForkTreeReadFacade::new(self.other_store)` in the session caller. The v4
verifier must reject it before any production candidate can be called ready.
