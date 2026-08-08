# Correction-I structural shared-view proof

This test/report-only successor corrects the 30e88f92 gate. The prior gate
required `forktree_reader` and `query_source.forktree_reader.clone()` tokens,
but that could accept two providers whose fields had the same name while their
read identities differed.

`structural_view_proof.py` is an executable, function-scoped source verifier.
It identifies Rust struct and function bodies, records each provider's actual
`forktree_reader:` right-hand-side binding, resolves chronology calls against
function definitions under `packages/lix/src/forktree/`, and requires both
checkpoint SQL and filesystem working-diff call chains to use the exact
caller-owned `query_source.forktree_reader.clone()` expression. Provider
production bodies are rejected if they call `begin_read` or construct a local
`ForkTreeReadFacade`.

The positive fixture proves the accepted chain. The negative fixtures are
discriminating, not token variants:

* `distinct_views` binds one provider to `independent.forktree_reader`;
* `independent_acquisition` constructs a fresh facade from `store.begin_read()`;
* `fake_seam` calls an undeclared `checkpoint_history_fake` name while the
  actual ForkTree source defines only `checkpoint_history_real`.

All three must fail, while the positive fixture must pass. The unchanged 479
production source remains expected RED: it has no provider-owned retained
chronology chain and no ForkTree checkpoint chronology seam. This verifier does
not build, execute product code, or edit the candidate tree.

Calibration command output:

* `/tmp/correction-i-v3-calibration.log`
* SHA-256 `8e47e3bf07e23baa24003ec46e191244444ac92739cf66e3246037b9ae4838b8`
* result `RED failures=17` (the additional structural proof failure is
  candidate-owned acceptance evidence; the exact 479 compiler frontiers remain
  138/9 library and 381/16 library-tests, with no added diagnostics/warnings).
