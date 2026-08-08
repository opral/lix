# W4a v4 file-content correction manifest

TEST/REPORT-ONLY direct successor of immutable
`f2f4c41bd3a64187f8288ca0396fd364a1f2f8fe`.

This v4 child is the narrow verifier self-path correction from immutable v3
`82f4ac5c202ae54ee90b56c45739656891ce442c`.

No production source, adapter, PR, or merge changes are present. The package
freezes the exact e1af RED baseline, a candidate-parametric source verifier
with executable negative fixtures, and a warnings-denied persisted-state
model.

## Package artifacts

SHA256SUMS covers every listed artifact below; the index is regenerated only
after final content is frozen and is intentionally not self-listed.

| file | SHA-256 |
|---|---|
| W4A_FILE_CONTENT_READINESS_E1AF.md | 7bff451054a3762a98d4ca09eea68e17b1c3f7221d4323fd826ecbe91db0524f |
| verify_w4a_source.sh | 43dd09317ded5b9c09f79c271cb10903d47ae11004c5a860a6c908a0121b3ad7 |
| w4a_file_content_model.rs | ea103b6716304a82115738f7785cdea671657752283ea60ff68b1e092640c138 |
| MODEL_RUN.log | 558bff56ab77f03e073771ec30436747aa2376df10ef138ca4fc5c5ee30ada08 |
| SOURCE_RED.log | ac461bec69f0992379c159f9d12dc3fd5ac5ab0d588aeb0f050a68333b29adce |
| SOURCE_GREEN.log | e8f0493f055c1c7bfb8a3ed575fd640621c406f950973fe02b8e2919a2547a32 |
| INVOCATION_MATRIX.log | 61890b6aebf9ad65944299cea4ac3ecdae032ce8c80c8cf8b94a2b088827274f |

## Acceptance summary

- exact e1af source calibration: RED;
- candidate source-positive gate: GREEN;
- source-negative fixtures: 10/10 rejected;
- warnings-denied persisted model: 13/13;
- production/adapters/runtime/performance: UNRUN by scope;
- production source/build/PR/merge: untouched.
