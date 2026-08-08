# W4a file-content correction manifest

TEST/REPORT-ONLY direct successor of:
29f83418ddfbd7509ac7f9ba0245b6340a5fa522

No production source, adapter, PR, or merge changes are present. The package
freezes the e1af baseline RED and a candidate-parametric source GREEN path.

## Immutable production baseline

- commit: e1af471b9ab0f598dafa7c2ddec7867667c81740
- tree: bfa0d271a723da8250ab76ada16fda90926f1099
- parent: b484e20d845aee3f8137bfa3496f9b3cd0e8cd35

## Package files

SHA256SUMS covers every listed artifact below; all entries exist in this
successor and verify.

| file | SHA-256 |
|---|---|
| W4A_FILE_CONTENT_READINESS_E1AF.md | 9b02c930a4de48f7b001278fa8d8e2536f0ce6db984e4426c9ce3b86e35fbbdf |
| verify_w4a_source.sh | 4c90cc74752dc123f71fffa696a316ebd2281d07b97db068eb64d2d88cc868d1 |
| w4a_file_content_model.rs | 7f30c5555a4121f38c579ae06f790f5d1145c2107e799da3f069c2cc7fd53065 |
| MODEL_RUN.log | 39b32e65babb01e667e8966b8c2601d23c3518fbf2a83c95e01f49193f3a4988 |
| SOURCE_RED.log | 820dbe04f4ce6f675d92c3b5bca950d669e51fba99c1cbbbf8798ae6eb0d4e2c |
| SOURCE_GREEN.log | eb18a39e9970d8bbc79ee4fd397ccf3eb858b2cc81c1120f3525b5646021240e |

SHA256SUMS itself is the checksum index and is intentionally not self-listed.

## Gate identities

- baseline source verifier: SOURCE_RED.log
- candidate-parametric GREEN self-test: SOURCE_GREEN.log
- warnings-denied model run: MODEL_RUN.log
- model source uses no production or adapter dependency
- adapter and production runtime gates: UNRUN
