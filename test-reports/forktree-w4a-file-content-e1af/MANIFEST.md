# W4a file-content readiness manifest

TEST/REPORT-ONLY package; no production source, adapter runtime, production
build, PR, or merge.

## Exact source binding

- e1af: `e1af471b9ab0f598dafa7c2ddec7867667c81740`
- e1af tree: `bfa0d271a723da8250ab76ada16fda90926f1099`
- e1af parent: `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35`
- e1af full-index binary diff:
  `9795ee3da81a06657a45a47a50417522a6a6bd7057e21eeb75597096417c9f3c`
- e1af stable patch ID: `31cc575644bf17e65c59d558a03acffc848c2e20`

## Package files

| file | SHA-256 |
|---|---|
| `W4A_FILE_CONTENT_READINESS_E1AF.md` | `adb75158953ca4b80a548999b876eaac4a89c09ab374108413bdc346ea06a1f5` |
| `w4a_file_content_model.rs` | `76f5e454b800d3e8dcc1d3925b971fe7a0b56a0b16847ebffdf114e90ab2d3ef` |
| `verify_w4a_source.sh` | `4fea7713d70245dec6ca998edf4408f8eac58ee3ad90c01c57595523bf3e8429` |
| `MODEL_RUN.log` | `4aa987fa5e7bfbb435aa82a15da6237139a16afd5dc6221d8fb65f45dc1fe520` |
| `SOURCE_RED.log` | `bc873d73c10a3d078cc784a4893184275b4114e793013ea3ff594d7975c9edfc` |

The compiled standalone test executable was outside the package and hashed as
`3d619843f6c7b17bbc87dd74e94ab4e91e8e056d47829219466b28bc0a998ae4`.

## Qualification result

- source verifier: expected RED, exit 1;
- standalone model: 7/7 green under `rustc --edition=2021 -D warnings --test`;
- Memory/RocksDB/SlateDB: UNRUN by contract.
