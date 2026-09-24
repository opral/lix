# Snapshot fixtures

These `.lixsnap` files are frozen test evidence. Their provenance is kept here,
outside the deterministic snapshot payload.

| Fixture | Source | Purpose | SHA-256 |
| --- | --- | --- | --- |
| `v68_bundled_csv_history.lixsnap` | Converted on 2026-08-27 from the previously tracked v68 `LIXMEM` fixture; its earlier origin was not recorded. | Proves the intentional pre-v72 hard cut rejects a complete, verified v68 artifact. | `aba486aa7e510fd4f69687221639ce32f5dc15a5238897f83dba151de74ac541` |
| `v68_external_tombstones.lixsnap` | Converted on 2026-08-27 from the previously tracked v68 `LIXMEM` fixture; its earlier origin was not recorded. | Covers explicit rejection of v68 state with external tombstones. | `c2e85790b8b99209df557ca443f36c5511e7f0842d4212a587e86612c2ddfd01` |
| `v72_account_without_profile_uri.lixsnap` | State generated at Lix `4816fdba591d7165ff1b0195e74471aa8fc73660`; converted to `LIXSNAP` on 2026-08-27. | Exercises account-schema migration before `profile_uri`. | `92456923e13bdd5d171e68cd3cb0f1860cd06d8275aad133269598178ed0ed94` |
| `v72_filesystem_checkpoints.lixsnap` | State generated at Lix `4816fdba591d7165ff1b0195e74471aa8fc73660`; converted to `LIXSNAP` on 2026-08-27. | Reproduces filesystem descriptors whose checkpoint tree could lose their directory descriptors during migration. | `76cd929c48a41f5e6cfcb9bc01bd134b50cbc767e5c5eca72b4d03dcb6c1e193` |
| `v75_released_repository.lixsnap` | State generated from clean Lix `939901cc215b42ba7606432cee40088abb1b3671`; converted to `LIXSNAP` on 2026-08-27. | Covers the released pre-epoch layout, divergent branches, checkpoints, JSON rows, filesystem rows, and a 65,537-byte blob. | `1072330958f35668cd988ff5e8ec5ac8d45c7a784af448c32a3daf99cfc00a7c` |

Regenerate one only as a deliberate fixture migration, update its digest and
provenance here, and explain why the old evidence is no longer retained.

## Deployed plugin recovery fixture

`v78_deployed_markdown.lixplugin.gz` freezes the actual Markdown plugin shipped in
LixRay's prepared browser SDK at Lix revision
`0b940f913a8bec633098036ed91a961b78d86069`. It was copied on 2026-09-09 from
`vendor/lix/packages/js-sdk/dist/bundled-plugins/plugin_markdown.lixplugin` in the
LixRay migration worktree. The adjacent `.lixray-browser-build.json` records:

- Producer run: [34398308943](https://github.com/opral/lix/actions/runs/34398308943).
- Artifact ID: `10122869169`.
- Artifact digest: `sha256:5b94afbb7f1146dcf793789283771e3abca24402d7265fb9bbb554f045ae8e8f`.
- Source revision: `0b940f913a8bec633098036ed91a961b78d86069` (also verified against the vendor Git checkout).
- Original archive: 2,239,187 bytes; SHA-256 `3eff83bd98a282a9748ad068d33d90c5ff111c1e0fac2a75dfe63659ac8c50c6`.
- Gzip fixture: 716,025 bytes; SHA-256 `93f8bb9a191feb0ff9f1cab82dcbfb30ef5bd2eff0a73c54a61a734d2c227bf6`.

The gzip wrapper uses a zero timestamp and decompresses to the byte-identical
original archive. The core recovery regression reads and decompresses this file
at runtime, so CI tests real Markdown rendering after recovery without rebuilding
a plugin or embedding its bytes in every test executable. Keep this artifact
fixed when current plugins change: it represents the deployed format that local
recovery must continue to understand.
