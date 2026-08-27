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
