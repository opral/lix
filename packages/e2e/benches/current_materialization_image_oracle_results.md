# Current materialization-image oracle results

This experiment tests a Neon-style split between immutable semantic history
and exact current materialization images. Plugin-selectable content would keep
only its current file image in binary CAS; historical bytes would be rendered
from the semantic authority. Binary, unclassified, and plugin-WASM payloads
remain ordinary CAS values. The shape follows Neon's separation of delta
layers from image layers and its reconstruction of a page at a requested LSN.
See Neon's [pageserver storage model][neon-storage] and [GetPage@LSN][neon-lsn].

The benchmark is intentionally optimistic. Every payload without a NUL byte
in its first 8 KiB is treated as plugin-selectable, so an actual plugin
selection boundary can only retain more data. It then traverses the production
CAS manifests and retains every dependency needed by a surviving single,
chunked, or one-hop delta value. Shared chunks, base layouts, manifest pages,
and chunk-presence rows are included.

## Exact reachable-row bound

The accepted SlateDB replay databases are unchanged: VS Code 100 commits,
Brands 80, and Wesnoth 15. `current CAS rows` and `reclaimable CAS rows` include
the physical four-byte space prefix on every logical storage row.

| corpus | complete database | current images | current CAS rows | retained CAS rows | reclaimable CAS rows | optimistic complete cut |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| VS Code | 56,912,396 | 495 | 49,813,305 | 48,798,448 | 1,014,857 | 1.78% |
| Brands | 15,715,693 | 258 | 15,553,043 | 15,550,778 | 2,265 | 0.01% |
| Wesnoth | 4,194,632 | 46 | 3,155,642 | 3,023,749 | 131,893 | 3.14% |
| **aggregate** | **76,822,721** | **799** | **68,521,990** | **67,372,975** | **1,149,015** | **1.50%** |

VS Code removes 1,640 historical manifests, but their retained one-hop delta
representation is already small. Brands removes only ten manifests because
almost all storage is unique current media. Wesnoth removes 43 manifests, yet
their complete reachable footprint is only 132 KiB.

## Decision

Do not add a current-image materialization mode or historical render fallback.
The most permissive lower bound misses the requested 20% complete-database
gate by more than an order of magnitude. Production support would add a second
historical file-read path, proof validation, image lifecycle management, and
failure recovery while reclaiming at most 1.50% on this corpus.

The result also narrows the remaining search space: historical file CAS is not
the large duplicate. A future radical candidate must change the encoding of
semantic history/current-state rows themselves, or demonstrate a combined
representation bound above 20%, before production implementation.

[neon-storage]: https://github.com/neondatabase/neon/blob/main/docs/pageserver-storage.md
[neon-lsn]: https://neon.com/blog/get-page-at-lsn
