---
type: patch
target: lix
---
Release remote sessions when browser partial replicas close. Keep the last successfully admitted same-account credentials available only for the exact session-release request while child handles and the shared owner finish shutting down; do not refresh admission during teardown. Classify close-triggered fetch cancellation as transport abort, then retire the provider and callback channel. Preserve other attached clients, cancel active streams, and retain the storage ownership fence if local cleanup fails. Apply the same cleanup ordering when repository opening fails after creating its owner.
