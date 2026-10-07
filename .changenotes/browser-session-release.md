---
type: patch
target: lix
---
Release remote sessions when browser partial replicas close. Keep the authenticated session-release transport available while child handles and the shared owner finish shutting down, then retire the provider and callback channel. Preserve other attached clients, cancel active streams, and retain the storage ownership fence if local cleanup fails. Apply the same cleanup ordering when repository opening fails after creating its owner.
