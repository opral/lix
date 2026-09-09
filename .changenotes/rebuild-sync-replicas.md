---
type: patch
---

Synced replicas now retain their previous storage generation and rebuild from the authoritative server during format upgrades. Local pending work no longer prevents opening the current repository. Standalone and authoritative repositories continue to migrate their stored data.

New recovery APIs list retained sources, export available logical recovery data, and restore tracked rows onto separate branches without overwriting the active branch. Recovery reports unresolved content, preserves local-only data, and keeps original sources across subsequent upgrades. Permanent sync rejection preserves pending edits instead of silently resetting them.

OPFS SQLite initialization failures now report their original error instead of surfacing only as a storage startup timeout.
