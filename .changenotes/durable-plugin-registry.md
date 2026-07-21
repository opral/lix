---
type: major
---

Plugin execution now uses a durable branch-local registry and per-file owner
records instead of discovering and reopening plugin archives on current-state
reads and writes. Ordinary plugin-free file-data writes stop after the exact
registry lookup, and warm plugin execution reuses compiled matchers and
hash-matched WASM instances.

The original `.lixplugin` ZIP remains the filesystem artifact; installation
extracts and content-addresses its WASM component once. Adding, replacing, or
removing the archive remains the install, update, or uninstall operation; the
registry is engine-owned derived state. Pre-registry installations are not
discovered or decoded and must be removed and re-added. Registered schema keys
are immutable while a declaring plugin is active; uninstall the plugin before
a schema migration, then install the updated package.

Registry v1 supports branch-local plugins only: `GLOBAL` and `UNTRACKED`
archives are rejected. Registry entries require every v1 field; path-only
matching is represented explicitly by a null content type rather than an
omitted field. The internal `lix_plugin_registry_v1` and
`lix_plugin_owner_v1` keys are
reserved from public `lix_key_value` writes, and each branch may install at most
128 plugins. Uninstall retains plugin-owned document state for reinstall; reads
that require an absent plugin fail with `LIX_ERROR_PLUGIN_UNAVAILABLE` instead
of silently returning empty bytes.
