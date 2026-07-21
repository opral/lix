---
type: major
---

Plugin execution now uses a durable branch-local registry and per-file owner
records instead of discovering and reopening plugin archives on current-state
reads and writes. Ordinary plugin-free file-data writes stop after the exact
registry lookup, and warm plugin execution reuses compiled matchers and
hash-matched WASM instances.

The original `.lixplugin` ZIP remains the filesystem artifact; installation
extracts and content-addresses its WASM component once. Existing workspaces
created before the registry format require an explicit migration that
reinstalls plugins and rematerializes owned files. Registered schema keys are
immutable while a declaring plugin is active; uninstall the plugin before a
schema migration, then install the updated package.

Registry v1 supports branch-local plugins only: `GLOBAL` and `UNTRACKED`
archives, along with manifests that set `match.content_type`, are rejected. The
internal `lix_plugin_registry_v1` and `lix_plugin_owner_v1` keys are reserved
from public `lix_key_value` writes, each branch may install at most 128 plugins,
and deleting one exact `.lixplugin` archive is the supported uninstall
operation.
