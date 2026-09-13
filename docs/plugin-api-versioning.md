# Plugin API compatibility

Plugin releases, Lix releases, and the plugin API have separate versions. A
plugin compiled against a supported API can keep running when Lix releases a
new version, without rebuilding or upgrading the plugin.

The current contract is Plugin API v2, identified in the Component's WIT imports
and exports as `lix:plugin-v2`. Lix also accepts the historical
`lix:plugin@2.0.0` spelling for the same contract. The Component declares its API;
the plugin manifest does not repeat it.

## Evolving the contract

Keep the identifier unchanged for compatible changes, such as providing a new
host operation that existing plugins do not import. A plugin using that new
operation needs a Lix host that provides it; compatibility means older plugins
continue to run on newer hosts, not the reverse.

Use a new major identifier for incompatible changes to existing operations,
records, resources, or their behavior. Keep the previous host implementation
available when adding the new one. Do not make a new plugin API release remove
support for the old one. Only implement an adapter when a second contract
actually exists; no version negotiation or migration chain is needed today.

The compatibility boundary includes installation and archive/schema validation,
file projection and rendering, column merging, and persisted plugin state. A
signature-compatible change that alters those operations can still break a
plugin. Security fixes and documented execution limits remain applicable.

## Verifying compatibility

Run frozen compiled archives against the Rust Wasmtime host and the shared JS
host in Node and Chromium. Keep these historical ZIP bytes unchanged: rebuilding
a fixture with current bindings would conceal compatibility regressions. The
fixtures live in `packages/lix/tests/fixtures/plugin-api` with provenance and
checksums. Their tests install archives, edit files and rows, merge divergent
changes, restore a snapshot, and execute the restored plugin again.

When introducing another supported API, add its fixtures and retain every older
supported API's tests. Current-source plugin tests remain useful alongside the
frozen binaries.

## Independent plugin releases

Publish compiled `.lixplugin` ZIP archives and checksums on GitHub Releases under
plugin-specific version tags. Release notes should report the Component's actual
API identifier. The plugin package version identifies that plugin's release;
it does not select the Lix plugin API or force a matching Lix version number.
