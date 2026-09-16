# Releasing plugins

Plugins have independent Cargo versions and changelogs in `plugins/<name>`. Their compiled archives are published on GitHub Releases under tags such as `plugin_json/v0.16.2`. Plugin releases do not publish crates or npm packages and do not change the repository's latest Lix release.

## Request a release

Add a changenote for the affected plugin:

```md
---
target: plugin_json
type: patch
---

Preserve JSON formatting when updating an existing value.
```

Use the plugin's crate key as the target: `plugin_csv`, `plugin_excalidraw`, `plugin_json`, `plugin_markdown`, or `plugin_text`. Omit `target`, or use `lix`, for a Lix release. Use separate notes when a change affects several release targets, including changes to shared code that require rebuilding several plugins.

The release PR workflow prepares a draft candidate for each target with pending notes. It bumps only that target's version, updates its changelog, and consumes only its notes. Review plugin candidates separately from Lix candidates.

Mark a candidate ready when its content is settled. Ready candidates are frozen against automatic refresh for their own target; other targets can continue releasing. Return a candidate to draft to allow it to be refreshed. Merge the validated candidate to release it.

## Published artifacts

The plugin publisher builds the exact merged release source with the repository's pinned toolchain and locked dependencies. The shared packager reads each plugin's manifest and includes its compiled Wasm entry and declared schemas in a ZIP with stable entry ordering and timestamps. Identical input files produce identical archive bytes; this is not a promise that unrelated compiler versions produce identical Wasm.

Each release contains:

- `<plugin-key>.lixplugin`: the compiled archive ready to install;
- `SHA256SUMS`: the archive checksum;
- release notes from the plugin changelog and the API identity found in the
  compiled component.

The publisher validates the archive before publishing. A draft release is made public only after its expected assets have been uploaded and verified.

## Retry a failed publication

Use the plugin publishing workflow's manual retry with the original plugin and release source commit. Do not bump the version just to retry an interrupted upload.

A retry verifies the source tag and asset checksums. Matching published releases are left unchanged; missing draft assets can be uploaded. A different tag target or different existing asset bytes causes a failure instead of overwriting a published version. Correct the source or investigate the mismatch before retrying.

## SDK transition

The SDK's existing CSV and Markdown bundling temporarily delegates to the same packager. Removing bundled SDK archives and `bundledPluginArchives()` is a separate follow-up after compiled GitHub releases are available.

See [Installing and managing plugins](plugins.md) for the user-facing download and installation flow, and [Plugin API compatibility](plugin-api-versioning.md) for the compatibility contract.
