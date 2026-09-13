# Frozen legacy Plugin API v2 archives

These are compiled ZIP archives targeting `lix:plugin@2.0.0`, copied from the JS
SDK's existing build outputs before migration to `lix:plugin-v2`. Their build
provenance is PR #1762 revision `423071922dc4d47d37da72b043833aa8c3b888b2`;
they were captured at revision `852c1151f9fad27b640570c18a2caebbc5e41d55`.
They are historical build outputs, not newly reproduced release artifacts.

| Archive | SHA-256 |
| --- | --- |
| plugin_csv.lixplugin | 5ed1908dfebe6df0349a4f83aa9455eb8539e49541e56031a20038cf94bf29e8 |
| plugin_markdown.lixplugin | 30a491d0c1d490b7abbc7e361c88ff1457b41631fb15df2cea952cbc2d218943 |

Never regenerate or overwrite these fixtures when changing the current guest
bindings. Their purpose is to detect regressions that rebuilding plugins would
hide. Add a new directory for a new supported API instead. Rust Wasmtime and the
Node/Chromium JS hosts install these exact bytes; the tests exercise file
projection, SQL-driven rendering, column merging and snapshot restoration.
