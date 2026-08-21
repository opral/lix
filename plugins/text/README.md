# Text line-row Component plugin

This component is the Lix-native counterpart to text classification. It
matches paths whose first 8 KiB contain no NUL byte, the same bounded predicate
the matcher uses before choosing text-oriented behavior.

Each LF-delimited byte segment becomes one durable `text_line` row. The
line's bytes, including a trailing LF when present, are URL-safe base64 encoded
so NUL-free but invalid UTF-8 Text remains lossless. Rows have stable
schema-defaulted UUIDv7 IDs and fractional order keys: a localized line update changes
that row only, an insertion adds one row, and a reorder updates only the rows
whose order must move.

Selected text files retain exact source bytes and durable line rows. The
blob-backed policy lets a cold v2 actor reopen directly from accepted bytes
without reconstructing a complete document from semantic rows. NUL-bearing
input inside the first-8-KiB window never selects this plugin and remains an
ordinary raw binary file.

Build it with:

```sh
cargo build --release -p plugin_text --target wasm32-wasip2
```

Package `manifest.json`, `schema/text_line.json`, and the resulting
`plugin.wasm` into a stored `.lixplugin` ZIP, then install it through the
normal tracked plugin path:

```text
/.lix/plugins/plugin_text.lixplugin
```

Plugin installation is setup work, not part of replay timing.
