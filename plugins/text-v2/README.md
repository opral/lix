# Git text line-row Component v2 plugin

This component is the Lix-native counterpart to Git's text classification. It
matches paths whose first 8 KiB contain no NUL byte, the same bounded predicate
Git uses before choosing text-oriented behavior.

Each LF-delimited byte segment becomes one durable `git_text_line_v2` row. The
line's bytes, including a trailing LF when present, are URL-safe base64 encoded
so NUL-free but invalid UTF-8 Git text remains lossless. Rows have stable
host-allocated IDs and fractional order keys: a localized line update changes
that row only, an insertion adds one row, and a reorder updates only the rows
whose order must move.

For selected Git-text files, Lix makes the line rows authoritative and stores
only a renderer-path/SHA-256/length proof of their rendered bytes; it does not
retain a second raw source blob in binary CAS. Reads re-render the rows and
verify that proof. NUL-bearing input inside Git's first-8-KiB window never selects this
plugin and remains an ordinary raw binary file.

Build it with:

```sh
cargo build --release -p plugin_git_text_v2 --target wasm32-wasip2
```

Package `manifest.json`, `schema/git_text_line_v2.json`, and the resulting
`plugin.wasm` into a stored `.lixplugin` ZIP, then install it through the
normal tracked plugin path:

```text
/.lix/plugins/plugin_git_text_v2.lixplugin
```

Plugin installation is setup work, not part of replay timing.
