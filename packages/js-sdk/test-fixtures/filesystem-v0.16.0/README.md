This physical RocksDB filesystem repository was generated on Linux x64 using
published `@lix-js/sdk@0.16.0` and `@lix-js/storage-filesystem@0.16.0` packages.
The stored format is 78. It reproduces the old filesystem opening path in issue #1839 without rewriting
current-format markers. It installs the published Markdown plugin archive and contains `/notes.md`,
one checkpoint with `# Notes` and `Checkpoint paragraph.`, and an
uncheckpointed change to `Working paragraph.`. The generator verifies the
plugin tracked document, heading, and paragraph rows before closing.

To regenerate, install those exact packages in an isolated directory, copy
`generate.mjs` there, and pass the desired Markdown `.lixplugin` release archive:
`node generate.mjs ./repository ./plugin_markdown.lixplugin`. After it closes,
archive with `tar -czf repository.tar.gz -C repository .`.

SHA-256: `a80b30a42de996efde01f522e5aa1fbb6bae9469d2746dda6b5c7318fde69b0f`.

`npm run test:native:production` installs packed current SDK, filesystem, and
native platform packages in isolation and opens this repository normally. It
checks working and checkpoint content plus Markdown plugin rows, scoped
progress/reporting, and an
idempotent reopen. No detached migration addon is packaged or loaded.
