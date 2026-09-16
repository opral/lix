# Installing and managing plugins

Plugins extend Lix with support for file formats and custom merge behavior. Each plugin is distributed as a compiled `.lixplugin` ZIP. You do not need Rust or a Wasm compiler to install one.

Plugins are stored inside your Lix repository. Their archives travel with the repository and do not need to be downloaded again each time it opens.

## Download

Choose a published plugin version from [GitHub Releases](https://github.com/opral/lix/releases). Plugin tags use `<plugin-key>/v<version>`, independently of Lix releases. Replace `<VERSION>` below with a version present in the release list; it is not a literal tag.

```sh
gh release download 'plugin_json/v<VERSION>' \
  --repo opral/lix \
  --pattern plugin_json.lixplugin \
  --dir ./plugins
```

Pin a specific version so your application installs the same bytes each time. Each release also provides `SHA256SUMS` and identifies the plugin API it uses. If a plugin has no published release yet, its source can be built with the [repository packager](#building-an-archive-from-source).

## Install

Write the archive into Lix at `/.lix/plugins/<plugin-key>.lixplugin`. The filename must match the key declared by the plugin's manifest.

```ts
import { openLix } from "@lix-js/sdk";
import { readFile } from "node:fs/promises";

const lix = await openLix();
const archive = new Uint8Array(
  await readFile("./plugins/plugin_json.lixplugin"),
);

await lix.execute(
  `INSERT INTO lix_file (path, content)
   VALUES ($1, $2)
   ON CONFLICT (path) DO UPDATE SET content = excluded.content`,
  ["/.lix/plugins/plugin_json.lixplugin", archive],
);

// Continue using lix, then close it when finished.
```

Lix validates the archive and registers its schemas atomically. If installation fails, the statement leaves the previous state unchanged. Installation applies to the current branch. The plugin archive is tracked like other repository files.

Install plugins before writing files you want them to process. Installation does not promise an automatic backfill of all files already in the repository.

## Install in a browser

Download the pinned archive during application setup or build and serve it with your application's static assets. Read those bytes in the browser:

```ts
const response = await fetch("/plugins/plugin_json.lixplugin");
if (!response.ok) throw new Error("Could not download plugin");
const archive = new Uint8Array(await response.arrayBuffer());
```

Use the same SQL installation statement with those bytes. This setup does not require a cross-origin browser request to GitHub.

## List installed archives

```ts
const plugins = await lix.execute(
  `SELECT name, path
   FROM lix_file
   WHERE path LIKE '/.lix/plugins/%.lixplugin'
   ORDER BY name`,
);
```

## Update

Download the desired release and write its bytes to the same archive path using the installation statement above. Updates are explicit; Lix does not automatically download newer releases.

Lix validates the replacement against existing plugin-owned data. An update that changes an incompatible schema or ownership contract is rejected. Follow that plugin's migration instructions when a migration is required.

## Uninstall

Delete the exact archive path:

```ts
await lix.execute(
  "DELETE FROM lix_file WHERE path = $1",
  ["/.lix/plugins/plugin_json.lixplugin"],
);
```

Uninstalling stops that plugin's processing on the current branch. It preserves existing user files, registered schemas, and stored rows. Those rows no longer stay synchronized with files through the removed plugin.

Reinstall by writing the archive again. Uninstalling and reinstalling is not an automatic data migration.

## Compatibility

Plugin release versions and plugin API majors are independent. A plugin targeting API v2 can continue running on newer Lix releases that support v2 without being rebuilt. Compatible API additions preserve existing plugins; a plugin using a newly added API may require a newer Lix release.

See [Plugin API compatibility](plugin-api-versioning.md) for the contract and compatibility tests.

## Building an archive from source

In a checkout of the Lix repository, with its Rust toolchain and Node.js installed:

```sh
npm ci --prefix packages/js-sdk
node scripts/build-plugin.mjs plugin_json --out-dir ./dist/plugins/plugin_json
```

The packager compiles the plugin for `wasm32-wasip2`, reads its manifest, and packages the declared Wasm entry and schemas. The output is a compiled `plugin_json.lixplugin` archive and its checksum. Use those archive bytes in the same installation statement.

For authoring a plugin, see [Writing Lix plugins](../packages/lix/PLUGIN.md).
