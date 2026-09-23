# The `.lix` directory

Lix is a version control system that can store files and application data, with
history, branching, and synchronization. This directory contains repository
content used by Lix and applications built on Lix.

- `app_data/`: application-owned repository content. Each app uses its own
  subdirectory, for example `app_data/atelier/extensions/` for Atelier extensions.
- `plugins/`: installed `.lixplugin` archives that teach Lix how to track changes
  within supported file formats. Use Lix's plugin installation and removal APIs
  to manage these archives.
- `README.md`: this guide, created with the repository's initial commit on `main`.

These files and directories are repository content: they participate in Lix
history, branching, and synchronization. Apps should store their files under
`app_data/<app-name>/` to avoid conflicts with other apps and Lix.
New repositories may not have application data or plugins yet; apps add those
files as needed.

## Querying Lix with JavaScript

Applications and agents can query Lix programmatically with the
[Lix JavaScript SDK](https://www.npmjs.com/package/@lix-js/sdk). See the
[Lix documentation](https://lix.dev/docs) for setup and API details.

For example, open a filesystem-backed repository and list its files:

```js
import { openLix } from "@lix-js/sdk";
import { FilesystemStorage } from "@lix-js/storage-filesystem";

const lix = await openLix({ storage: new FilesystemStorage({ path: "." }) });
const files = await lix.execute("SELECT path FROM lix_file ORDER BY path");
console.log(files.rows.map((row) => row.path));
await lix.close();
```

When using filesystem storage, Lix also manages local files in this directory:

- `.internal/`: local database and storage internals. Let Lix manage this directory;
  do not edit its contents manually.
- `.gitignore`: excludes the contents of `.lix/` from Git. Lix tracks its
  repository content independently of Git.

These local storage files are not part of Lix's tracked repository content and
may not exist when using other storage adapters. Lix creates this README only
when initializing a new repository; reopening it does not overwrite your edits.
