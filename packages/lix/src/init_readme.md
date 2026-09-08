# The `.lix` directory

This directory contains files used by Lix and applications built on Lix.

- `app_data/`: application-owned repository content. Each app uses its own
  subdirectory, for example `app_data/atelier/extensions/` for Atelier extensions.
- `plugins/`: installed `.lixplugin` archives that teach Lix how to track changes
  within supported file formats. Use Lix's plugin installation and removal APIs
  to manage these archives.
- `README.md`: this guide, created with the repository's initial commit on `main`.

These files and directories are repository content: they participate in Lix
history, branching, and synchronization. Apps should store their files under
`app_data/<app-name>/` to avoid conflicts with other apps and Lix.

When using filesystem storage, Lix also manages local files in this directory:

- `.internal/`: local database and storage internals. Let Lix manage this directory;
  do not edit its contents manually.
- `.gitignore`: excludes the contents of `.lix/` from Git. Lix tracks its
  repository content independently of Git.

These local storage files are not part of Lix's tracked repository content and
may not exist when using other storage adapters. Lix creates this README only
when initializing a new repository; reopening it does not overwrite your edits.
