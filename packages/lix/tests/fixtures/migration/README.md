# Released repository migration fixtures

`released-v017-format81.lix` is an unchanged snapshot exported by the published
`@lix-js/sdk@0.17.1` native engine. Its opening report identifies storage format
81. It contains only synthetic data: an indexed `legacy_index` schema registered
in local and global scopes, rows `local` / `kept-local` and `global` /
`kept-global`, and a branch named `historical-fork`.

No storage markers or commit bytes were rewritten. This preserves the actual
released catalogs and packed v7 commit layout; downgrading a current repository's
version marker does not reproduce the validation failure.
