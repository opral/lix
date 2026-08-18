# Semantic Input Corpus

This manifest covers executable round-trip input cases under `commonmark/` and
`gfm/`. The fixture runner checks the total case count and executable profiles
against the files on disk.

Total executable input cases: 2300

## Profile Counts

- `commonmark`: 1486
- `extras`: 45
- `frontmatter`: 25
- `gfm`: 331
- `math`: 106
- `mdx`: 287
- `wikilink-after`: 9
- `wikilink-before`: 11

## Source Metadata

Each executable case file carries a `source:` provenance identifier. The copied
upstream Rust sources are not present in this tree, and `source:` is not an
on-disk path.
