# Derived Round-Trip Case Corpus

This directory holds `markdown-syntax` round-trip-stability case files derived
from upstream test sources. The tests do not execute any upstream source
directly; the upstream license text is under
`../../conformance/THIRD-PARTY-LICENSES/`.

Cases are organized by dialect, not by upstream tool. `origin:` in each case
header is a count-bucket label (`commonmark` or `gfm`) selecting which
expected-behavior dialect produced the input; it is no longer a path segment.

## Executable Inputs

`commonmark/` and `gfm/` hold the executable derived corpus. They contain
Markdown source arguments extracted from recognized upstream parser-facing calls
and package-owned stability inputs. Executable cases declare
`role: upstream-input`.

Each semantic case declares a `profile` in its case header. The fixture runner
uses that profile to choose `CommonMark`, `GFM`, `MDX`, math, frontmatter,
wikilink, or `extras` syntax options per case.

The executable check uses the public `markdown-syntax` boundary:

- parse Markdown source into AST
- serialize AST back to Markdown source
- parse the serialized Markdown again
- compare the public AST projection

## Format

Each file starts with metadata:

```text
# markdown-syntax semantic input cases v2
origin: commonmark
commit: 1506572
source: upstream-tests/html_flow.rs
role: upstream-input
count: 151
profiles: commonmark
```

The `source:` value is a historical provenance identifier. The vendored upstream
sources are not present in this tree, and `source:` is not cross-checked against
an on-disk file.

Executable semantic cases include a profile in the case header. Each case is
length-prefixed so the Markdown body can contain arbitrary delimiter-like text:

```text
--- case 1 profile gfm bytes 17
| a |
| - |
| b |
--- end
```

The `bytes` value is the UTF-8 byte length of the case body.
