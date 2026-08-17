# CommonMark Example Inputs

This directory contains `markdown-syntax` case files derived from the
generated CommonMark official example suite (the standard CommonMark spec
examples).

`official-stable-inputs.cases` is the executable package-owned stability subset
derived from the generated suite. Those examples are checked through the public
Markdown syntax boundary:

- parse CommonMark source into AST
- serialize AST back to Markdown source
- parse the serialized Markdown again
- compare the public AST projection

The copied upstream Rust source is not present in this tree. The full
CommonMark expected-HTML suite is checked by the AST-to-HTML conformance bench
under `../../conformance/`. License text for the upstream source snapshots is
under `../../conformance/THIRD-PARTY-LICENSES/`.

The file format is the same length-prefixed `.cases` format used by
`../cases/`; executable subset cases include `profile commonmark` in
their case headers.
