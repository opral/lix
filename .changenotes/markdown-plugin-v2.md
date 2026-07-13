---
type: major
---

Replaced the bundled Markdown plugin's top-level block model with stable, fine-grained GitHub Flavored Markdown entities.

Markdown files now use the `markdown_node` schema for structural nodes, text-bearing leaves, and durable table identities. This enables local diffs and independently addressable state changes, but is incompatible with the previous `markdown_document` and `markdown_block` schemas. The plugin now targets `.md` and `.markdown` files rather than `.mdx`.
