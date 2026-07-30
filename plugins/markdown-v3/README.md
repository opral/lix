# Markdown Component v3 plugin

This is the hard-cut Component v3 port of the production Markdown plugin. It
keeps the `markdown_node_v2` schema and semantic granularity unchanged while
moving accepted bytes, durable entities, and locator state into host-owned
immutable arenas.

Localized file edits use a 1 KiB top-level range locator and a compact
copy-on-write shift overlay. Cold reconciliation can use ordered durable
entity pages with stable semantic fingerprints.
