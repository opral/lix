# Excalidraw v2 plugin

This guest implements Excalidraw scenes on the Lix Wasm Component plugin API
v2. Its semantic boundary follows the units users usually edit independently:

- one stable `excalidraw_scene` root for top-level scene metadata and layout;
- one `excalidraw_element` per native element ID, with a fractional order key;
- one `excalidraw_file` per native key in the `files` map.

The element and file payloads are complete JSON encoded as strings. This is a
deliberate packet-v1 compatibility choice: durable entity snapshots currently
reject JSON numbers, while Excalidraw geometry is predominantly numeric. It
also keeps the plugin forward-compatible with element fields introduced by
newer Excalidraw versions.

The root stores an exact source template with reserved internal markers where
the `elements` and `files` contents belong. Together with per-entity layout
fragments, this makes an unchanged file round-trip byte-for-byte, including
whitespace and numeric spelling. A localized edit inside one existing element
changes only that element entity. Entity-side updates to one element or file
are rendered as one localized byte splice; structural changes fall back to a
full-file splice.

The current implementation reparses a changed file in full. That is the
intentional Pareto starting point: sparse entity output and localized rendering
cover the common collaborative edit path without introducing a second
incremental JSON parser.
