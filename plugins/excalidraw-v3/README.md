# Excalidraw Component v3 plugin

This hard-cut Component v3 port keeps the production `excalidraw_scene`,
`excalidraw_element`, and `excalidraw_file` schemas unchanged. Accepted bytes,
durable entities, and opaque object-locator state live in host-owned immutable
arenas.

Localized element and file edits use a 4 KiB object locator, return one sparse
entity change, and record length changes in a compact copy-on-write shift
overlay.
