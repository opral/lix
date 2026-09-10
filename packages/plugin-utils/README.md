# Shared plugin source utilities

Guest plugins include `order_key.rs` as a private module. Keeping it source-only
preserves each plugin's compilation and optimization boundaries without adding
a host runtime dependency or another public crate API. The module owns the
order-key byte format and its tests; plugin-specific ordering behavior stays
in each plugin.
