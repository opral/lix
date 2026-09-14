---
type: minor
---

Plugins can use shared SDK order-key allocation and the SQL `lix_order_between` function instead of implementing fractional ordering themselves.

The native plugin testing harness now applies generated identities to small row fixtures and reports transition I/O counters. A reusable UUID-to-ordinal private-state index supports bounded lookup.

Text content updates and ordinary Markdown paragraph updates now use sparse accepted-file reads and state updates. Structural and formatting-sensitive changes retain the existing full-document fallback.
