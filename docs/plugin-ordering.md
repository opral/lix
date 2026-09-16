# Ordering plugin rows

Use `lix::plugin::ordering::{order_between, order_between_batch}` to allocate canonical order keys. Both bounds are exclusive. An absent bound is an open end; two absent bounds allocate into an empty sequence.

```rust
use lix::plugin::ordering::{order_between, order_between_batch};

let first = order_between(None, None)?;
let appended = order_between(Some(&first), None)?;
let inserted = order_between_batch(Some(&first), Some(&appended), 20)?;
# Ok::<(), String>(())
```

SQL uses the same allocator. NULL represents an open bound:

```sql
SELECT lix_order_between(NULL, NULL); -- first key
SELECT lix_order_between($1, NULL);   -- append after the last key
SELECT lix_order_between(NULL, $1);   -- prepend before the first key
SELECT lix_order_between($1, $2);     -- insert between neighbors
```

Pass the result as the inserted row's `order_key`, or call the function directly in an INSERT value. Allocate a batch with the SDK when inserting several rows into one gap. Repeating a call with identical bounds produces identical keys.

Read rows with `ORDER BY order_key, id`. Keys are lowercase hexadecimal strings whose lexical order matches their byte order. Concurrent callers can allocate the same key; their UUID IDs determine the tie order. This function does not reserve positions, lock neighbors, or make ties follow wall-clock insertion order. To insert between two rows whose keys tie, first assign distinct keys to that tied group using its surrounding distinct bounds.

Empty, noncanonical, equal, and reversed bounds return errors. Open-end allocation reserves a wide integer stride, keeping ordinary append/prepend keys at 34 characters; exhausted end space falls back to fractional allocation. Repeated interior insertion can grow keys. This API does not rebalance existing rows or impose a fixed maximum key length.
