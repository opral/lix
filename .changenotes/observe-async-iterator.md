---
type: patch
---

JavaScript `lix.observe(sql, params, { signal })` now returns a standard async iterator. Consume results with `for await...of`; manual `next()` calls return `{ value, done }`. The custom `ObserveEvents` export and observation `close()` method are removed without a compatibility adapter.

Abort the supplied signal to cancel from outside the loop. Breaking out of iteration, closing Lix, or reaching the end of the stream releases the observation; cancellation settles pending reads even while binding setup is pending. Observation failures terminate iteration and propagate through normal promise rejection and `try/catch`. Existing result contents and coalescing remain unchanged.
