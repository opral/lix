# `@lix-js/storage-opfs`

Durable browser storage for Lix using SQLite Wasm and the Origin Private File System (OPFS).

```ts
import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";

const lix = await openLix({
	storage: new OpfsStorage({ name: "my-repository" }),
});
```

The provider runs in the same dedicated worker as the Lix Wasm engine. A Web Lock gives one tab exclusive ownership of a storage name; opening that name concurrently rejects instead of risking corruption.
