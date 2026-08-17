# `@lix-js/storage-opfs`

Durable browser storage for Lix using SQLite Wasm and the Origin Private File System (OPFS).

```ts
import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";

const lix = await openLix({
	storage: new OpfsStorage({ name: "my-repository" }),
});
```

The provider runs in the same dedicated worker as the Lix Wasm engine. It uses
SQLite Wasm's OPFS SAH-pool VFS, indexed point reads, and bounded range pages;
reopening does not replay the repository into a JavaScript or Rust map.

OPFS and Web Locks are required. A Web Lock gives one tab exclusive ownership
of a storage name; opening that name concurrently rejects instead of risking
corruption.

## Development

Run `npm run test:browser` for provider persistence and ownership tests, and
`npm run test:browser:production` to test packed SDK and storage artifacts in a
minimal Vite application. `npm run benchmark` reports the memory-versus-OPFS
execution scorecard plus raw 10k/1M-row seed, page, reopen, and deletion samples
with p50/p95 reopen timings.
