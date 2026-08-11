---
description: Install the Lix JavaScript SDK, write a file, inspect its history, and undo a change.
---

# JavaScript quickstart

This guide creates an in-memory Lix repository, writes a file, reads its
history, and undoes the latest change.

## Install

```bash
npm install @lix-js/sdk
```

## Write and update a file

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();

await lix.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
  "/hello.txt",
  new TextEncoder().encode("Hello"),
]);

await lix.execute("UPDATE lix_file SET content = $1 WHERE path = $2", [
  new TextEncoder().encode("Hello from Lix"),
  "/hello.txt",
]);
```

Lix records both writes automatically. You do not need to create commits.

## Read history

```ts
const history = await lix.execute(
  `SELECT path, content, lixcol_depth
     FROM lix_file_history()
    WHERE path = $1
    ORDER BY lixcol_depth`,
  ["/hello.txt"],
);

for (const row of history.rows) {
  const bytes = row.value("content").asBytes();
  const text = bytes ? new TextDecoder().decode(bytes) : "<deleted>";
  console.log(row.get("lixcol_depth"), text);
}
```

Depth `0` is the state at the head. Higher numbers walk back through history.

## Undo the update

```ts
await lix.undo();
await lix.close();
```

The repository is in memory and disappears when the process ends. Continue
with [Persistence and Storage](./persistence.md) to save it locally or connect
to a server.

## Next

- [Store application data](./schemas.md)
- [Work with files and media](./files-and-media.md)
- [Branch, review, and merge](./branching.md)
- [Add real-time collaboration](./realtime-collaboration.md)
- [Persistence and Storage](./persistence.md)
