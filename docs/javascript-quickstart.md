---
description: Install the Lix JavaScript SDK, write a file, inspect its history, and undo a change.
---

# JavaScript quickstart

This guide creates an in-memory Lix repository, writes a file, reads its history, and undoes the latest change.

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

Every tracked write becomes a commit automatically. You never run a commit command.

`execute()` runs one statement. To run several statements atomically, pass an array of statements to `lix.executeBatch()`. Do not concatenate SQL into one script string.

## Read history

```ts
const history = await lix.execute(
  `SELECT diff_type, from_path, to_path, lixcol_to_commit_id, lixcol_position
   FROM lix_history('lix_file')
   WHERE from_path = $1 OR to_path = $1
   ORDER BY lixcol_position`,
  ["/hello.txt"],
);

for (const row of history.rows) {
  console.log(row.diff_type, row.from_path, row.to_path);
}
```

Position `0` is the head commit. Higher positions walk back through the first-parent chain.

To read the file's bytes at one of those commits, pass its `lixcol_to_commit_id` to `lix_as_of`:

```ts
const snapshot = await lix.execute(
  "SELECT content FROM lix_as_of('lix_file', $1) WHERE path = $2",
  [history.rows[0].lixcol_to_commit_id, "/hello.txt"],
);
```

## Undo the update

```ts
await lix.undo();
await lix.close();
```

The repository is in memory and disappears when the process ends. Continue with [Storage](./persistence.md) to save it locally or connect to a server.

## Next

- [Store application data](./schemas.md)
- [Work with files and media](./files-and-media.md)
- [Branch, review, and merge](./branching.md)
- [Add collaboration and local sync](./collaboration-and-sync.md)
- [Storage](./persistence.md)
