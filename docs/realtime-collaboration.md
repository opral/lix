---
description: Connect several clients to one Lix repository and observe changes as they happen.
---

# Real-time collaboration

Real-time collaboration means that several clients connect to the same hosted
repository. One client writes data and the other clients receive updated query
results through `observe()`.

## Connect two clients

```ts
import { openLix } from "@lix-js/sdk";

const url = "https://example.com/repositories/acme";
const alice = await openLix({ server: { mode: "remote", url } });
const bob = await openLix({ server: { mode: "remote", url } });
```

Production clients should also provide authentication headers. The server owns
persistence and authentication. See [Hosting](./hosting.md).

## Observe a query

```ts
const files = bob.observe("SELECT path FROM lix_file ORDER BY path");

const initial = await files.next();
console.log(initial?.result.rows.length);

await alice.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
  "/notes/plan.txt",
  new TextEncoder().encode("Draft"),
]);

const update = await files.next();
console.log(update?.result.rows[0]?.get("path"));

files.close();
await alice.close();
await bob.close();
```

`observe()` sends an initial result and then a new result when a matching write
changes the query.

## Open another session in Rust

Embedded Rust applications can open independent sessions on one repository:

```rust
let alice = lix::open_lix().await?;
let bob = alice.open_another_session().await?;
```

The new session starts on Alice's current branch and account. Its transactions,
observations, active branch, and lifecycle are independent. To attribute its
changes to another existing account, configure the builder before awaiting it:

```rust
let bob = alice
    .open_another_session()
    .with_account(bob_account_id)
    .await?;
```

JavaScript applications use the equivalent `openAnotherSession()` method:

```ts
const main = await openLix();
const review = await main.openAnotherSession({
  branchId: reviewBranchId,
  accountId: reviewerAccountId,
});
```

The returned `Lix` has its own active branch, account, transactions,
observations, and close lifecycle while sharing the repository storage. Closing
either handle does not close the other. Remote sessions always use the server's
authenticated account; `accountId` may only restate that identity in remote
mode.

## Live editing and branches

Use the same branch when collaborators should see each other's accepted writes
immediately. Use separate branches when work must be reviewed before it joins
the target branch.

Branches and observations solve different problems:

| Need                                | Use                                        |
| :---------------------------------- | :----------------------------------------- |
| See accepted changes as they happen | One shared branch and `observe()`          |
| Isolate an agent or draft           | A separate branch                          |
| Review before accepting work        | `mergeBranchPreview()` and `mergeBranch()` |

## Presence

Lix synchronizes repository data. Cursor positions, selections, typing status,
and user avatars are temporary presence data. Lix does not provide them. Send
them through your application's real-time presence service.
