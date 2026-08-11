# Lix Server Protocol

The Lix Server Protocol is the HTTP contract for talking to a remote Lix
repository. It is version `2` and lives under `/lix/v1`.

It defines the methods, wire formats, session behavior, and error envelopes.
It does not define HTTP frameworks, authentication schemes, URLs above
`/lix/v1`, or deployment policy.

## Why it exists

The protocol is the interop layer between clients and hosts.

A server that implements `/lix/v1` is a Lix server, and every Lix client works
against it unchanged. Point a client at a different server and nothing in the
client changes but the URL. The OpenAPI document plus the normative behavior
below is the entire contract. There is no separate vendor API.

The `lix` crate contains the reference implementation, so a server provides HTTP
and authentication and forwards requests to it. See [Hosting](./hosting.md).

## Surface

| Group       | Paths                                                                                         |
| :---------- | :-------------------------------------------------------------------------------------------- |
| Handshake   | `/lix/v1`, `/lix/v1/session`                                                                  |
| SQL         | `/lix/v1/execute`, `/lix/v1/execute-batch`                                                    |
| Transaction | `/lix/v1/transaction/{begin,execute,commit,rollback}`                                         |
| Files       | `/lix/v1/file`, `/lix/v1/file/upsert`, `/lix/v1/file/upsert-batch`                            |
| Versioning  | `/lix/v1/branch/{create,switch}`, `/lix/v1/checkpoint/create`, `/lix/v1/undo`, `/lix/v1/redo` |
| Observation | `/lix/v1/observe`, `/lix/v1/observe/multiplex`                                                |

Clients do not construct these paths. `openLix()` appends `/lix/v1/` to the
repository URL, opens a session, carries the server-issued `Lix-Session-Id` on
later requests, and reconnects observation streams.

## Identity and sessions

The protocol does not read bearer tokens, cookies, API keys, or certificates. It
receives an already-trusted principal in process and never derives identity from
request headers.

On session creation it ensures the Lix account exists, pins the session to it,
and scopes mutation idempotency to that principal. A session reused through a
different principal returns `403`. Clients cannot select `activeAccountId`
during the handshake.

SQL and file mutations accept an optional `Idempotency-Key` header. Replaying a
key after a lost response applies the mutation once.

## Contract

The machine-readable surface is
[`packages/lix/server-protocol.openapi.yaml`](../packages/lix/server-protocol.openapi.yaml).

Behavior that OpenAPI cannot express — session pinning, transaction ownership,
idempotency replay, observation ordering, and terminal storage semantics — is
normative in the reference implementation and its tests.

To run a server, see [Hosting](./hosting.md).
