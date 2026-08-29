# Lix reference server

This package is a deployable **reference implementation** of a host for the
[Lix Server Protocol](../../docs/server-protocol.md). It is one example of how
to operate Lix over HTTP; it is not the protocol itself and it is not required
in order to build a Lix server. Any service in any language can be a Lix server
by implementing the documented protocol contract.

The reference server combines the canonical Rust protocol handler with a
multi-Lix runtime pool, SlateDB, and S3-compatible object storage. It is useful
as a ready-made internal data plane or as source code for a custom host.

## Trust boundary

The binary deliberately does not implement product authentication,
authorization, tenancy, billing, or repository discovery. Put it behind a
trusted gateway that owns those policies. A valid Lix UUID is opened on demand,
so the gateway must reject targets the caller is not allowed to access.

`LIX_SERVER_INTERNAL_TOKEN` is required at startup. Protocol requests must carry
`Authorization: Bearer <token>`. After authenticating the end user, the trusted
gateway may also set:

- `x-lix-account-id`: the verified Lix account ID;
- `x-lix-idempotency-scope`: a stable identity-provider scope, defaulting to
  the account ID when omitted.

These headers are implementation-specific and are removed before canonical
protocol dispatch. They are rejected when internal authentication is disabled;
unprotected local deployments always use the anonymous protocol principal.
Clients must never be allowed to set or forward them directly.

`GET /healthz` and the internal bearer token are operational features of this
binary, not Lix Server Protocol endpoints. All protocol operations live below
`/lix/v1/{lix_id}`.

## Run

The server requires an S3-compatible object store:

```sh
S3_ENDPOINT=https://s3.example.com \
S3_BUCKET=lix \
S3_ACCESS_KEY_ID=... \
S3_SECRET_ACCESS_KEY=... \
LIX_SERVER_INTERNAL_TOKEN=... \
cargo run --release --package lix-server
```

The container is published as `ghcr.io/opral/lix-server`. Deployments should
pin its immutable digest rather than a moving tag. GitHub creates the package
as private on its first publication; an organization administrator must make
`lix-server` public once. The release workflow verifies anonymous digest pulls
and will remain red until that one-time setting is complete.

### Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `BIND_ADDR` | `0.0.0.0:$PORT` | Listen address |
| `PORT` | `8080` | Port used when `BIND_ADDR` is absent |
| `LIX_SERVER_INTERNAL_TOKEN` | required | Protect protocol routes and enable trusted identity headers |
| `LIX_SERVER_MAX_OPEN_LIXS` | `32` | Maximum retained Lix runtimes |
| `LIX_SERVER_PROTOCOL_TIMEOUT_SECS` | `60` | Admission and request deadline |
| `LIX_SERVER_RECOVERY_CLOSE_TIMEOUT_SECS` | `30` | Runtime recovery close deadline |
| `S3_ENDPOINT` | required | S3-compatible endpoint |
| `S3_BUCKET` | required | Object-store bucket |
| `S3_ACCESS_KEY_ID` | required | Object-store access key |
| `S3_SECRET_ACCESS_KEY` | required | Object-store secret key |
| `S3_REGION` | `auto` | Object-store region |
| `S3_PREFIX` | empty | Explicit object key prefix |
| `S3_ALLOW_HTTP` | `false` | Allow an insecure object-store endpoint |
| `SLATEDB_CACHE_DIR` | `/tmp/lix-server-slatedb-cache` | Local cache root |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | unset | Optional OTLP/HTTP trace endpoint |

`S3_PREFIX` is part of the persistent object layout. Set it explicitly and do
not change it for an existing deployment. Cache settings affect only local,
rebuildable data.

## Build the image

Use the repository root as the Docker build context:

```sh
docker build -f packages/server/Dockerfile -t lix-server .
```

LixRay-specific public routing, Supabase policy, demo identities, MCP routes,
and product analytics remain in LixRay's gateway. They are intentionally not
part of this reference implementation.
