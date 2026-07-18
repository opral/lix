# Lix server protocol

`lix_server_protocol::handler` exposes the canonical HTTP protocol for one
workspace-mode `lix_sdk::Lix` handle. A host owns authentication, workspace
routing, storage construction, and process lifecycle, then merges this handler
into its HTTP application.

```rust,no_run
use std::sync::Arc;
use axum::Router;
use lix_sdk::{OpenLixOptions, open_lix};

# async fn example() -> Result<(), lix_sdk::LixError> {
let lix = Arc::new(open_lix(OpenLixOptions::default()).await?);
let app = Router::new().merge(lix_server_protocol::handler(lix));
# let _ = app;
# Ok(())
# }
```

The handler owns `/lix/v1`, request validation, wire values, Lix error mapping,
and multiplexed observations. Host-specific routes such as authentication,
health checks, and compare-and-swap filesystem mutations stay outside it.
