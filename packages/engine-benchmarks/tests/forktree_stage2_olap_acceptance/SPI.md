# Closed `AcceptancePhysicalLayout` SPI

The production candidate must expose exactly two benchmark-feature-only symbols from
`lix::storage_bench`:

```rust
pub trait AcceptancePhysicalLayout: private::Sealed {
    fn run_cli() -> Result<(), LixError>;
}

pub struct Stage2ProductionPhysicalLayout(/* private */);
```

`Stage2ProductionPhysicalLayout` is the sole implementation. Neither symbol may be exported without
`lix/storage-benches`; applications and public SDKs must not observe this surface.

`run_cli` accepts:

```text
control <rocksdb|slatedb> <10000|50000|500000>
corrupt <rocksdb|slatedb> 10000 <malformed_block|substituted_block>
```

The control path must seed and query through public Lix SQL/DataFusion. The SPI may observe the real
physical owner and inject a deterministic read fault, but it must not produce query rows, digest rows,
reconstruct a model, maintain a cache/index, or act as persistence authority.

Each invocation emits newline-delimited JSON. A control cell requires:

- one `identity` row with `spi=AcceptancePhysicalLayout/v1`,
  `owner=forktree-stage2-production`, and the exact 40-hex candidate head;
- one `query` and one `reopen` row for `pk_point`, `pk_range`, `column_projection`, `group_by`, and
  `simple_join`;
- one `storage` row for settled disk, maximum RSS, and query-phase writes.

Every query row contains `digest`, `result_rows`, `wall_us`, `cpu_us`, `alloc_bytes`,
`coherent_storage_reads`, `authenticated_block_batching`, `authenticated_blocks`,
`projection_before_row_allocation`, `backend_calls`, `physical_read_objects`,
`physical_read_bytes`, `write_objects`, and `write_bytes`.

Every corruption cell emits one `corruption` row containing the exact backend/rows/fault,
`fail_closed=true`, `error_class=corruption`, zero writes/bytes, and equal `disk_before`/`disk_after`.
The malformed fault changes authenticated bytes under the named object identity. The substituted fault
returns a different valid object's bytes under the requested identity. Both must traverse the ordinary
production reader and fail before result publication.

Exact `a12` deliberately has neither SPI symbol. Enabling the acceptance feature on `a12` is expected
to fail solely at the unresolved import in the benchmark bridge. Do not add a baseline implementation,
fallback, model, or public owner widening to make it compile.
