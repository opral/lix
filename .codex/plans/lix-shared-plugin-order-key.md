# Shared plugin order-key implementation

Markdown, text and Excalidraw compile one private source module from
`packages/plugin-utils/order_key.rs`. It is byte-identical to their previous
three copies (SHA-256 `237b572648179c5516827218a4855562e64bc7c33c95b139345cbddc3f7f1d94`).
Source-only inclusion retains the compilation/optimization boundary and guest
compatibility without a public crate API or host dependency. All three plugins
are unpublished workspace crates; their Rust dep-info tracks the included file.

Two 285-line copies are removed, including two copies of the same nine tests.
The retained tests still compile in each consuming plugin. Production reduction
is 328 lines minus three module-path attributes = 325 lines. The README and this
plan are excluded from that production count.

Independent subagent review found no correctness or packaging issues. Standalone
nine-test suite passes and the shared module compiles for wasm32-wasip2. Optimized
LLVM comparison remaps both filenames identically so panic-location strings do
not obscure algorithm/code-generation equality. Fresh consuming-plugin validation: `cargo nextest run -p plugin_markdown -p plugin_text -p plugin_excalidraw --no-fail-fast --test-threads 8` passed 231 tests (one existing ignored test). `cargo build --target wasm32-wasip2 -p plugin_markdown -p plugin_text -p plugin_excalidraw` passed; both logs explicitly compiled all three plugin roots from this checkout. Filename-remapped optimized LLVM IR is byte-identical, so no runtime algorithm or code-generation difference is claimed.
