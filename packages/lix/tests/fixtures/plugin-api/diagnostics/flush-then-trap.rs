//! Rebuild: rustc --edition 2024 --target wasm32-wasip2 -C opt-level=s -C panic=abort -C strip=symbols
//! flush-then-trap.rs -o flush-then-trap.wasm
use std::io::Write;
fn main() {
    // The synchronous WASI linker used to call Tokio's block_on during this
    // flush, panicking the host instead of returning the subsequent guest trap.
    let _ = std::io::stderr().flush();
    panic!("intentional guest trap after diagnostic flush");
}
