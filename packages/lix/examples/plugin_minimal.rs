//! Minimal compiling Lix Component plugin.

use lix::plugin::{
    ColdUpdate, FileUpdate, OpenFile, Output, Plugin, RestoreFile, Result, RowUpdate,
};

struct MinimalPlugin;

impl Plugin for MinimalPlugin {
    fn open(_input: &OpenFile<'_>, _output: &mut Output<'_>) -> Result<()> {
        Ok(())
    }

    fn file_changed(_update: &FileUpdate<'_>, _output: &mut Output<'_>) -> Result<()> {
        Ok(())
    }

    fn rows_changed(_update: &mut RowUpdate<'_>, _output: &mut Output<'_>) -> Result<()> {
        Ok(())
    }

    fn restore(_input: &mut RestoreFile<'_>, _output: &mut Output<'_>) -> Result<()> {
        Ok(())
    }

    fn cold_file_changed(_update: &mut ColdUpdate<'_>, _output: &mut Output<'_>) -> Result<()> {
        Ok(())
    }
}

lix::plugin::export!(MinimalPlugin);

// Cargo builds examples as binaries during `cargo test --all-targets`. The
// packaged-plugin qualification copies this source into a downstream
// `wasm32-wasip2` library, where no binary entry point is needed.
#[cfg(not(all(target_arch = "wasm32", target_os = "wasi", target_env = "p2")))]
fn main() {}
