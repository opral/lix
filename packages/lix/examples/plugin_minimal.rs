//! Minimal compiling Lix Component plugin.

use lix::plugin::{
    ColdUpdate, EntityUpdate, FileUpdate, OpenFile, Output, Plugin, RestoreFile, Result,
};

struct MinimalPlugin;

impl Plugin for MinimalPlugin {
    fn open(_input: &OpenFile<'_>, _output: &mut Output<'_>) -> Result<()> {
        Ok(())
    }

    fn file_changed(_update: &FileUpdate<'_>, _output: &mut Output<'_>) -> Result<()> {
        Ok(())
    }

    fn entities_changed(_update: &mut EntityUpdate<'_>, _output: &mut Output<'_>) -> Result<()> {
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
