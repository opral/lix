//! Minimal real Component guest used to validate v3 host arena ownership.
//!
//! This is infrastructure scaffolding, not one of the four format ports.

#![cfg_attr(not(target_family = "wasm"), allow(dead_code))]

use lix_plugin_api_v3 as sdk;

struct FixturePlugin;

impl sdk::FormatPlugin for FixturePlugin {
    fn open_file(budget: &sdk::Budget, input: sdk::OpenFileInput) -> sdk::Result<sdk::FileResult> {
        let len = input.accepted.file_len();
        let mut offset = 0_u64;
        while offset < len {
            let requested = u32::try_from((len - offset).min(64 * 1024))
                .map_err(|_| sdk::Error::internal("fixture page length overflowed"))?;
            let page = input
                .accepted
                .read_file(budget, offset, requested)
                .map_err(arena_error)?;
            if page.len() != requested as usize {
                return Err(sdk::Error::invalid_input(
                    "fixture root returned a short page",
                ));
            }
            offset += u64::from(requested);
        }
        input
            .successor
            .put_state(budget, b"fixture/length", &len.to_le_bytes())
            .map_err(arena_error)?;
        Ok(sdk::FileResult {
            successor: input.successor,
            changes: vec![fixture_change(len)],
        })
    }

    fn file_changed(budget: &sdk::Budget, input: sdk::FileUpdate) -> sdk::Result<sdk::FileResult> {
        let len = input.successor.file_len().map_err(arena_error)?;
        for edit in &input.edits {
            if len == 0 {
                continue;
            }
            let offset = edit.offset.min(len - 1);
            let page = input
                .successor
                .read_file(budget, offset, 1)
                .map_err(arena_error)?;
            if page.len() != 1 {
                return Err(sdk::Error::invalid_input(
                    "fixture successor returned a short affected page",
                ));
            }
        }
        input
            .successor
            .put_state(budget, b"fixture/length", &len.to_le_bytes())
            .map_err(arena_error)?;
        Ok(sdk::FileResult {
            successor: input.successor,
            changes: vec![fixture_change(len)],
        })
    }

    fn open_entities(
        _budget: &sdk::Budget,
        input: sdk::OpenEntitiesInput,
    ) -> sdk::Result<sdk::EntityResult> {
        Ok(sdk::EntityResult {
            successor: input.successor,
            edits: Vec::new(),
        })
    }

    fn entities_changed(
        _budget: &sdk::Budget,
        input: sdk::EntityUpdate,
    ) -> sdk::Result<sdk::EntityResult> {
        Ok(sdk::EntityResult {
            successor: input.successor,
            edits: Vec::new(),
        })
    }
}

fn fixture_change(len: u64) -> sdk::EntityChange {
    sdk::EntityChange {
        schema_key: "fixture".to_owned(),
        entity_pk: vec!["root".to_owned()],
        snapshot: Some(format!("{{\"length\":{len}}}").into_bytes()),
        format_only: false,
    }
}

fn arena_error(error: sdk::lix::plugin::arena::ArenaError) -> sdk::Error {
    use sdk::lix::plugin::arena::ArenaError;
    match error {
        ArenaError::InvalidRange => sdk::Error::invalid_input("invalid arena range"),
        ArenaError::RecordTooLarge(bytes) => sdk::Error::RecordTooLarge(bytes),
        ArenaError::LimitExceeded(message) => sdk::Error::LimitExceeded(message),
        ArenaError::DeadlineExceeded => sdk::Error::DeadlineExceeded,
        ArenaError::Unavailable(message) => sdk::Error::internal(message),
    }
}

#[cfg(target_family = "wasm")]
lix_plugin_api_v3::export_v3!(FixturePlugin);
