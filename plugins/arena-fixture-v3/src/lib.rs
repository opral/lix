//! Minimal real Component guest used to validate v3 host arena ownership.
//!
//! This is infrastructure scaffolding, not one of the four format ports.

#![cfg_attr(not(target_family = "wasm"), allow(dead_code))]

use lix_plugin_api_v3 as sdk;

struct FixturePlugin;

struct InvalidPacketSource {
    emitted: bool,
}

impl sdk::EntityChangePacketSource for InvalidPacketSource {
    fn next_packet(
        &mut self,
        _budget: &sdk::Budget,
        _max_bytes: u32,
    ) -> sdk::Result<Option<Vec<u8>>> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(b"invalid-v3-change-packet".to_vec()))
    }
}

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
            changes: vec![fixture_change(len)].into(),
        })
    }

    fn file_changed(budget: &sdk::Budget, input: sdk::FileUpdate) -> sdk::Result<sdk::FileResult> {
        let before_pages = input
            .before
            .scan_entity_pages(budget, None, 1)
            .map_err(arena_error)?;
        let successor_pages = input
            .successor
            .scan_entity_pages(budget, None, 1)
            .map_err(arena_error)?;
        let [before_page] = before_pages.pages.as_slice() else {
            return Err(sdk::Error::invalid_input(
                "fixture expected one predecessor semantic page",
            ));
        };
        let [successor_page] = successor_pages.pages.as_slice() else {
            return Err(sdk::Error::invalid_input(
                "fixture expected one successor semantic page",
            ));
        };
        if before_pages.next_key.is_some()
            || successor_pages.next_key.is_some()
            || before_page.first_key != successor_page.first_key
            || before_page.last_key != successor_page.last_key
            || before_page.record_count != 1
            || successor_page.record_count != 1
            || before_page.fingerprint.len() != 32
            || before_page.fingerprint != successor_page.fingerprint
        {
            return Err(sdk::Error::invalid_input(
                "fixture semantic page cursor was not stable",
            ));
        }
        if input.after_descriptor.path.as_deref() == Some("/invalid-output") {
            input
                .successor
                .put_state(budget, b"fixture/invalid", b"must-roll-back")
                .map_err(arena_error)?;
            return Ok(sdk::FileResult {
                successor: input.successor,
                changes: sdk::EntityChanges::from_packet_source(InvalidPacketSource {
                    emitted: false,
                }),
            });
        }
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
            changes: vec![fixture_change(len)].into(),
        })
    }

    fn open_entities(
        budget: &sdk::Budget,
        input: sdk::OpenEntitiesInput,
    ) -> sdk::Result<sdk::EntityResult> {
        if input.descriptor.path.as_deref() == Some("/invalid-edits") {
            input
                .successor
                .put_state(budget, b"fixture/invalid-edits", b"must-roll-back")
                .map_err(arena_error)?;
            return Ok(sdk::EntityResult {
                successor: input.successor,
                edits: vec![
                    sdk::ByteEdit {
                        offset: 0,
                        delete_len: 2,
                        insert: b"a".to_vec(),
                    },
                    sdk::ByteEdit {
                        offset: 1,
                        delete_len: 1,
                        insert: b"b".to_vec(),
                    },
                ],
            });
        }
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
