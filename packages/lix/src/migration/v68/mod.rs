//! Frozen readers for repository protocol v68.
//!
//! These codecs belong to the v68-to-current migration boundary. They must
//! not be reused by the live changelog read path.

mod changelog_codec;
mod columnar;
mod commit_delta;
mod commit_state_manifest;
mod current_state_data_part;
mod hot_state;
mod mutation_directory;
mod replacement_part;
mod standalone;

pub(in crate::migration) use changelog_codec::V68ChangeRecord;
pub(super) use changelog_codec::decode_change_record;
pub(in crate::migration) use columnar::load_columnar_changes;
pub(in crate::migration) use commit_delta::{
    CommitDeltaMember, CommitDeltaPayloadDescriptor, CommitDeltaSegmentBounds,
    decode_commit_delta_segment, decode_key,
};
pub(in crate::migration) use commit_state_manifest::load_commit_state_manifest;
pub(in crate::migration) use hot_state::{
    V68HotStateSlot, V68HotStateValue, V68WorkingDiffBaseline, V68WorkingDiffSlot,
    V68WorkingDiffVersion, decode_hot_state_value,
};
pub(in crate::migration) use replacement_part::{ReplacementPartRow, decode_replacement_part};
pub(in crate::migration) use standalone::{V68StandaloneChange, preflight_standalone_changelog};
