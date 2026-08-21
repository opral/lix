//! Physical v68 caches deleted by the v69 hard cut.

use crate::storage_adapter::{StorageSpace, StorageSpaceId, ValueSemantics};

pub(super) const CERTIFIED_ROW_BATCH_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_001f),
    "hot_state.certified_row_batch.v1",
    ValueSemantics::Mutable,
);
pub(super) const CERTIFIED_ROW_BATCH_MANIFEST_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0021),
    "hot_state.certified_row_batch_manifest.v2",
    ValueSemantics::Mutable,
);
pub(super) const CERTIFIED_ROW_BATCH_PAGE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0022),
    "hot_state.certified_row_batch_page.v1",
    ValueSemantics::Mutable,
);
pub(super) const PLUGIN_CHECKPOINT_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0004_0026),
    "plugin.current_checkpoint.v2",
    ValueSemantics::Mutable,
);

pub(super) const ALL: &[StorageSpace] = &[
    CERTIFIED_ROW_BATCH_SPACE,
    CERTIFIED_ROW_BATCH_MANIFEST_SPACE,
    CERTIFIED_ROW_BATCH_PAGE_SPACE,
    PLUGIN_CHECKPOINT_SPACE,
];
