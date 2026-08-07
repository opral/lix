//! Authenticated persistent physical owner for tracked repository state.
//!
//! This root module is the only boundary other engine owners may import. The
//! implementation children stay private so object encodings, tree mechanics,
//! selector fencing, and reachability cannot become competing authorities.

mod codec;
mod model;
mod object;
mod publication;
mod tree;
mod view;

pub(crate) use model::{
    BlobChunkRefV1, BlobChunkV1, BranchSelectorV1, BranchSnapshotV1, CanonicalBranchId,
    CanonicalUploadId, ChangeCatalogEntry, ChangeCatalogOwner, ChangeId, ChangeObjectV1,
    CommitCatalogEntry, CommitId, CommitObjectV1, GlobalSelectorV1, RepositoryRootV1, UploadPartV1,
    UploadProgressV1, UploadSelectorV1,
};
pub(crate) use object::{OBJECT_SPACE, ObjectId};
pub(crate) use publication::{PreparedPublication, SelectorExpectation};
pub(crate) use tree::{
    RECEIPT_TREE_FANOUT, RECEIPT_TREE_LEAF_ENTRIES, ReceiptTreeEdit, ReceiptTreeRoot,
};
pub(crate) use view::{CoherentView, open_coherent_view};

#[cfg(test)]
mod tests;
