//! Overlay precedence resolution over tracked and untracked live state.

mod contracts;
mod resolve;

pub use contracts::{
    EffectiveRow, EffectiveRowIdentity, EffectiveRowRequest, EffectiveRowSet, EffectiveRowState,
    EffectiveRowsRequest, LaneResult, OverlayLane, ReadContext,
};
pub use resolve::{
    overlay_lanes, overlay_lanes_for_version, resolve_effective_row, resolve_effective_rows,
};

#[cfg(test)]
mod tests;
