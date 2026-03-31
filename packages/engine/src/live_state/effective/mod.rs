//! Overlay precedence resolution over tracked and untracked live state.

mod resolve;

pub use crate::contracts::artifacts::{
    EffectiveRow, EffectiveRowIdentity, EffectiveRowRequest, EffectiveRowSet, EffectiveRowState,
    EffectiveRowsRequest, LaneResult, OverlayLane,
};

#[cfg(test)]
mod tests;
