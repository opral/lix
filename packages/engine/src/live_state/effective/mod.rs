//! Overlay precedence resolution over tracked and untracked live state.

#[cfg(test)]
mod resolve;

pub use crate::contracts::{
    EffectiveRow, EffectiveRowIdentity, EffectiveRowRequest, EffectiveRowSet, EffectiveRowState,
    EffectiveRowsRequest, LaneResult, OverlayLane,
};

#[cfg(test)]
mod tests;
