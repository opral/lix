//! Overlay precedence resolution over tracked and untracked live state.

#[cfg(test)]
use async_trait::async_trait;

#[cfg(test)]
mod resolve;

pub use crate::contracts::{
    EffectiveRow, EffectiveRowIdentity, EffectiveRowRequest, EffectiveRowSet, EffectiveRowState,
    EffectiveRowsRequest, LaneResult, OverlayLane,
};

#[cfg(test)]
#[async_trait(?Send)]
pub trait EffectiveRowsResolver {
    async fn resolve_effective_rows(
        &self,
        request: &EffectiveRowsRequest,
    ) -> Result<EffectiveRowSet, crate::LixError>;
}

#[cfg(test)]
mod tests;
