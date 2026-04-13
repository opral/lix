//! Routing stage ownership.
//!
//! Route selection and lowering-capability decisions live here so they do not
//! masquerade as logical optimization.

mod public_reads;
mod registry;

#[cfg(test)]
pub(crate) use public_reads::forbid_broad_routing_for_test;
pub(crate) use public_reads::{
    classify_public_read_plan_kind, route_broad_public_read_statement_with_known_live_layouts,
    PublicReadPlanKind,
};
pub use public_reads::{delay_broad_routing_for_test, BroadRoutingDelayForTestGuard};
pub(crate) use registry::RoutingPassTrace;
