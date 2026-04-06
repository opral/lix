//! Home for future declarative filesystem projection definitions.
//!
//! The current filesystem SQL rendering helpers still live outside
//! `projections/*` because they are consumed by compiler and runtime code.
//! When filesystem surfaces are expressed as real `ProjectionTrait`
//! definitions, they should live here.
