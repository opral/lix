//! Conformance harness for backend_v2 implementations.
//!
//! The harness is colocated with the experimental API for now. Once backend_v2
//! is stable, rs-sdk can re-export this as the public backend author test kit.

mod factory;
#[allow(dead_code)]
mod fixtures;
#[allow(dead_code)]
mod model;
mod projection;
mod pushdown;
mod runner;
mod scan;
mod baseline;
mod write;

pub use factory::{BackendFactory, BackendTestConfig};
pub use runner::{run_backend_conformance, ConformanceReport, ConformanceTest};
