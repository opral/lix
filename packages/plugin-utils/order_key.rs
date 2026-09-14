// Compatibility import for plugins sharing the SDK's ordering implementation.
#[path = "../lix/src/plugin/ordering/key.rs"]
mod key;
pub use key::OrderKey;
