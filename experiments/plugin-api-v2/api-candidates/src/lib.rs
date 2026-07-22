//! Compileable SDK facades used only for the plugin API AX evaluation.
//!
//! These traits deliberately omit runtime implementation. They normalize the
//! semantic vocabulary while allowing the evaluation to compare lifecycle and
//! data-flow models. See `../ax-eval/tasks` for the format tasks.

mod types;

pub mod candidate_a;
pub mod candidate_b;
pub mod candidate_b_refined;
pub mod candidate_c;
pub mod candidate_d;
pub mod current_v1;

pub use types::*;
