mod runtime_prep;

pub mod timestamp;
pub mod uuid_v7;
pub use crate::contracts::{LixFunctionProvider, SharedFunctionProvider};

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemFunctionProvider;

impl LixFunctionProvider for SystemFunctionProvider {
    fn uuid_v7(&mut self) -> String {
        uuid_v7::uuid_v7()
    }

    fn timestamp(&mut self) -> String {
        timestamp::timestamp()
    }
}
