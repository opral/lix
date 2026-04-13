mod runtime_provider;
mod timestamp;
pub mod uuid_v7;

use crate::contracts::LixFunctionProvider;
pub(crate) use runtime_provider::RuntimeFunctionProvider;
pub(crate) use timestamp::timestamp as current_timestamp;

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemFunctionProvider;

impl LixFunctionProvider for SystemFunctionProvider {
    fn uuid_v7(&mut self) -> String {
        uuid_v7::uuid_v7()
    }

    fn timestamp(&mut self) -> String {
        current_timestamp()
    }
}
