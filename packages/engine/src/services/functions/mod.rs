pub mod timestamp;
pub mod uuid_v7;

use crate::contracts::LixFunctionProvider;

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
