use crate::LixError;

use super::table::{Capability, PublicSurface, PublicTableContracts};
use super::DmlOperation;

pub(crate) fn validate_table_operation(
    surface: &PublicSurface,
    operation: DmlOperation,
    contracts: &PublicTableContracts,
) -> Result<(), LixError> {
    let Some(contract) = contracts.get(surface) else {
        return Ok(());
    };
    match contract.operation(operation) {
        Capability::Allowed => Ok(()),
        Capability::ReadOnly(hint) => {
            let message = if surface.name().ends_with("_history") {
                format!(
                    "DML cannot write read-only history view '{}'",
                    surface.name()
                )
            } else {
                format!(
                    "{} {} is not allowed because the SQL surface is read-only",
                    operation.as_str(),
                    surface.name()
                )
            };
            Err(LixError::new(LixError::CODE_READ_ONLY, message).with_hint(hint))
        }
        Capability::Unsupported(hint) => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            format!(
                "{} {} is not supported by Lix SQL",
                operation.as_str(),
                surface.name()
            ),
        )
        .with_hint(hint)),
    }
}
