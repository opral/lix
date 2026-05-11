use std::collections::BTreeSet;

use crate::LixError;

use super::table::{PublicSurface, PublicTableContracts};

pub(crate) fn validate_update_assignments(
    surface: &PublicSurface,
    columns: Vec<String>,
    contracts: &PublicTableContracts,
) -> Result<(), LixError> {
    let Some(contract) = contracts.get(surface) else {
        return Ok(());
    };
    let mut seen = BTreeSet::new();
    for column in columns {
        if !seen.insert(column.clone()) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "update {} assigns column '{column}' more than once",
                    surface.name()
                ),
            ));
        }
        let Some(column_contract) = contract.column(&column) else {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "update {} references unknown column '{column}'",
                    surface.name()
                ),
            ));
        };
        if !column_contract.writable {
            return Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                format!(
                    "update {} cannot assign read-only column '{column}'",
                    surface.name()
                ),
            ));
        }
    }
    Ok(())
}
