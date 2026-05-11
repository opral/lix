use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;

use crate::schema::schema_key_from_definition;
use crate::LixError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Capability {
    Allowed,
    ReadOnly(&'static str),
    Unsupported(&'static str),
}

#[derive(Clone, Debug)]
pub(crate) struct ColumnContract {
    pub(crate) writable: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct TableContract {
    pub(crate) insert: Capability,
    pub(crate) update: Capability,
    pub(crate) delete: Capability,
    pub(crate) columns: BTreeMap<String, ColumnContract>,
}

impl TableContract {
    pub(crate) fn operation(&self, operation: super::DmlOperation) -> Capability {
        match operation {
            super::DmlOperation::Insert => self.insert,
            super::DmlOperation::Update => self.update,
            super::DmlOperation::Delete => self.delete,
        }
    }

    pub(crate) fn column(&self, column: &str) -> Option<&ColumnContract> {
        self.columns.get(column)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct PublicSurface {
    name: String,
}

impl PublicSurface {
    pub(crate) fn named(name: impl Into<String>) -> Self {
        Self {
            name: name.into().to_ascii_lowercase(),
        }
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PublicTableContracts {
    contracts: BTreeMap<String, TableContract>,
}

impl PublicTableContracts {
    pub(crate) fn new(visible_schemas: &[JsonValue]) -> Result<Self, LixError> {
        let mut contracts = builtin_contracts();
        for schema in visible_schemas {
            let schema_key = schema_key_from_definition(schema)?.schema_key;
            contracts.insert(
                format!("{}_history", schema_key.to_ascii_lowercase()),
                history_contract(),
            );
        }
        Ok(Self { contracts })
    }

    pub(crate) fn get(&self, surface: &PublicSurface) -> Option<&TableContract> {
        self.contracts.get(surface.name())
    }
}

fn builtin_contracts() -> BTreeMap<String, TableContract> {
    let mut contracts = BTreeMap::new();

    for table in [
        "lix_change",
        "lix_commit",
        "lix_commit_by_version",
        "lix_commit_edge",
        "lix_commit_edge_by_version",
        "lix_change_set",
        "lix_change_set_by_version",
        "lix_change_set_element",
        "lix_change_set_element_by_version",
    ] {
        contracts.insert(table.to_string(), commit_graph_contract());
    }

    for table in [
        "lix_state_history",
        "lix_file_history",
        "lix_directory_history",
    ] {
        contracts.insert(table.to_string(), history_contract());
    }

    contracts.insert(
        "lix_registered_schema".to_string(),
        TableContract {
            insert: Capability::Allowed,
            update: Capability::Allowed,
            delete: Capability::Unsupported(
                "lix_registered_schema deletion is not supported; register an amended schema instead",
            ),
            columns: columns(&["value", "lixcol_metadata", "lixcol_global", "lixcol_untracked"]),
        },
    );

    contracts.insert(
        "lix_key_value".to_string(),
        TableContract {
            insert: Capability::Allowed,
            update: Capability::Allowed,
            delete: Capability::Allowed,
            columns: columns(&["key", "value", "lixcol_metadata"]),
        },
    );

    contracts
}

fn commit_graph_contract() -> TableContract {
    TableContract {
        insert: Capability::ReadOnly(
            "Commit graph and changelog surfaces are read-only; Lix creates them when transactions commit.",
        ),
        update: Capability::ReadOnly(
            "Commit graph and changelog surfaces are read-only; Lix creates them when transactions commit.",
        ),
        delete: Capability::ReadOnly(
            "Commit graph and changelog surfaces are read-only; Lix creates them when transactions commit.",
        ),
        columns: BTreeMap::new(),
    }
}

fn history_contract() -> TableContract {
    TableContract {
        insert: Capability::ReadOnly(
            "History views are query-only; write to the live surface such as lix_state, lix_file, lix_directory, or the typed entity table.",
        ),
        update: Capability::ReadOnly(
            "History views are query-only; write to the live surface such as lix_state, lix_file, lix_directory, or the typed entity table.",
        ),
        delete: Capability::ReadOnly(
            "History views are query-only; write to the live surface such as lix_state, lix_file, lix_directory, or the typed entity table.",
        ),
        columns: BTreeMap::new(),
    }
}

fn columns(writable: &[&str]) -> BTreeMap<String, ColumnContract> {
    let writable = writable.iter().copied().collect::<BTreeSet<_>>();
    writable
        .into_iter()
        .map(|column| (column.to_string(), ColumnContract { writable: true }))
        .collect()
}
