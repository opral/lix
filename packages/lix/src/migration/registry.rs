/// One directed repository-format migration edge.
///
/// The engine-owned open path executes the complete registered chain inside a
/// hidden storage epoch before activation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Migration {
    pub(crate) from_version: u32,
    pub(crate) to_version: u32,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        from_version: 72,
        to_version: 73,
    },
    Migration {
        from_version: 73,
        to_version: 74,
    },
    Migration {
        from_version: 74,
        to_version: 75,
    },
    Migration {
        from_version: 75,
        to_version: 76,
    },
    Migration {
        from_version: 76,
        to_version: 77,
    },
];

pub(crate) fn registered_migrations() -> &'static [Migration] {
    MIGRATIONS
}

pub(crate) fn migration_from(from_version: u32) -> Option<&'static Migration> {
    MIGRATIONS
        .iter()
        .find(|migration| migration.from_version == from_version)
}

pub(crate) fn has_complete_migration_path(mut from_version: u32, to_version: u32) -> bool {
    while from_version < to_version {
        let Some(migration) = migration_from(from_version) else {
            return false;
        };
        if migration.to_version <= from_version || migration.to_version > to_version {
            return false;
        }
        from_version = migration.to_version;
    }
    from_version == to_version
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_describes_format_edges() {
        // The v75 chain starts at v72: older repositories carry commit-record
        // arities the in-place chain cannot traverse (see migrate_lix's gate).
        assert_eq!(migration_from(68), None);
        assert_eq!(migration_from(71), None);
        assert_eq!(
            migration_from(72),
            Some(&Migration {
                from_version: 72,
                to_version: 73,
            })
        );
        assert_eq!(
            migration_from(73),
            Some(&Migration {
                from_version: 73,
                to_version: 74,
            })
        );
        assert_eq!(
            migration_from(74),
            Some(&Migration {
                from_version: 74,
                to_version: 75,
            })
        );
        assert_eq!(
            migration_from(75),
            Some(&Migration {
                from_version: 75,
                to_version: 76,
            })
        );
        assert_eq!(
            migration_from(76),
            Some(&Migration {
                from_version: 76,
                to_version: 77,
            })
        );
        assert_eq!(registered_migrations().len(), 5);
        assert!(has_complete_migration_path(
            72,
            crate::init::CURRENT_FORMAT_VERSION
        ));
        assert!(!has_complete_migration_path(
            68,
            crate::init::CURRENT_FORMAT_VERSION
        ));
        assert_eq!(migration_from(crate::init::CURRENT_FORMAT_VERSION), None);
    }
}
