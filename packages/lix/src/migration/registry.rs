/// One directed repository-format migration edge.
///
/// Registration does not authorize migration during normal repository open;
/// the explicit offline migration API owns execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Migration {
    pub(crate) from_version: u32,
    pub(crate) to_version: u32,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        from_version: 68,
        to_version: 69,
    },
    Migration {
        from_version: 69,
        to_version: 70,
    },
    Migration {
        from_version: 70,
        to_version: 71,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_describes_format_edges() {
        assert_eq!(
            migration_from(68),
            Some(&Migration {
                from_version: 68,
                to_version: 69,
            })
        );
        assert_eq!(
            migration_from(69),
            Some(&Migration {
                from_version: 69,
                to_version: 70,
            })
        );
        assert_eq!(
            migration_from(70),
            Some(&Migration {
                from_version: 70,
                to_version: 71,
            })
        );
        assert_eq!(registered_migrations().len(), 3);
        assert_eq!(migration_from(crate::init::CURRENT_FORMAT_VERSION), None);
    }
}
