//! Completeness of native current-state inputs in a partial replica.
//!
//! Rows alone do not establish coverage: an empty fetched scope can be
//! complete, while a nonempty local table can still be incomplete. Coverage
//! is installed only with the final, verified scope snapshot, in the same
//! publication as its rows and resume boundary. This module deliberately
//! does not infer completeness from row counts or query-result caches.

use serde::{Deserialize, Serialize};

use crate::row_pk::RowPk;

/// Identity of one coverage registry, scoped to repository, authorization and
/// catalog interpretation. The caller reads this registry and native data
/// through the same coherent storage snapshot.
///
/// Ordinary local commits and gap-free remote updates preserve this epoch: they
/// update rows, indexes and completeness atomically. Reset, schema invalidation
/// or incompatible authorization changes rotate it. It is not a branch-head
/// ID or local mutation counter; rotating it on each write would make every
/// subsequent read cold. The applied head/cursor belongs to the registry, not
/// to individual scope ownership records.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct CurrentCoverageVersion {
    pub(super) repository_id: String,
    pub(super) authorization_scope: String,
    pub(super) epoch: String,
}

/// `None` is an exact fileless domain, not the wildcard domain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum CurrentFileScope {
    All,
    Exact(Option<String>),
}

/// Typed native primary keys retain their ordering and identity. Transport
/// stringification must not conflate integer keys with textual keys.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct CurrentKeyBound {
    #[serde(with = "typed_row_pk")]
    pub(super) key: RowPk,
    pub(super) inclusive: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum CurrentKeyScope {
    All,
    Exact(#[serde(with = "typed_row_pk")] RowPk),
    Range {
        lower: Option<CurrentKeyBound>,
        upper: Option<CurrentKeyBound>,
    },
}

/// A complete native input domain. Every retained row in the domain is
/// complete; content-addressed file bytes have separate residency tracking.
/// `untracked: None` includes both tracked and current-only rows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct CurrentDataScope {
    pub(super) branch_id: String,
    pub(super) schema_key: String,
    pub(super) file: CurrentFileScope,
    pub(super) keys: CurrentKeyScope,
    pub(super) untracked: Option<bool>,
}

impl CurrentDataScope {
    /// Conservative containment, not arbitrary SQL predicate implication.
    /// Multiple point fetches never certify a range or a whole relation.
    pub(super) fn contains(&self, requested: &Self) -> bool {
        self.branch_id == requested.branch_id
            && self.schema_key == requested.schema_key
            && (self.file == CurrentFileScope::All || self.file == requested.file)
            && (self.untracked.is_none() || self.untracked == requested.untracked)
            && self.keys.contains(&requested.keys)
    }
}

impl CurrentKeyScope {
    fn contains(&self, requested: &Self) -> bool {
        match (self, requested) {
            (Self::All, _) => true,
            (Self::Exact(known), Self::Exact(requested)) => known == requested,
            (Self::Range { lower, upper }, Self::Exact(key)) => {
                lower
                    .as_ref()
                    .is_none_or(|bound| key > &bound.key || (bound.inclusive && key == &bound.key))
                    && upper.as_ref().is_none_or(|bound| {
                        key < &bound.key || (bound.inclusive && key == &bound.key)
                    })
            }
            (
                Self::Range { lower, upper },
                Self::Range {
                    lower: requested_lower,
                    upper: requested_upper,
                },
            ) => {
                lower_contains(lower.as_ref(), requested_lower.as_ref())
                    && upper_contains(upper.as_ref(), requested_upper.as_ref())
            }
            _ => false,
        }
    }
}

fn lower_contains(known: Option<&CurrentKeyBound>, requested: Option<&CurrentKeyBound>) -> bool {
    match (known, requested) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(known), Some(requested)) => {
            known.key < requested.key
                || (known.key == requested.key && (known.inclusive || !requested.inclusive))
        }
    }
}

fn upper_contains(known: Option<&CurrentKeyBound>, requested: Option<&CurrentKeyBound>) -> bool {
    match (known, requested) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(known), Some(requested)) => {
            known.key > requested.key
                || (known.key == requested.key && (known.inclusive || !requested.inclusive))
        }
    }
}

/// One retained scope owns its completeness independently of overlapping
/// scopes. Removing one owner cannot erase another owner's coverage.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct CompleteCurrentScope {
    pub(super) owner: String,
    pub(super) version: CurrentCoverageVersion,
    pub(super) scope: CurrentDataScope,
}

pub(super) fn current_scope_is_covered(
    retained: &[CompleteCurrentScope],
    version: &CurrentCoverageVersion,
    requested: &CurrentDataScope,
) -> bool {
    retained
        .iter()
        .any(|entry| &entry.version == version && entry.scope.contains(requested))
}

// RowPk's ordinary serde representation is an API projection which erases
// UUID/bytes types. Coverage must retain native identity and key ordering.
mod typed_row_pk {
    use super::*;

    pub(super) fn serialize<S: serde::Serializer>(
        key: &RowPk,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        key.as_typed_json_array_value()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<RowPk, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        RowPk::from_typed_json_array_value(&value).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(value: i64) -> RowPk {
        RowPk::from_schema_values(&[lix_schema::Value::Int8(value)]).expect("integer key")
    }

    fn scope(keys: CurrentKeyScope) -> CurrentDataScope {
        CurrentDataScope {
            branch_id: "branch-a".into(),
            schema_key: "example".into(),
            file: CurrentFileScope::Exact(None),
            keys,
            untracked: Some(false),
        }
    }

    fn version() -> CurrentCoverageVersion {
        CurrentCoverageVersion {
            repository_id: "repository-a".into(),
            authorization_scope: "account-a".into(),
            epoch: "coverage-a".into(),
        }
    }

    fn retained(keys: CurrentKeyScope) -> CompleteCurrentScope {
        CompleteCurrentScope {
            owner: "query-a".into(),
            version: version(),
            scope: scope(keys),
        }
    }

    #[test]
    fn fetched_points_do_not_certify_count_or_unseen_keys() {
        let fetched = (0..10)
            .map(|value| retained(CurrentKeyScope::Exact(key(value))))
            .collect::<Vec<_>>();
        assert!(!current_scope_is_covered(
            &fetched,
            &version(),
            &scope(CurrentKeyScope::All)
        ));
        assert!(!current_scope_is_covered(
            &fetched,
            &version(),
            &scope(CurrentKeyScope::Exact(key(10)))
        ));
        assert!(current_scope_is_covered(
            &fetched,
            &version(),
            &scope(CurrentKeyScope::Exact(key(3)))
        ));
    }

    #[test]
    fn complete_empty_domain_certifies_absence_without_row_count() {
        // Completeness comes from the verified scope publication, even when
        // its accompanying row set is empty. No row-count heuristic is used.
        assert!(current_scope_is_covered(
            &[retained(CurrentKeyScope::All)],
            &version(),
            &scope(CurrentKeyScope::Exact(key(42)))
        ));
        assert!(!current_scope_is_covered(
            &[],
            &version(),
            &scope(CurrentKeyScope::Exact(key(42)))
        ));
    }

    #[test]
    fn pagination_does_not_certify_unfetched_boundary() {
        let page = CurrentKeyScope::Range {
            lower: Some(CurrentKeyBound {
                key: key(0),
                inclusive: true,
            }),
            upper: Some(CurrentKeyBound {
                key: key(10),
                inclusive: false,
            }),
        };
        assert!(page.contains(&CurrentKeyScope::Exact(key(0))));
        assert!(page.contains(&CurrentKeyScope::Exact(key(9))));
        assert!(!page.contains(&CurrentKeyScope::Exact(key(10))));
        assert!(!page.contains(&CurrentKeyScope::All));
        assert!(!page.contains(&CurrentKeyScope::Range {
            lower: None,
            upper: Some(CurrentKeyBound {
                key: key(5),
                inclusive: true,
            }),
        }));
        assert!(!page.contains(&CurrentKeyScope::Range {
            lower: Some(CurrentKeyBound {
                key: key(0),
                inclusive: true,
            }),
            upper: Some(CurrentKeyBound {
                key: key(10),
                inclusive: true,
            }),
        }));
    }

    #[test]
    fn coverage_never_crosses_branch_schema_file_or_tracking_domain() {
        let known = scope(CurrentKeyScope::All);
        for requested in [
            CurrentDataScope {
                branch_id: "branch-b".into(),
                ..known.clone()
            },
            CurrentDataScope {
                schema_key: "other".into(),
                ..known.clone()
            },
            CurrentDataScope {
                file: CurrentFileScope::Exact(Some("file".into())),
                ..known.clone()
            },
            CurrentDataScope {
                file: CurrentFileScope::All,
                ..known.clone()
            },
            CurrentDataScope {
                untracked: None,
                ..known.clone()
            },
            CurrentDataScope {
                untracked: Some(true),
                ..known.clone()
            },
        ] {
            assert!(!known.contains(&requested));
        }
    }

    #[test]
    fn incompatible_registry_context_does_not_certify_data() {
        let entry = retained(CurrentKeyScope::All);
        for changed in [
            CurrentCoverageVersion {
                epoch: "coverage-b".into(),
                ..version()
            },
            CurrentCoverageVersion {
                authorization_scope: "account-b".into(),
                ..version()
            },
            CurrentCoverageVersion {
                repository_id: "repository-b".into(),
                ..version()
            },
        ] {
            assert!(!current_scope_is_covered(
                std::slice::from_ref(&entry),
                &changed,
                &scope(CurrentKeyScope::All)
            ));
        }
    }

    #[test]
    fn removing_an_overlap_owner_preserves_other_coverage() {
        let first = retained(CurrentKeyScope::All);
        let second = CompleteCurrentScope {
            owner: "query-b".into(),
            scope: scope(CurrentKeyScope::Exact(key(2))),
            ..first.clone()
        };
        let mut entries = vec![first, second];
        entries.retain(|entry| entry.owner != "query-a");
        assert!(current_scope_is_covered(
            &entries,
            &version(),
            &scope(CurrentKeyScope::Exact(key(2)))
        ));
        assert!(!current_scope_is_covered(
            &entries,
            &version(),
            &scope(CurrentKeyScope::Exact(key(3)))
        ));
    }
    #[test]
    fn durable_coverage_keeps_typed_composite_keys() {
        use crate::row_pk::RowPkComponent;
        let uuid = "01920000-0000-7000-8000-000000001499";
        let identity = RowPk::from_components(smallvec::smallvec![
            RowPkComponent::Uuid(
                crate::storage_codec::id_string::uuid_bytes_from_canonical(uuid).unwrap(),
            ),
            RowPkComponent::String(uuid.into()),
            RowPkComponent::Integer(-42),
            RowPkComponent::Bytes(bytes::Bytes::from_static(b"\0bytes")),
        ])
        .unwrap();
        for keys in [
            CurrentKeyScope::Exact(identity.clone()),
            CurrentKeyScope::Range {
                lower: Some(CurrentKeyBound {
                    key: identity.clone(),
                    inclusive: true,
                }),
                upper: None,
            },
        ] {
            let entry = retained(keys);
            let encoded = serde_json::to_vec(&entry).unwrap();
            let decoded: CompleteCurrentScope = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(decoded, entry);
            assert!(
                decoded
                    .scope
                    .keys
                    .contains(&CurrentKeyScope::Exact(identity.clone()))
            );
        }
        let known = CurrentKeyScope::Exact(key(-1));
        assert!(!known.contains(&CurrentKeyScope::Exact(RowPk::single("-1"))));
    }
}
