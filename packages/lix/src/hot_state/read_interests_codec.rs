//! Lossless native recipe identity. Public RowPk JSON intentionally erases
//! UUID/text/bytes distinctions and must never encode durable read interests.
use crate::hot_state::{HotStateFilter, HotStateScanRequest};
use crate::row_pk::RowPk;
use crate::tracked_state::{RowPkRangeBound, TrackedStateFilter};
use serde::{Deserialize, Serialize};

pub(super) mod key {
    use super::*;
    pub(crate) fn serialize<S: serde::Serializer>(
        value: &RowPk,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .as_typed_json_array_value()
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
    pub(crate) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<RowPk, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        if !value.as_array().is_some_and(|parts| {
            parts.iter().all(|part| {
                part.as_object().is_some_and(|part| {
                    part.len() == 2 && part.contains_key("type") && part.contains_key("value")
                })
            })
        }) {
            return Err(serde::de::Error::custom(
                "native key components require exactly type and value",
            ));
        }
        RowPk::from_typed_json_array_value(&value).map_err(serde::de::Error::custom)
    }
}
#[derive(Serialize, Deserialize)]
struct NativeKey(#[serde(with = "key")] RowPk);
mod keys {
    use super::*;
    pub(super) fn serialize<S: serde::Serializer>(
        values: &[RowPk],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct KeyRef<'a>(#[serde(with = "key")] &'a RowPk);
        use serde::ser::SerializeSeq;
        let mut sequence = serializer.serialize_seq(Some(values.len()))?;
        for key in values {
            sequence.serialize_element(&KeyRef(key))?;
        }
        sequence.end()
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<RowPk>, D::Error> {
        Ok(Vec::<NativeKey>::deserialize(deserializer)?
            .into_iter()
            .map(|key| key.0)
            .collect())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeBound {
    #[serde(with = "key")]
    row_pk: RowPk,
    inclusive: bool,
}
mod bound {
    use super::*;
    pub(super) fn serialize<S: serde::Serializer>(
        value: &Option<RowPkRangeBound>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value
            .as_ref()
            .map(|value| NativeBound {
                row_pk: value.row_pk.clone(),
                inclusive: value.inclusive,
            })
            .serialize(serializer)
    }
    pub(super) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<RowPkRangeBound>, D::Error> {
        Ok(
            Option::<NativeBound>::deserialize(deserializer)?.map(|value| RowPkRangeBound {
                row_pk: value.row_pk,
                inclusive: value.inclusive,
            }),
        )
    }
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "HotStateFilter", deny_unknown_fields)]
struct NativeHotFilter {
    rows: crate::hot_state::HotStateRowFilter,
    schema_keys: Vec<String>,
    #[serde(with = "keys")]
    row_pks: Vec<RowPk>,
    #[serde(with = "bound")]
    row_pk_lower: Option<RowPkRangeBound>,
    #[serde(with = "bound")]
    row_pk_upper: Option<RowPkRangeBound>,
    branch_ids: Vec<String>,
    file_ids: Vec<crate::NullableKeyFilter<String>>,
    untracked: Option<bool>,
    constraints: Vec<crate::hot_state::ScanConstraint>,
    declared_column_eq: Option<crate::hot_state::DeclaredColumnEq>,
    declared_column_range: Option<Box<crate::hot_state::DeclaredColumnRange>>,
    include_tombstones: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "HotStateScanRequest", deny_unknown_fields)]
pub(super) struct NativeScan {
    #[serde(with = "NativeHotFilter")]
    filter: HotStateFilter,
    projection: crate::hot_state::HotStateProjection,
    limit: Option<usize>,
}
#[derive(Serialize, Deserialize)]
#[serde(remote = "TrackedStateFilter", deny_unknown_fields)]
pub(super) struct NativeTrackedFilter {
    schema_keys: Vec<String>,
    #[serde(with = "keys")]
    row_pks: Vec<RowPk>,
    #[serde(with = "bound")]
    row_pk_lower: Option<RowPkRangeBound>,
    #[serde(with = "bound")]
    row_pk_upper: Option<RowPkRangeBound>,
    file_ids: Vec<crate::NullableKeyFilter<String>>,
    include_tombstones: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hot_state::read_interests::{
        DiffInterestEndpoint, ExactReadIdentity, FilePathInterest, InterestDomain,
        LogicalReadInterest, ReadInterestRegistry,
    };
    fn identities() -> Vec<RowPk> {
        let uuid = "00000000-0000-7000-8000-000000000123";
        vec![RowPk::uuid_from_canonical(uuid).unwrap(),RowPk::single(uuid),
            RowPk::from_typed_json_array_value(&serde_json::json!([{"type":"bytes","value":"AQID"}])).unwrap(),
            RowPk::single("AQID"),
            RowPk::from_typed_json_array_value(&serde_json::json!([{"type":"integer","value":i64::MIN}])).unwrap(),
            RowPk::from_typed_json_array_value(&serde_json::json!([{"type":"integer","value":i64::MAX}])).unwrap(),
            RowPk::from_typed_json_array_value(&serde_json::json!([{"type":"uuid","value":uuid},{"type":"string","value":uuid},{"type":"bytes","value":"AQID"},{"type":"integer","value":i64::MIN}])).unwrap()]
    }
    #[test]
    fn every_nested_native_identity_roundtrips_without_public_json_erasure() {
        let keys = identities();
        let request = HotStateScanRequest {
            filter: HotStateFilter {
                row_pks: keys.clone(),
                row_pk_lower: Some(RowPkRangeBound {
                    row_pk: keys[0].clone(),
                    inclusive: true,
                }),
                row_pk_upper: Some(RowPkRangeBound {
                    row_pk: keys[6].clone(),
                    inclusive: false,
                }),
                ..Default::default()
            },
            ..Default::default()
        };
        let recipes = vec![
            LogicalReadInterest::Scan {
                request: request.clone(),
                domain: InterestDomain::Combined,
            },
            LogicalReadInterest::FileContent {
                request: request.clone(),
                file_ids: None,
                directory_ids: None,
                root_directory: false,
                indexed: false,
                path_predicate: FilePathInterest::All,
                byte_range: None,
            },
            LogicalReadInterest::Diff {
                branch_id: None,
                relation: "diff".into(),
                from: DiffInterestEndpoint::ActiveHead,
                to: DiffInterestEndpoint::WorkingCheckpoint,
                filter: TrackedStateFilter {
                    row_pks: keys.clone(),
                    row_pk_lower: request.filter.row_pk_lower.clone(),
                    row_pk_upper: request.filter.row_pk_upper.clone(),
                    ..Default::default()
                },
                retain_payloads: true,
                projected_columns: vec![],
                limit: None,
            },
            LogicalReadInterest::Exact {
                rows: keys
                    .iter()
                    .map(|key| ExactReadIdentity {
                        schema_key: "schema".into(),
                        branch_id: crate::GLOBAL_BRANCH_ID.into(),
                        file_id: None,
                        row_pk: key.clone(),
                    })
                    .collect(),
                projection: Default::default(),
                untracked: None,
                include_tombstones: false,
            },
        ];
        for recipe in recipes {
            let bytes = serde_json::to_vec(&recipe).unwrap();
            assert_eq!(
                serde_json::from_slice::<LogicalReadInterest>(&bytes).unwrap(),
                recipe
            );
        }
        assert_eq!(
            serde_json::to_value(&keys[0]).unwrap(),
            serde_json::to_value(&keys[1]).unwrap(),
            "public JSON remains intentionally unchanged"
        );
    }
    #[test]
    fn registry_restore_preserves_distinct_uuid_and_text_recipes() {
        let registry = ReadInterestRegistry::new(16, 64 * 1024);
        for key in identities() {
            registry
                .register(LogicalReadInterest::Exact {
                    rows: vec![ExactReadIdentity {
                        schema_key: "schema".into(),
                        branch_id: crate::GLOBAL_BRANCH_ID.into(),
                        file_id: None,
                        row_pk: key,
                    }],
                    projection: Default::default(),
                    untracked: None,
                    include_tombstones: false,
                })
                .unwrap();
        }
        let before = registry.snapshot().unwrap();
        assert_eq!(before.interests.len(), 7);
        let encoded = serde_json::to_vec(
            &before
                .interests
                .iter()
                .map(|recipe| recipe.as_ref())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        registry
            .merge_persisted(serde_json::from_slice(&encoded).unwrap())
            .unwrap();
        assert_eq!(registry.snapshot().unwrap().interests, before.interests);
        let mut erased = serde_json::to_value(before.interests[0].as_ref()).unwrap();
        erased["rows"][0]["rowPk"] = serde_json::json!(["00000000-0000-7000-8000-000000000123"]);
        assert!(
            serde_json::from_value::<LogicalReadInterest>(erased).is_err(),
            "old lossy wire must never be guessed into native identity"
        );
    }
}
