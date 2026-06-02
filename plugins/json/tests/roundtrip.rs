mod common;

use std::collections::BTreeMap;

use common::{
    active_state_from_changes, apply_changes_to_active_state, detect_changes_from_files,
    file_from_json,
};
use plugin_json_v2::exports::lix::plugin::api::Guest;
use plugin_json_v2::{DetectedChange, JsonPlugin, SCHEMA_KEY};
use serde_json::Value;

fn merge_latest_state_rows(changesets: Vec<Vec<DetectedChange>>) -> Vec<DetectedChange> {
    let mut latest = BTreeMap::new();
    for changes in changesets {
        for change in changes {
            if change.schema_key != SCHEMA_KEY {
                continue;
            }
            latest.insert(
                (change.schema_key.clone(), change.entity_pk.clone()),
                change,
            );
        }
    }
    latest.into_values().collect()
}

fn projected_changes_for_transition(before_json: &str, after_json: &str) -> Vec<DetectedChange> {
    let baseline = detect_changes_from_files(None, file_from_json(before_json))
        .expect("baseline detect_changes should succeed");
    let delta = detect_changes_from_files(
        Some(file_from_json(before_json)),
        file_from_json(after_json),
    )
    .expect("delta detect_changes should succeed");
    merge_latest_state_rows(vec![baseline, delta])
}

fn render_projection(changes: Vec<DetectedChange>) -> Value {
    let reconstructed = common::render_projection(changes).expect("render should succeed");
    serde_json::from_slice(&reconstructed).expect("reconstructed bytes should parse")
}

fn assert_projection_roundtrip(before_json: &str, after_json: &str) {
    let reconstructed_json =
        render_projection(projected_changes_for_transition(before_json, after_json));
    let expected_json: Value =
        serde_json::from_str(after_json).expect("expected JSON should parse");
    assert_eq!(reconstructed_json, expected_json);
}

#[test]
fn roundtrip_reconstructs_after_document() {
    assert_projection_roundtrip(
        r#"{"Name":"Samuel","address":{"city":"Berlin","zip":"10115"},"tags":["a","b","c"]}"#,
        r#"{"Name":"Sam","address":{"city":"Berlin"},"tags":["a","x"],"active":true}"#,
    );
}

#[test]
fn roundtrip_file_creation_from_empty_seed() {
    assert_projection_roundtrip(
        r"{}",
        r#"{"profile":{"name":"Anna"},"roles":["admin","editor"]}"#,
    );
}

#[test]
fn roundtrip_handles_numeric_object_keys() {
    assert_projection_roundtrip(r"{}", r#"{"foo":{"0":"x","1":"y"}}"#);
}

#[test]
fn roundtrip_handles_multi_delete_arrays() {
    assert_projection_roundtrip(r#"{"list":["a","b","c","d"]}"#, r#"{"list":["a"]}"#);
}

#[test]
fn roundtrip_preserves_pointer_escaped_keys() {
    assert_projection_roundtrip(
        r#"{"a/b":"old","tilde~key":"x"}"#,
        r#"{"a/b":"new","tilde~key":"y"}"#,
    );
}

#[test]
fn roundtrip_replacing_empty_object_in_array_index_keeps_neighbors() {
    assert_projection_roundtrip(r#"{"arr":[{}, "x"]}"#, r#"{"arr":[1, "x"]}"#);
}

#[test]
fn roundtrip_replacing_empty_array_with_empty_object_in_array_index_keeps_neighbors() {
    assert_projection_roundtrip(r#"{"arr":[[], "x"]}"#, r#"{"arr":[{}, "x"]}"#);
}

#[test]
fn roundtrip_deleting_non_empty_container_removes_descendants() {
    assert_projection_roundtrip(r#"{"a":{"b":1}}"#, r"{}");
}

#[test]
fn roundtrip_replacing_non_empty_container_with_scalar_removes_descendants() {
    assert_projection_roundtrip(r#"{"a":{"b":1}}"#, r"2");
}

#[test]
fn roundtrip_deleting_whole_object_property_removes_subtree_rows() {
    assert_projection_roundtrip(
        r#"{"keep":1,"obj":{"k":1,"nested":{"z":2}}}"#,
        r#"{"keep":1}"#,
    );
}

#[test]
fn roundtrip_deleting_whole_array_property_removes_subtree_rows() {
    assert_projection_roundtrip(r#"{"keep":1,"arr":[{"x":1},2,3]}"#, r#"{"keep":1}"#);
}

#[test]
fn roundtrip_deleting_nested_subtree_removes_descendants() {
    assert_projection_roundtrip(
        r#"{"a":{"b":{"c":1,"d":2},"e":3},"x":0}"#,
        r#"{"a":{"e":3},"x":0}"#,
    );
}

#[test]
fn roundtrip_replacing_root_array_with_scalar_removes_descendants() {
    assert_projection_roundtrip(r#"[{"a":1},{"b":2},3]"#, r"7");
}

#[test]
fn roundtrip_with_proto_like_keys_is_supported() {
    assert_projection_roundtrip(
        r#"{"__proto__":{"ok":true},"prototype":[1],"constructor":{"x":1}}"#,
        r#"{"__proto__":{"ok":false},"prototype":[1,2],"constructor":{"x":2}}"#,
    );
}

#[test]
fn roundtrip_handles_object_key_dash() {
    assert_projection_roundtrip(r"{}", r#"{"obj":{"-":{"x":1}}}"#);
}

#[test]
fn roundtrip_handles_pointer_escape_edge_keys() {
    assert_projection_roundtrip(r"{}", r#"{"":{"/":1,"~":2,"~1":3,"~0":4}}"#);
}

#[test]
fn roundtrip_replacing_root_object_with_array_allows_non_numeric_old_keys() {
    assert_projection_roundtrip(r#"{"~1":"x"}"#, r"[]");
}

#[test]
fn roundtrip_replacing_nested_object_with_array_allows_non_numeric_old_keys() {
    assert_projection_roundtrip(r#"{"x":{"~1":"v"}}"#, r#"{"x":[]}"#);
}

#[test]
fn roundtrip_replacing_object_with_array_allows_dash_and_leading_zero_keys() {
    assert_projection_roundtrip(r#"{"-":"dash","01":"lead","foo":"bar"}"#, r"[]");
}

#[derive(Clone)]
struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u32(&mut self) -> u32 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        (self.state >> 32) as u32
    }

    fn next_usize(&mut self, max_exclusive: usize) -> usize {
        if max_exclusive == 0 {
            return 0;
        }
        (self.next_u32() as usize) % max_exclusive
    }

    fn next_bool(&mut self) -> bool {
        (self.next_u32() & 1) == 0
    }
}

fn random_scalar(rng: &mut Lcg) -> Value {
    match rng.next_usize(5) {
        0 => Value::Null,
        1 => Value::Bool(rng.next_bool()),
        2 => Value::Number(i64::from(rng.next_u32() % 100).into()),
        3 => Value::String(format!("s{}", rng.next_u32() % 10)),
        _ => Value::String(String::new()),
    }
}

fn random_json(rng: &mut Lcg, depth: usize) -> Value {
    if depth == 0 {
        return random_scalar(rng);
    }

    match rng.next_usize(5) {
        0 => random_scalar(rng),
        1 => {
            let len = rng.next_usize(3);
            let mut values = Vec::new();
            for _ in 0..len {
                values.push(random_json(rng, depth - 1));
            }
            Value::Array(values)
        }
        _ => {
            let candidate_keys = ["", "a", "b", "x", "~", "~0", "~1", "/", "a/b"];
            let count = rng.next_usize(4);
            let mut object = serde_json::Map::new();
            for _ in 0..count {
                let key = candidate_keys[rng.next_usize(candidate_keys.len())].to_string();
                object
                    .entry(key)
                    .or_insert_with(|| random_json(rng, depth - 1));
            }
            Value::Object(object)
        }
    }
}

#[test]
fn roundtrip_randomized_transition_invariant() {
    let mut rng = Lcg::new(0x000A_11CE_5EED_u64);

    for _ in 0..300 {
        let before = random_json(&mut rng, 3);
        let after = random_json(&mut rng, 3);
        let before_json = serde_json::to_string(&before).expect("before should serialize");
        let after_json = serde_json::to_string(&after).expect("after should serialize");
        assert_projection_roundtrip(&before_json, &after_json);
    }
}

#[test]
fn roundtrip_is_invariant_to_change_order_permutations() {
    let before_json = r#"{"list":["a","b","c","d"],"flags":{"active":false},"old":"x"}"#;
    let after_json = r#"{"list":["a"],"flags":{"active":true},"team":[{"name":"Ada"}]}"#;
    let projected = projected_changes_for_transition(before_json, after_json);
    let expected: Value = serde_json::from_str(after_json).expect("expected JSON should parse");

    let mut permutations = Vec::new();
    permutations.push(projected.clone());

    let mut reversed = projected.clone();
    reversed.reverse();
    permutations.push(reversed);

    let mut rotated = projected.clone();
    if !rotated.is_empty() {
        rotated.rotate_left(1);
    }
    permutations.push(rotated);

    let mut lexicographic = projected.clone();
    lexicographic.sort_by(|a, b| a.entity_pk.cmp(&b.entity_pk));
    permutations.push(lexicographic);

    let mut reverse_lexicographic = projected;
    reverse_lexicographic.sort_by(|a, b| b.entity_pk.cmp(&a.entity_pk));
    permutations.push(reverse_lexicographic);

    for changes in permutations {
        let reconstructed = render_projection(changes);
        assert_eq!(reconstructed, expected);
    }
}

#[test]
fn roundtrip_reconstructs_with_lexicographic_entity_pk_order() {
    let before_json = r#"{"list":["a","b","c","d"]}"#;
    let after_json = r#"{"list":["a"]}"#;
    let mut projected = projected_changes_for_transition(before_json, after_json);
    projected.sort_by(|a, b| a.entity_pk.cmp(&b.entity_pk));

    let reconstructed = render_projection(projected);
    let expected: Value = serde_json::from_str(after_json).expect("expected JSON should parse");
    assert_eq!(reconstructed, expected);
}

#[test]
fn guest_interface_applies_delta_to_active_state_and_renders_json() {
    let before_json = r#"{"keep":1,"drop":{"nested":true},"list":["a","b"]}"#;
    let after_json = r#"{"keep":2,"list":["a"],"add":{"x":1}}"#;

    let before_state = active_state_from_changes(
        JsonPlugin::detect_changes(Vec::new(), file_from_json(before_json))
            .expect("initial detect_changes should succeed"),
    );

    let delta = JsonPlugin::detect_changes(before_state.clone(), file_from_json(after_json))
        .expect("delta detect_changes should succeed");
    assert!(
        delta.iter().any(|change| change.snapshot_content.is_none()),
        "delta should include tombstones for removed JSON projection rows"
    );

    let after_state = apply_changes_to_active_state(before_state, delta);
    let output = JsonPlugin::render(after_state).expect("render should succeed");

    let actual: Value = serde_json::from_slice(&output).expect("rendered JSON should parse");
    let expected: Value = serde_json::from_str(after_json).expect("expected JSON should parse");
    assert_eq!(actual, expected);
}
