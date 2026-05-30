use plugin_csv_v2::{PluginApiError, PluginFile, apply_changes, detect_changes};

fn file_from_bytes(id: &str, path: &str, data: &[u8]) -> PluginFile {
    PluginFile {
        id: id.to_string(),
        path: path.to_string(),
        data: data.to_vec(),
    }
}

#[test]
fn detect_changes_reports_stubbed_implementation() {
    let file = file_from_bytes("f1", "/table.csv", b"a,b\n1,2\n");

    let error = detect_changes(None, file).expect_err("detect_changes should be stubbed");

    assert_not_implemented(error, "detect_changes");
}

#[test]
fn apply_changes_reports_stubbed_implementation() {
    let file = file_from_bytes("f1", "/table.csv", b"a,b\n1,2\n");

    let error = apply_changes(file, Vec::new()).expect_err("apply_changes should be stubbed");

    assert_not_implemented(error, "apply_changes");
}

fn assert_not_implemented(error: PluginApiError, operation: &str) {
    match error {
        PluginApiError::Internal(message) => {
            assert!(message.contains(operation));
            assert!(message.contains("not implemented yet"));
        }
        PluginApiError::InvalidInput(message) => {
            panic!("expected Internal, got InvalidInput({message})");
        }
    }
}
