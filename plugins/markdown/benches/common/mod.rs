use std::fmt::Write as _;

use plugin_md_v2::PluginFile;

pub fn file_from_markdown(id: &str, path: &str, markdown: &str) -> PluginFile {
    PluginFile {
        id: id.to_string(),
        path: path.to_string(),
        data: markdown.as_bytes().to_vec(),
    }
}

pub fn dataset_small() -> (String, String) {
    let before = "# Title\n\nA short paragraph.\n".to_string();
    let after = "# Title\n\nA short paragraph with update.\n".to_string();
    (before, after)
}

pub fn dataset_medium() -> (String, String) {
    let mut before = String::new();
    let mut after = String::new();
    before.push_str("---\ntitle: Medium\n---\n\n");
    after.push_str("---\ntitle: Medium\n---\n\n");

    for idx in 0..120 {
        let _ = write!(before, "## Section {idx}\n\nParagraph {idx}.\n\n");
        let _ = write!(
            after,
            "## Section {idx}\n\nParagraph {idx} changed with value {}.\n\n",
            idx * 3
        );
    }

    (before, after)
}

pub fn dataset_large() -> (String, String) {
    let mut before = String::new();
    let mut after = String::new();
    before.push_str("---\ntitle: Large\n---\n\n");
    after.push_str("---\ntitle: Large\n---\n\n");

    for idx in 0..450 {
        let _ = write!(
            before,
            "### Item {idx}\n\n- [x] done\n- [ ] pending\n\nInline math $a_{idx} + b_{idx}$\n\n<Component value={{ {idx} }} />\n\n"
        );
        let _ = write!(
            after,
            "### Item {idx}\n\n- [x] done\n- [x] pending\n\nInline math $a_{idx} + b_{idx} + c_{idx}$\n\n<Component value={{ {idx} }} flag />\n\n"
        );
    }

    (before, after)
}
