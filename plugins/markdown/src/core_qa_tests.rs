use super::*;

#[test]
fn qa_nested_subtree_signatures_use_linear_space() {
    let (document, _) = Document::open_file(
        b"leaf\n".to_vec(),
        Some("nested.md"),
        IdNamespace::from_halves(30, 1),
    )
    .unwrap();
    let mut tree = document.tree.materialize().children.remove(0);
    let node = tree.node.clone();
    for _ in 0..32 {
        tree = NodeTree {
            node: node.clone(),
            children: vec![tree],
        };
    }
    let signature = tree.subtree_signature();
    assert!(
        signature.len() < 32_000,
        "signature grew to {} bytes",
        signature.len()
    );
    let mut different = tree.clone();
    different.children.push(NodeTree {
        node,
        children: vec![],
    });
    assert_ne!(signature, different.subtree_signature());
}

fn edit_target(document: &Document) -> Document {
    let tree = document.tree.materialize();
    let mut node = tree
        .children
        .iter()
        .find(|child| {
            serde_json::to_string(&child.node.payload)
                .unwrap()
                .contains("QA_TARGET")
        })
        .expect("target paragraph")
        .node
        .clone();
    node.payload = serde_json::from_str(
        &serde_json::to_string(&node.payload)
            .unwrap()
            .replace("QA_TARGET", "QA_EDITED"),
    )
    .unwrap();
    document
        .rows_changed(vec![RowChange {
            schema_key: NODE_SCHEMA_KEY.into(),
            row_pk: vec![node.id],
            row: Some(node_to_typed_row(&node).unwrap()),
            effect: ChangeEffect::Content,
        }])
        .expect("edit target")
        .0
}

#[test]
fn qa_semantic_edit_preserves_unrelated_source_format() {
    let cases = [
        "#   Unusual heading  ###\n\nQA_TARGET\n",
        "*Counter:\n\nQA_TARGET\n",
        "Title\n=====\n\nQA_TARGET\n",
        "first  \nsecond\n\nQA_TARGET\n",
        "\n\n# Heading\n\n\nQA_TARGET\n\n\n",
        "> a\n>\n> b\n\nQA_TARGET\n",
        "#   Header ###\n\n[x]: /url\n\nUse [x].\n\nQA_TARGET\n",
        "---\nkey: value\n---\n\n#   Header ###\n\nQA_TARGET\n",
        "| a | b |\n|---|---|\n| c | d |\n\n#   Header ###\n\nQA_TARGET\n",
    ];
    let mut failures = Vec::new();
    for source in cases {
        let (document, _) = Document::open_file(
            source.as_bytes().to_vec(),
            Some("qa.md"),
            IdNamespace::from_halves(21, 1),
        )
        .unwrap();
        let actual = edit_target(&document).bytes();
        let expected = source.replace("QA_TARGET", "QA_EDITED").into_bytes();
        if actual != expected {
            failures.push(format!(
                "source={source:?} actual={:?}",
                String::from_utf8_lossy(&actual)
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn qa_replayed_unchanged_row_preserves_source_bytes() {
    let source = b"#   Unusual heading  ###\n\n*Counter:\n\nQA_TARGET\n".to_vec();
    let (document, _) = Document::open_file(
        source.clone(),
        Some("qa.md"),
        IdNamespace::from_halves(21, 2),
    )
    .unwrap();
    let node = document
        .tree
        .materialize()
        .children
        .last()
        .unwrap()
        .node
        .clone();
    let (successor, edits) = document
        .rows_changed(vec![RowChange {
            schema_key: NODE_SCHEMA_KEY.into(),
            row_pk: vec![node.id],
            row: Some(node_to_typed_row(&node).unwrap()),
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    assert_eq!(successor.bytes(), source);
    assert!(edits.is_empty());
}

#[test]
fn qa_file_edits_and_reopen_preserve_bytes_deterministically() {
    let mut source =
        b"# Header\n\nSame paragraph\n\nSame paragraph\n\n- [ ] task\n\n```rs\nfn main() {}\n```\n"
            .to_vec();
    let (mut document, _) = Document::open_file(
        source.clone(),
        Some("qa.md"),
        IdNamespace::from_halves(21, 3),
    )
    .unwrap();
    for index in 0..80 {
        let offset = (index * 17) % (source.len() + 1);
        let insert = [b'x', b'\n', b'*', b' ', b'`', b'\r', b'|'][index % 7];
        let edit = FileEdit {
            offset: offset as u64,
            delete_len: 0,
            insert: &[insert],
        };
        let namespace = IdNamespace::from_halves(22, index as u32);
        let (next, changes) = document.file_changed(&[edit], namespace).unwrap();
        let (repeated, repeated_changes) = document.file_changed(&[edit], namespace).unwrap();
        assert_eq!(
            changes, repeated_changes,
            "nondeterministic changes at {index}"
        );
        assert_eq!(next.bytes(), repeated.bytes());
        source.insert(offset, insert);
        assert_eq!(next.bytes(), source, "edit {index}");
        let (cold, _) = Document::open_file(source.clone(), Some("qa.md"), namespace).unwrap();
        assert_eq!(
            render_tree(&next.tree.materialize()).unwrap(),
            render_tree(&cold.tree.materialize()).unwrap(),
            "incremental semantic divergence at edit {index}"
        );
        let (root, blocks) = next.arena_state().unwrap();
        document = Document::open_arena(source.clone(), &root, blocks).unwrap();
        assert_eq!(document.bytes(), source);
    }
}

#[test]
fn qa_incremental_context_edits_match_full_parse() {
    let cases = [
        ("[name]: /old\n\nUse [name].\n", "/old", "/new"),
        ("[name]: /url\n\nUse [name].\n", "name", "other"),
        (
            "```\ninside\n```\n\noutside\n",
            "```\n\noutside",
            "``\n\noutside",
        ),
        ("#   Header  ###\n\nfirst\n\nsecond\n", "second", "changed"),
    ];
    let mut failures = Vec::new();
    for (source, before, after) in cases {
        let (document, _) = Document::open_file(
            source.as_bytes().to_vec(),
            Some("qa.md"),
            IdNamespace::from_halves(23, 1),
        )
        .unwrap();
        let edit = FileEdit {
            offset: source.find(before).unwrap() as u64,
            delete_len: before.len() as u64,
            insert: after.as_bytes(),
        };
        let expected = source.replacen(before, after, 1);
        let (incremental, _) = document
            .file_changed(&[edit], IdNamespace::from_halves(23, 2))
            .unwrap();
        let (cold, _) = Document::open_file(
            expected.as_bytes().to_vec(),
            Some("qa.md"),
            IdNamespace::from_halves(23, 2),
        )
        .unwrap();
        let actual = render_tree(&incremental.tree.materialize()).unwrap();
        let expected_render = render_tree(&cold.tree.materialize()).unwrap();
        if actual != expected_render {
            failures.push(format!(
                "source={source:?} incremental={:?} cold={:?}",
                String::from_utf8_lossy(&actual),
                String::from_utf8_lossy(&expected_render)
            ));
        }
        assert_eq!(incremental.bytes(), expected.as_bytes());
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn qa_semantic_edits_preserve_bom_crlf_and_legacy_encoding() {
    let utf8 = "\u{feff}#   Header ###\r\n\r\nQA_TARGET\r\n";
    let mut utf16 = vec![0xff, 0xfe];
    for unit in "#   Header ###\r\n\r\nQA_TARGET\r\n".encode_utf16() {
        utf16.extend(unit.to_le_bytes());
    }
    let cases = [
        utf8.as_bytes().to_vec(),
        utf16,
        b"#   Caf\xe9 ###\r\n\r\nQA_TARGET\r\n".to_vec(),
    ];
    let mut failures = Vec::new();
    for source in cases {
        let (document, _) = Document::open_file(
            source.clone(),
            Some("qa.md"),
            IdNamespace::from_halves(25, 1),
        )
        .unwrap();
        let mut expected = source.clone();
        let (before, after) = if source.starts_with(&[0xff, 0xfe]) {
            (
                "QA_TARGET"
                    .encode_utf16()
                    .flat_map(u16::to_le_bytes)
                    .collect::<Vec<_>>(),
                "QA_EDITED"
                    .encode_utf16()
                    .flat_map(u16::to_le_bytes)
                    .collect::<Vec<_>>(),
            )
        } else {
            (b"QA_TARGET".to_vec(), b"QA_EDITED".to_vec())
        };
        let offset = source
            .windows(before.len())
            .position(|window| window == before)
            .unwrap();
        expected.splice(offset..offset + before.len(), after);
        let actual = edit_target(&document).bytes();
        if actual != expected {
            failures.push(format!("source={source:?} actual={actual:?}"));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn qa_semantic_edit_then_reopen_remains_source_preserving() {
    let mut source = "\n#   Header ###\n\nQA_TARGET\n\n*untouched:\n\n".to_owned();
    let (mut document, _) = Document::open_file(
        source.as_bytes().to_vec(),
        Some("qa.md"),
        IdNamespace::from_halves(25, 2),
    )
    .unwrap();
    for _ in 0..12 {
        document = edit_target(&document);
        source = source.replace("QA_TARGET", "QA_EDITED");
        assert_eq!(document.bytes(), source.as_bytes());
        let (root, blocks) = document.arena_state().unwrap();
        document = Document::open_arena(document.bytes(), &root, blocks).unwrap();
        let offset = source.find("QA_EDITED").unwrap();
        let edit = FileEdit {
            offset: offset as u64,
            delete_len: 9,
            insert: b"QA_TARGET",
        };
        document = document
            .file_changed(&[edit], IdNamespace::from_halves(25, 3))
            .unwrap()
            .0;
        source = source.replace("QA_EDITED", "QA_TARGET");
        assert_eq!(document.bytes(), source.as_bytes());
    }
}

#[test]
fn qa_deleting_paragraph_preserves_unrelated_format() {
    let source = "#   Header ###\n\nQA_TARGET\n\n*untouched:\n";
    let (document, _) = Document::open_file(
        source.as_bytes().to_vec(),
        Some("qa.md"),
        IdNamespace::from_halves(25, 4),
    )
    .unwrap();
    let node = document.tree.materialize().children[1].node.clone();
    let (result, _) = document
        .rows_changed(vec![RowChange {
            schema_key: NODE_SCHEMA_KEY.into(),
            row_pk: vec![node.id],
            row: None,
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    let actual = String::from_utf8(result.bytes()).unwrap();
    assert!(!actual.contains("QA_TARGET"));
    assert!(actual.contains("#   Header ###"), "{actual:?}");
    assert!(actual.contains("\n*untouched:"), "{actual:?}");
}

#[test]
fn qa_legacy_encoded_file_edit_matches_cold_parse() {
    let source = b"Caf\xe9 plain text\n\nSecond paragraph\n";
    let (document, _) = Document::open_file(
        source.to_vec(),
        Some("qa.md"),
        IdNamespace::from_halves(25, 5),
    )
    .unwrap();
    let offset = source
        .windows(5)
        .position(|window| window == b"plain")
        .unwrap();
    let (next, _) = document
        .file_changed(
            &[FileEdit {
                offset: offset as u64,
                delete_len: 5,
                insert: b"edited",
            }],
            IdNamespace::from_halves(25, 6),
        )
        .unwrap();
    let mut expected = source.to_vec();
    expected.splice(offset..offset + 5, b"edited".iter().copied());
    assert_eq!(next.bytes(), expected);
    let (cold, _) =
        Document::open_file(expected, Some("qa.md"), IdNamespace::from_halves(25, 6)).unwrap();
    assert_eq!(
        render_tree(&next.tree.materialize()).unwrap(),
        render_tree(&cold.tree.materialize()).unwrap()
    );
}

#[test]
fn qa_bulk_incompatible_siblings_preserve_existing_identities() {
    let source = (0..1_000)
        .map(|i| format!("paragraph {i}\n\n"))
        .collect::<String>();
    let (document, _) = Document::open_file(
        source.as_bytes().to_vec(),
        Some("bulk.md"),
        IdNamespace::from_halves(30, 2),
    )
    .unwrap();
    let before = document.tree.materialize();
    let append = (0..1_000)
        .map(|i| format!("```\ncode {i}\n```\n\n"))
        .collect::<String>();
    let (updated, _) = document
        .file_changed(
            &[FileEdit {
                offset: source.len() as u64,
                delete_len: 0,
                insert: append.as_bytes(),
            }],
            IdNamespace::from_halves(30, 3),
        )
        .unwrap();
    let after = updated.tree.materialize();
    assert_eq!(after.children.len(), 2_000);
    for (old, new) in before.children.iter().zip(&after.children) {
        assert_eq!(old.node.id, new.node.id);
    }
}

#[test]
fn qa_semantic_inline_edits_override_stale_spelling() {
    for (source, field, value, expected) in [
        ("&amp;\n", "value", "<", "&#x3C;\n"),
        ("`old`\n", "value", "new", "`new`\n"),
        (
            "https://old.example\n",
            "destination",
            "https://new.example",
            "https://new.example\n",
        ),
    ] {
        let (document, _) = Document::open_file(
            source.as_bytes().to_vec(),
            Some("edit.md"),
            IdNamespace::from_halves(31, 1),
        )
        .unwrap();
        let mut node = document.tree.materialize().children.remove(0).node;
        node.payload["inline"][0][field] = serde_json::json!(value);
        let (updated, _) = document
            .rows_changed(vec![RowChange {
                schema_key: NODE_SCHEMA_KEY.into(),
                row_pk: vec![node.id],
                row: Some(node_to_typed_row(&node).unwrap()),
                effect: ChangeEffect::Content,
            }])
            .unwrap();
        assert_eq!(String::from_utf8(updated.bytes()).unwrap(), expected);
        let (reopened, _) = Document::open_file(
            updated.bytes(),
            Some("edit.md"),
            IdNamespace::from_halves(31, 2),
        )
        .unwrap();
        assert_eq!(
            reopened.tree.materialize().children[0].node.payload["inline"][0][field],
            value
        );
    }
}

#[test]
fn qa_reference_identifier_edits_override_stale_labels() {
    for source in ["[old]: /url\n\n[old]\n", "[^old]: note\n\n[^old]\n"] {
        let (document, _) = Document::open_file(
            source.as_bytes().to_vec(),
            Some("edit.md"),
            IdNamespace::from_halves(31, 3),
        )
        .unwrap();
        let mut node = document
            .tree
            .materialize()
            .children
            .into_iter()
            .find(|node| node.node.payload.get("identifier").is_some())
            .unwrap()
            .node;
        node.payload["identifier"] = serde_json::json!("new");
        let (updated, _) = document
            .rows_changed(vec![RowChange {
                schema_key: NODE_SCHEMA_KEY.into(),
                row_pk: vec![node.id],
                row: Some(node_to_typed_row(&node).unwrap()),
                effect: ChangeEffect::Content,
            }])
            .unwrap();
        let (reopened, _) = Document::open_file(
            updated.bytes(),
            Some("edit.md"),
            IdNamespace::from_halves(31, 4),
        )
        .unwrap();
        assert!(
            reopened.tree.materialize().children.iter().any(|node| node
                .node
                .payload
                .get("identifier")
                == Some(&serde_json::json!("new"))),
            "{}",
            String::from_utf8_lossy(&updated.bytes())
        );
    }
}

#[test]
fn qa_literal_punctuation_runs_import_losslessly() {
    for source in [
        "||||||||".to_owned(),
        "]>||~`>1.".to_owned(),
        "|".repeat(10_000),
    ] {
        let (document, _) = Document::open_file(
            source.as_bytes().to_vec(),
            Some("punctuation.md"),
            IdNamespace::from_halves(32, 1),
        )
        .unwrap();
        assert_eq!(document.bytes(), source.as_bytes());
        let (root, blocks) = document.arena_state().unwrap();
        let reopened = Document::open_arena(document.bytes(), &root, blocks).unwrap();
        assert_eq!(reopened.bytes(), source.as_bytes());
    }
}

#[test]
fn qa_invalid_parent_changes_are_rejected_without_losing_rows() {
    for source in ["first\n\nsecond\n", "| a |\n| - |\n\nsecond\n"] {
        let (document, _) = Document::open_file(
            source.as_bytes().to_vec(),
            Some("parent.md"),
            IdNamespace::from_halves(32, 2),
        )
        .unwrap();
        let tree = document.tree.materialize();
        let mut node = tree.children.last().unwrap().node.clone();
        node.parent_id = Some(tree.children[0].node.id);
        let result = document.rows_changed(vec![RowChange {
            schema_key: NODE_SCHEMA_KEY.into(),
            row_pk: vec![node.id],
            row: Some(node_to_typed_row(&node).unwrap()),
            effect: ChangeEffect::Content,
        }]);
        assert!(matches!(result, Err(PluginError::InvalidInput(_))));
        assert_eq!(document.bytes(), source.as_bytes());
    }
}

#[test]
fn qa_text_edits_after_autolinks_remain_literal() {
    let (document, _) = Document::open_file(
        b"https://example.com tail\n".to_vec(),
        Some("text.md"),
        IdNamespace::from_halves(33, 1),
    )
    .unwrap();
    let mut node = document.tree.materialize().children.remove(0).node;
    node.payload["inline"][1]["value"] = serde_json::json!(" *oops*");
    let (updated, _) = document
        .rows_changed(vec![RowChange {
            schema_key: NODE_SCHEMA_KEY.into(),
            row_pk: vec![node.id],
            row: Some(node_to_typed_row(&node).unwrap()),
            effect: ChangeEffect::Content,
        }])
        .unwrap();
    assert_eq!(updated.bytes(), b"https://example.com \\*oops\\*\n");
    let (reopened, _) = Document::open_file(
        updated.bytes(),
        Some("text.md"),
        IdNamespace::from_halves(33, 2),
    )
    .unwrap();
    assert!(
        !serde_json::to_string(&reopened.tree.materialize().children[0].node.payload)
            .unwrap()
            .contains("emphasis")
    );
}

#[test]
fn qa_table_inline_markers_preserve_cell_source_positions() {
    let source = "| _a_ | __b__ |\n| - | - |\n| café \\| _c_ | __d__ |\n";
    let (document, _) = Document::open_file(
        source.as_bytes().to_vec(),
        Some("table.md"),
        IdNamespace::from_halves(34, 1),
    )
    .unwrap();
    let tree = document.tree.materialize();
    let table = &tree.children[0];
    let mut markers = Vec::new();
    for row in table
        .children
        .iter()
        .filter(|child| child.node.kind == NodeKind::TableRow)
    {
        for cell in &row.children {
            for inline in cell.node.payload["inline"].as_array().unwrap() {
                if let Some(marker) = inline.get("format").and_then(|format| format.get("marker")) {
                    markers.push(marker.as_str().unwrap().to_owned());
                }
            }
        }
    }
    assert_eq!(markers, ["_", "__", "_", "__"]);
    assert_eq!(document.bytes(), source.as_bytes());
}

#[test]
fn qa_relaxed_autolink_spelling_survives_import_and_edit() {
    for source in ["://b\n", "custom://host\n", "http://\n"] {
        let (document, _) = Document::open_file(
            source.as_bytes().to_vec(),
            Some("urls.md"),
            IdNamespace::from_halves(35, 1),
        )
        .unwrap();
        assert_eq!(document.bytes(), source.as_bytes());
    }
}

#[test]
fn qa_adding_titles_chooses_default_quote_style() {
    for source in [
        "[label](/url)\n",
        "![alt](/url)\n",
        "[label]: /url\n\n[label]\n",
    ] {
        let (document, _) = Document::open_file(
            source.as_bytes().to_vec(),
            Some("title.md"),
            IdNamespace::from_halves(35, 2),
        )
        .unwrap();
        let mut node = document.tree.materialize().children.remove(0).node;
        if node.kind == NodeKind::Definition {
            node.payload["title"] = serde_json::json!("new title");
        } else {
            node.payload["inline"][0]["title"] = serde_json::json!("new title");
        }
        let (updated, _) = document
            .rows_changed(vec![RowChange {
                schema_key: NODE_SCHEMA_KEY.into(),
                row_pk: vec![node.id],
                row: Some(node_to_typed_row(&node).unwrap()),
                effect: ChangeEffect::Content,
            }])
            .unwrap();
        let (reopened, _) = Document::open_file(
            updated.bytes(),
            Some("title.md"),
            IdNamespace::from_halves(35, 3),
        )
        .unwrap();
        assert!(
            serde_json::to_string(&reopened.tree.materialize().children[0].node.payload)
                .unwrap()
                .contains("new title")
        );
    }
}

#[test]
fn qa_indented_code_edits_preserve_boundary_blank_lines() {
    for value in ["\nnew\n\n", "\n", ""] {
        let (document, _) = Document::open_file(
            b"    old\n".to_vec(),
            Some("code.md"),
            IdNamespace::from_halves(36, 1),
        )
        .unwrap();
        let mut node = document.tree.materialize().children.remove(0).node;
        node.payload["value"] = serde_json::json!(value);
        let (updated, _) = document
            .rows_changed(vec![RowChange {
                schema_key: NODE_SCHEMA_KEY.into(),
                row_pk: vec![node.id],
                row: Some(node_to_typed_row(&node).unwrap()),
                effect: ChangeEffect::Content,
            }])
            .unwrap();
        let (reopened, _) = Document::open_file(
            updated.bytes(),
            Some("code.md"),
            IdNamespace::from_halves(36, 2),
        )
        .unwrap();
        assert_eq!(
            reopened.tree.materialize().children[0].node.payload["value"],
            value
        );
    }
}

#[test]
fn qa_unrepresentable_inline_code_edits_are_rejected() {
    for value in ["", "a\nb", "a\rb"] {
        let (document, _) = Document::open_file(
            b"`old`\n".to_vec(),
            Some("code.md"),
            IdNamespace::from_halves(36, 3),
        )
        .unwrap();
        let mut node = document.tree.materialize().children.remove(0).node;
        node.payload["inline"][0]["value"] = serde_json::json!(value);
        assert!(matches!(
            document.rows_changed(vec![RowChange {
                schema_key: NODE_SCHEMA_KEY.into(),
                row_pk: vec![node.id],
                row: Some(node_to_typed_row(&node).unwrap()),
                effect: ChangeEffect::Content
            }]),
            Err(PluginError::InvalidInput(_))
        ));
    }
}
