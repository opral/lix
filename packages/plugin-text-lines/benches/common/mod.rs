use plugin_text_lines::PluginFile;

pub struct DetectScenario {
    pub name: &'static str,
    pub before: Option<Vec<u8>>,
    pub after: Vec<u8>,
}

pub fn file_from_bytes(id: &str, path: &str, data: &[u8]) -> PluginFile {
    PluginFile {
        id: id.to_string(),
        path: path.to_string(),
        data: data.to_vec(),
    }
}

pub fn detect_scenarios() -> Vec<DetectScenario> {
    vec![
        DetectScenario {
            name: "small_single_line_edit",
            before: Some(build_small_before()),
            after: build_small_after(),
        },
        DetectScenario {
            name: "lockfile_large_create",
            before: None,
            after: build_lockfile(1200),
        },
        DetectScenario {
            name: "lockfile_large_patch",
            before: Some(build_lockfile(1800)),
            after: build_lockfile_with_patch(1800),
        },
        DetectScenario {
            name: "lockfile_large_block_move_and_patch",
            before: Some(build_lockfile(2200)),
            after: build_lockfile_with_block_move_and_patch(2200),
        },
    ]
}

fn build_small_before() -> Vec<u8> {
    b"const a = 1;\nconst b = 2;\nconst c = a + b;\n".to_vec()
}

fn build_small_after() -> Vec<u8> {
    b"const a = 1;\nconst b = 3;\nconst c = a + b;\n".to_vec()
}

fn build_lockfile(pkg_count: usize) -> Vec<u8> {
    let mut out = String::with_capacity(pkg_count * 170);
    for idx in 0..pkg_count {
        out.push_str(&package_block(idx));
    }
    out.into_bytes()
}

fn build_lockfile_with_patch(pkg_count: usize) -> Vec<u8> {
    let mut blocks = (0..pkg_count).map(package_block).collect::<Vec<_>>();

    let patch_index = pkg_count / 2;
    blocks[patch_index] = patched_package_block(patch_index);

    let insert_at = pkg_count / 3;
    let inserted = (0..120)
        .map(|offset| package_block(pkg_count + offset + 10_000))
        .collect::<Vec<_>>();
    blocks.splice(insert_at..insert_at, inserted);

    blocks.join("").into_bytes()
}

fn build_lockfile_with_block_move_and_patch(pkg_count: usize) -> Vec<u8> {
    let mut blocks = (0..pkg_count).map(package_block).collect::<Vec<_>>();

    let move_start = pkg_count / 5;
    let move_end = move_start + (pkg_count / 8);
    let moved = blocks.drain(move_start..move_end).collect::<Vec<_>>();

    let insert_at = pkg_count / 2;
    blocks.splice(insert_at..insert_at, moved);

    for idx in (pkg_count / 3)..(pkg_count / 3 + 64) {
        let clamped = idx.min(blocks.len().saturating_sub(1));
        blocks[clamped] = patched_package_block(90_000 + idx);
    }

    blocks.join("").into_bytes()
}

fn package_block(idx: usize) -> String {
    let major = (idx % 9) + 1;
    let minor = (idx * 7) % 40;
    let patch = (idx * 13) % 70;
    let integrity_a = idx.wrapping_mul(31).wrapping_add(17);
    let integrity_b = idx.wrapping_mul(53).wrapping_add(29);

    format!(
        "\"pkg-{idx}@^1.0.0\":\n  version \"{major}.{minor}.{patch}\"\n  resolved \"https://registry.yarnpkg.com/pkg-{idx}/-/pkg-{idx}-{major}.{minor}.{patch}.tgz\"\n  integrity sha512-{integrity_a:016x}{integrity_b:016x}\n\n"
    )
}

fn patched_package_block(idx: usize) -> String {
    let major = (idx % 9) + 2;
    let minor = (idx * 11) % 50;
    let patch = (idx * 17) % 80;
    let integrity_a = idx.wrapping_mul(67).wrapping_add(23);
    let integrity_b = idx.wrapping_mul(79).wrapping_add(31);

    format!(
        "\"pkg-{idx}@^1.0.0\":\n  version \"{major}.{minor}.{patch}\"\n  resolved \"https://registry.yarnpkg.com/pkg-{idx}/-/pkg-{idx}-{major}.{minor}.{patch}.tgz\"\n  integrity sha512-{integrity_a:016x}{integrity_b:016x}\n\n"
    )
}
