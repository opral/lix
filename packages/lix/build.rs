use std::env;
use std::fs;
use std::path::PathBuf;
use wit_bindgen_rust::{Opts, WithOption};

fn main() {
    // The unoptimized engine test executable can exceed 4 GiB. Mold 2.30
    // places unwind tables before other read-only data, which can overflow
    // their signed 32-bit panic-personality references across the large text
    // section. Rust's bundled LLD keeps those references within range and
    // diagnoses overflow instead of producing a broken unwind table.
    // Apply this to Lix-owned linked targets; the tests-only Cargo directive
    // does not cover library unit tests. Dependency executables keep their
    // own linker, and other targets (including Wasm) retain their settings.
    if env::var("TARGET").expect("Cargo sets TARGET") == "x86_64-unknown-linux-gnu" {
        println!("cargo:rustc-link-arg=-fuse-ld=lld");
    }

    generate(
        "plugin",
        "combined_bindings.rs",
        "export_combined_component",
        None,
        Vec::new(),
    );
    generate(
        "lix:plugin-column-merger-world/column-merger-plugin",
        "column_merger_bindings.rs",
        "export_column_merger_component",
        Some("column_merger"),
        vec![
            remap(
                "lix:plugin-v2/host",
                "crate::plugin::api::combined_bindings::lix::plugin_v2::host",
            ),
            remap(
                "lix:plugin-v2/types",
                "crate::plugin::api::combined_bindings::lix::plugin_v2::types",
            ),
            remap(
                "lix:plugin-v2/column-merger",
                "crate::plugin::api::combined_bindings::exports::lix::plugin_v2::column_merger",
            ),
        ],
    );
    generate(
        "lix:plugin-file-projection-world/file-projection-plugin",
        "file_projection_bindings.rs",
        "export_file_projection_component",
        Some("file_projection"),
        vec![
            remap(
                "lix:plugin-v2/host",
                "crate::plugin::api::combined_bindings::lix::plugin_v2::host",
            ),
            remap(
                "lix:plugin-v2/types",
                "crate::plugin::api::combined_bindings::lix::plugin_v2::types",
            ),
            remap(
                "lix:plugin-v2/file-projection",
                "crate::plugin::api::combined_bindings::exports::lix::plugin_v2::file_projection",
            ),
        ],
    );
}

fn remap(name: &str, path: &str) -> (String, WithOption) {
    (name.to_owned(), WithOption::Path(path.to_owned()))
}

fn generate(
    world: &str,
    output_name: &str,
    export_macro_name: &str,
    macro_prefix: Option<&str>,
    with: Vec<(String, WithOption)>,
) {
    let generated = Opts {
        export_macro_name: Some(export_macro_name.to_owned()),
        pub_export_macro: true,
        with,
        ..Opts::default()
    }
    .build()
    .generate_to_out_dir(Some(world))
    .unwrap_or_else(|error| panic!("failed to generate {world} bindings: {error:#}"));

    let mut source = fs::read_to_string(generated)
        .unwrap_or_else(|error| panic!("failed to read generated {world} bindings: {error}"));
    if let Some(prefix) = macro_prefix {
        source = source.replace("__export_", &format!("__export_{prefix}_"));
    }

    let output =
        PathBuf::from(env::var_os("OUT_DIR").expect("Cargo sets OUT_DIR")).join(output_name);
    fs::write(&output, source)
        .unwrap_or_else(|error| panic!("failed to write {}: {error}", output.display()));
}
