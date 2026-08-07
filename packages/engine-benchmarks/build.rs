use std::path::PathBuf;

fn main() {
    const ORACLE: &str = "benches/forktree_stage2_recovery_no_lease.rs";
    println!("cargo:rerun-if-changed={ORACLE}");
    let source = std::fs::read_to_string(ORACLE).expect("read frozen no-lease oracle");
    let includable = source.replacen("//!", "//", 3);
    let output = PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR"))
        .join("forktree_stage2_recovery_no_lease_includable.rs");
    std::fs::write(output, includable).expect("write includable frozen no-lease oracle");
}
