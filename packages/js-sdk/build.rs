fn main() {
    println!("cargo:rustc-check-cfg=cfg(lix_filesystem_native)");
}
