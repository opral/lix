fn main() {
    println!("cargo:rustc-check-cfg=cfg(lix_filesystem_native)");
    println!("cargo:rustc-cfg=lix_filesystem_native");
    if std::env::var("CARGO_CFG_TARGET_FAMILY").as_deref() == Ok("wasm") {
        return;
    }
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        println!("cargo:rustc-cdylib-link-arg=-undefined");
        println!("cargo:rustc-cdylib-link-arg=dynamic_lookup");
    }
}
