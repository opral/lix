fn main() {
    if lix_cli::run().is_err() {
        std::process::exit(1);
    }
}
