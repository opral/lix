fn main() {
    if let Err(error) = lix_cli::run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
