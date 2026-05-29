# Contributing

## Prerequisites

- [Node.js](https://nodejs.org/en/) (v20 or higher)
- [Cargo](https://doc.rust-lang.org/cargo/) for Rust packages

> [!INFO]  
> If you are developing on Windows, you need to use [WSL](https://en.wikipedia.org/wiki/Windows_Subsystem_for_Linux). 

## Development

1. Clone the repository
2. For Rust packages, run Cargo commands from the repository root, for example `cargo test --workspace`
3. For JavaScript packages, `cd` into the package directory and use that package's local scripts

## Opening a PR

1. Run workspace checks and tests, for example `cargo check --workspace` and `cargo test --workspace`. Also run package-specific checks for any JavaScript or CLI package you changed.
2. If the change affects a published core package, add a changenote in `.changenotes`. See `.changenotes/README.md` for the format and rules.
