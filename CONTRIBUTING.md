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

1. Run the package-specific tests and checks for the area you changed
2. Include release notes in the package repository if the change affects a published package
