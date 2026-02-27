pub mod app;
pub mod cli;
pub mod commands;
pub mod db;
pub mod error;
pub mod output;

pub fn run() -> Result<(), error::CliError> {
    app::run()
}
