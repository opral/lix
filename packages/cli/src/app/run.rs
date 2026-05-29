use super::context::AppContext;
use super::welcome;
use crate::cli::root::{Cli, Command};
use crate::commands;
use crate::error::CliError;
use crate::hints;
use clap::{CommandFactory, Parser};
use std::io::Write;

pub fn run() -> Result<(), CliError> {
    let cli = Cli::parse();
    let no_hints = cli.no_hints;
    let lix_path = cli.path;

    let command = match cli.command {
        Some(command) => command,
        None => {
            welcome::print_banner(lix_path.as_deref());
            Cli::command().print_help().ok();
            println!();
            return Ok(());
        }
    };

    let context = AppContext { lix_path, no_hints };

    let result = match command {
        Command::Exp(exp_command) => commands::exp::run(&context, exp_command),
        Command::Init(init_command) => commands::init::run(init_command),
        Command::Redo(redo_command) => commands::redo::run(&context, redo_command),
        Command::Sql(sql_command) => commands::sql::run(&context, sql_command),
        Command::Undo(undo_command) => commands::undo::run(&context, undo_command),
        Command::Version(version_command) => commands::version::run(&context, version_command),
    };

    match result {
        Ok(output) => {
            if !no_hints {
                hints::render_hints(&output.hints);
            }
            Ok(())
        }
        Err(err) => {
            let mut stderr = std::io::stderr().lock();
            render_error_output(&err, no_hints, &mut stderr);
            Err(err)
        }
    }
}

/// Render a `CliError` to the given writer: the error message on one line,
/// followed by a `hint:` line when hints are enabled and a hint is attached.
/// Factored out of [`run`] so the rendering path is unit-testable.
pub(crate) fn render_error_output<W: Write>(err: &CliError, no_hints: bool, out: &mut W) {
    writeln!(out, "{err}").ok();
    if !no_hints {
        for hint in hints::hint_from_error(err) {
            writeln!(out, "hint: {hint}").ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lix_sdk::LixError;

    fn rendered(err: &CliError, no_hints: bool) -> String {
        let mut buf: Vec<u8> = Vec::new();
        render_error_output(err, no_hints, &mut buf);
        String::from_utf8(buf).expect("render output is valid utf-8")
    }

    #[test]
    fn renders_hint_line_when_error_carries_hint() {
        let err = CliError::from_lix(
            "sql execution failed",
            LixError::new(
                "LIX_ERROR_UNSUPPORTED_WRITE_EXPRESSION",
                "json(...) is not supported",
            )
            .with_hint("use lix_json('...') instead"),
        );
        let out = rendered(&err, false);
        assert_eq!(
            out,
            "sql execution failed: json(...) is not supported\n\
             hint: use lix_json('...') instead\n"
        );
    }

    #[test]
    fn suppresses_hint_when_no_hints_is_set() {
        let err = CliError::from_lix(
            "sql execution failed",
            LixError::new("CODE", "boom").with_hint("try the fix"),
        );
        let out = rendered(&err, true);
        assert_eq!(out, "sql execution failed: boom\n");
    }

    #[test]
    fn omits_hint_line_when_error_has_no_hint() {
        let err = CliError::from_lix("ctx", LixError::new("CODE", "boom"));
        let out = rendered(&err, false);
        assert_eq!(out, "ctx: boom\n");
    }

    #[test]
    fn omits_hint_line_for_non_lix_error_variants() {
        let err = CliError::msg("plain message");
        let out = rendered(&err, false);
        assert_eq!(out, "plain message\n");
    }
}
