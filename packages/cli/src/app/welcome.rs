use std::io::IsTerminal;
use std::path::{Path, PathBuf};

const CYAN: &str = "\x1b[38;2;8;181;214m";
const RESET: &str = "\x1b[0m";

const LOGO: [&str; 6] = [
    "██╗     ██╗██╗  ██╗",
    "██║     ██║╚██╗██╔╝",
    "██║     ██║ ╚███╔╝ ",
    "██║     ██║ ██╔██╗ ",
    "███████╗██║██╔╝ ██╗",
    "╚══════╝╚═╝╚═╝  ╚═╝",
];

const TAGLINE: &str = "change control system for everything";

pub fn print_banner(explicit_lix_path: Option<&Path>) {
    let color = use_color();
    let (cyan, reset) = if color { (CYAN, RESET) } else { ("", "") };

    let version = env!("CARGO_PKG_VERSION");
    let info = [
        String::new(),
        format!("lix v{version}"),
        TAGLINE.to_string(),
        current_dir_display(),
        describe_lix_state(explicit_lix_path),
        String::new(),
    ];

    println!();
    for (logo_line, text) in LOGO.iter().zip(info.iter()) {
        if text.is_empty() {
            println!(" {cyan}{logo_line}{reset}");
        } else {
            println!(" {cyan}{logo_line}{reset}       {text}");
        }
    }
    println!();
}

fn use_color() -> bool {
    std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none()
}

fn current_dir_display() -> String {
    let Ok(cwd) = std::env::current_dir() else {
        return String::new();
    };
    if let Some(home) = std::env::var_os("HOME") {
        let home = PathBuf::from(home);
        if let Ok(relative) = cwd.strip_prefix(&home) {
            let rel = relative.display().to_string();
            return if rel.is_empty() {
                "~".to_string()
            } else {
                format!("~/{rel}")
            };
        }
    }
    cwd.display().to_string()
}

fn describe_lix_state(explicit: Option<&Path>) -> String {
    if let Some(path) = explicit {
        return format!("using {}", path.display());
    }
    let Ok(cwd) = std::env::current_dir() else {
        return String::new();
    };
    let mut lix_files: Vec<PathBuf> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&cwd) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("lix") {
                lix_files.push(path);
            }
        }
    }
    match lix_files.len() {
        0 => "no .lix file detected · run `lix init <path>`".to_string(),
        1 => {
            let name = lix_files[0]
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            format!("detected {name}")
        }
        n => format!("{n} .lix files · pass --path <path>"),
    }
}
