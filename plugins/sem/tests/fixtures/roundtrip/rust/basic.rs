use std::fmt;

// Keep this comment outside semantic entities.
const DEFAULT_NAME: &str = "Ada";

pub fn greeting(name: &str) -> String {
    format!("Hello, {name}!")
}

pub struct Greeter {
    name: String,
}

impl fmt::Display for Greeter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&greeting(&self.name))
    }
}

// Tail gap should survive rendering.
