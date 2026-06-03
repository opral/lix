pub enum Event {
    Created { id: String },
    Deleted { id: String },
}

impl Event {
    pub fn id(&self) -> &str {
        match self {
            Event::Created { id } => id,
            Event::Deleted { id } => id,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Event::Deleted { .. })
    }
}

fn describe(event: &Event) -> String {
    format!("event:{}", event.id())
}
