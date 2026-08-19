//! Minimal SSE parser for observe multiplex streams.

use super::http::ProtocolHttpStream;
use crate::LixError;
use super::wire::PROTOCOL_ERROR;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SseEvent {
    pub event: String,
    pub data: String,
    pub retry: Option<u64>,
}

pub struct SseReader {
    stream: Box<dyn ProtocolHttpStream>,
    buffered: String,
    event_name: String,
    retry: Option<u64>,
    data_lines: Vec<String>,
}

impl SseReader {
    pub fn new(stream: Box<dyn ProtocolHttpStream>) -> Self {
        Self {
            stream,
            buffered: String::new(),
            event_name: String::new(),
            retry: None,
            data_lines: Vec::new(),
        }
    }

    pub async fn next_event(&mut self) -> Result<Option<SseEvent>, LixError> {
        loop {
            if let Some(event) = self.drain_buffered()? {
                return Ok(Some(event));
            }
            match self.stream.next_chunk().await? {
                Some(chunk) => {
                    self.buffered.push_str(&String::from_utf8_lossy(&chunk));
                }
                None => {
                    if self.buffered.ends_with('\r') {
                        let line = std::mem::take(&mut self.buffered);
                        if let Some(event) = self.process_line(line.trim_end_matches('\r')) {
                            return Ok(Some(event));
                        }
                    } else if !self.buffered.is_empty() {
                        let line = std::mem::take(&mut self.buffered);
                        if let Some(event) = self.process_line(&line) {
                            return Ok(Some(event));
                        }
                    }
                    return Ok(self.dispatch_event());
                }
            }
        }
    }

    fn drain_buffered(&mut self) -> Result<Option<SseEvent>, LixError> {
        loop {
            let Some(newline) = self.buffered.find('\n') else {
                return Ok(None);
            };
            let mut line = self.buffered.drain(..=newline).collect::<String>();
            line.pop();
            if line.ends_with('\r') {
                line.pop();
            }
            if let Some(event) = self.process_line(&line) {
                return Ok(Some(event));
            }
        }
    }

    fn process_line(&mut self, line: &str) -> Option<SseEvent> {
        if line.is_empty() {
            return self.dispatch_event();
        }
        if line.starts_with(':') {
            return None;
        }
        let (field, value) = match line.find(':') {
            Some(index) => {
                let value = &line[index + 1..];
                let value = value.strip_prefix(' ').unwrap_or(value);
                (&line[..index], value)
            }
            None => (line, ""),
        };
        match field {
            "event" => self.event_name = value.to_owned(),
            "data" => self.data_lines.push(value.to_owned()),
            "retry" => {
                if value.bytes().all(|byte| byte.is_ascii_digit())
                    && let Ok(parsed) = value.parse::<u64>()
                {
                    self.retry = Some(parsed);
                }
            }
            _ => {}
        }
        None
    }

    fn dispatch_event(&mut self) -> Option<SseEvent> {
        let has_data = !self.data_lines.is_empty();
        let event = has_data.then(|| SseEvent {
            event: if self.event_name.is_empty() {
                "message".to_owned()
            } else {
                std::mem::take(&mut self.event_name)
            },
            data: self.data_lines.join("\n"),
            retry: self.retry,
        });
        self.event_name.clear();
        self.retry = None;
        self.data_lines.clear();
        event
    }
}

pub(crate) fn sse_protocol_error(message: impl Into<String>) -> LixError {
    LixError::new(PROTOCOL_ERROR, message)
}
