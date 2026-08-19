use bytes::Bytes;
use futures_core::Stream;
use futures_util::StreamExt;

use crate::LixError;

use super::wire::protocol_error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseEvent {
    pub event: String,
    pub data: String,
    pub retry: Option<u64>,
}

pub async fn next_sse_event(
    stream: &mut (impl Stream<Item = Result<Bytes, LixError>> + Unpin),
    buffered: &mut String,
    event_name: &mut String,
    retry: &mut Option<u64>,
    data_lines: &mut Vec<String>,
) -> Result<Option<SseEvent>, LixError> {
    loop {
        if let Some(event) = take_buffered_event(buffered, event_name, retry, data_lines)? {
            return Ok(Some(event));
        }
        match stream.next().await {
            Some(Ok(chunk)) => {
                let text = std::str::from_utf8(&chunk).map_err(|_| {
                    protocol_error("remote observe stream contained invalid UTF-8")
                })?;
                buffered.push_str(text);
            }
            Some(Err(error)) => return Err(error),
            None => {
                if !buffered.ends_with('\n') && !buffered.is_empty() {
                    buffered.push('\n');
                }
                return Ok(take_buffered_event(
                    buffered,
                    event_name,
                    retry,
                    data_lines,
                )?);
            }
        }
    }
}

fn take_buffered_event(
    buffered: &mut String,
    event_name: &mut String,
    retry: &mut Option<u64>,
    data_lines: &mut Vec<String>,
) -> Result<Option<SseEvent>, LixError> {
    loop {
        let Some(newline) = buffered.find('\n') else {
            return Ok(None);
        };
        let mut line = buffered.drain(..=newline).collect::<String>();
        if line.ends_with('\n') {
            line.pop();
        }
        if line.ends_with('\r') {
            line.pop();
        }
        if line.is_empty() {
            if let Some(event) = dispatch_event(event_name, retry, data_lines) {
                return Ok(Some(event));
            }
            continue;
        }
        if line.starts_with(':') {
            continue;
        }
        let (field, value) = match line.split_once(':') {
            Some((field, value)) => (field, value.strip_prefix(' ').unwrap_or(value)),
            None => (line.as_str(), ""),
        };
        match field {
            "event" => *event_name = value.to_owned(),
            "data" => data_lines.push(value.to_owned()),
            "retry" => {
                if value.bytes().all(|byte| byte.is_ascii_digit())
                    && let Ok(parsed) = value.parse::<u64>()
                {
                    *retry = Some(parsed);
                }
            }
            _ => {}
        }
    }
}

fn dispatch_event(
    event_name: &mut String,
    retry: &mut Option<u64>,
    data_lines: &mut Vec<String>,
) -> Option<SseEvent> {
    if data_lines.is_empty() {
        event_name.clear();
        *retry = None;
        return None;
    }
    let event = SseEvent {
        event: if event_name.is_empty() {
            "message".to_owned()
        } else {
            std::mem::take(event_name)
        },
        data: data_lines.join("\n"),
        retry: *retry,
    };
    data_lines.clear();
    *retry = None;
    Some(event)
}
