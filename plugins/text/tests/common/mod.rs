#![allow(dead_code)]

use serde::Deserialize;
use text_plugin::{DetectedChange, File};

#[derive(Debug, Deserialize)]
pub struct LineSnapshot {
    pub content_base64: String,
    pub ending: String,
}

#[derive(Debug, Deserialize)]
pub struct DocumentSnapshot {
    pub line_ids: Vec<String>,
}

pub fn file_from_bytes(data: &[u8]) -> File {
    File {
        data: data.to_vec(),
    }
}

pub fn parse_line_snapshot(change: &DetectedChange) -> LineSnapshot {
    let raw = change
        .snapshot_content
        .as_ref()
        .expect("line snapshot should exist");
    serde_json::from_str(raw).expect("line snapshot should parse")
}

pub fn parse_document_snapshot(change: &DetectedChange) -> DocumentSnapshot {
    let raw = change
        .snapshot_content
        .as_ref()
        .expect("document snapshot should exist");
    serde_json::from_str(raw).expect("document snapshot should parse")
}
