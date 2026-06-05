use crate::exports::lix::plugin::api::PluginError;
use crate::{
    DOCUMENT_SCHEMA_KEY, DetectedChange, File, Projection, ROOT_ENTITY_PK, reject_unknown_fields,
};
use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use encoding_rs::Encoding;
use serde_json::Value;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(crate) struct TextDocumentSnapshot {
    pub(crate) line_endings: TextLineEndings,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParsedText {
    pub(crate) document: TextDocumentSnapshot,
    pub(crate) lines: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub(crate) enum TextLineEndings {
    #[default]
    Lf,
    CrLf,
    Cr,
}

impl TextLineEndings {
    fn detect(endings: &[Self]) -> Self {
        let mut crlf_count = 0;
        let mut lf_count = 0;
        let mut cr_count = 0;

        for ending in endings {
            match ending {
                Self::Lf => lf_count += 1,
                Self::CrLf => crlf_count += 1,
                Self::Cr => cr_count += 1,
            }
        }

        if crlf_count > 0 && crlf_count >= lf_count && crlf_count >= cr_count {
            Self::CrLf
        } else if lf_count >= cr_count {
            Self::Lf
        } else {
            Self::Cr
        }
    }

    fn from_snapshot(raw: &str) -> Option<Self> {
        match raw {
            "\n" => Some(Self::Lf),
            "\r\n" => Some(Self::CrLf),
            "\r" => Some(Self::Cr),
            _ => None,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Lf => "\n",
            Self::CrLf => "\r\n",
            Self::Cr => "\r",
        }
    }
}

pub(crate) fn parse_file(file: &File) -> ParsedText {
    let (buf, encoding) = buffer_with_encoding(&file.data);
    let (decoded, _had_errors) = encoding.decode_without_bom_handling(buf);
    parse_text(&decoded)
}

fn buffer_with_encoding(buf: &[u8]) -> (&[u8], &'static Encoding) {
    if let Some((encoding, skip)) = Encoding::for_bom(buf) {
        (&buf[skip..], encoding)
    } else {
        let mut detector = EncodingDetector::new(Iso2022JpDetection::Allow);
        detector.feed(buf, true);
        (buf, detector.guess(None, Utf8Detection::Allow))
    }
}

fn parse_text(decoded: &str) -> ParsedText {
    let bytes = decoded.as_bytes();
    let mut lines = Vec::new();
    let mut endings = Vec::new();
    let mut line_start = 0;
    let mut index = 0;

    while index < bytes.len() {
        match bytes[index] {
            b'\r' => {
                lines.push(decoded[line_start..index].to_string());
                if index + 1 < bytes.len() && bytes[index + 1] == b'\n' {
                    endings.push(TextLineEndings::CrLf);
                    index += 2;
                } else {
                    endings.push(TextLineEndings::Cr);
                    index += 1;
                }
                line_start = index;
            }
            b'\n' => {
                lines.push(decoded[line_start..index].to_string());
                endings.push(TextLineEndings::Lf);
                index += 1;
                line_start = index;
            }
            _ => {
                index += 1;
            }
        }
    }

    if !decoded.is_empty() {
        lines.push(decoded[line_start..].to_string());
    }

    ParsedText {
        document: TextDocumentSnapshot {
            line_endings: TextLineEndings::detect(&endings),
        },
        lines,
    }
}

pub(crate) fn document_upsert_change(
    document: TextDocumentSnapshot,
) -> Result<DetectedChange, PluginError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "id": ROOT_ENTITY_PK,
        "line_endings": document.line_endings.as_str(),
    }))
    .map_err(|error| {
        PluginError::Internal(format!("failed to serialize text document: {error}"))
    })?;

    Ok(DetectedChange {
        entity_pk: vec![ROOT_ENTITY_PK.to_string()],
        schema_key: DOCUMENT_SCHEMA_KEY.to_string(),
        snapshot_content: Some(snapshot_content),
        metadata: None,
    })
}

pub(crate) fn parse_document_snapshot(raw: &str) -> Result<TextDocumentSnapshot, PluginError> {
    let value: Value = serde_json::from_str(raw).map_err(|error| {
        PluginError::InvalidInput(format!("invalid text document snapshot_content: {error}"))
    })?;
    let object = value.as_object().ok_or_else(|| {
        PluginError::InvalidInput("text document snapshot_content must be an object".to_string())
    })?;
    reject_unknown_fields(object.keys(), &["id", "line_endings"], "text document")?;

    let id = object.get("id").and_then(Value::as_str).ok_or_else(|| {
        PluginError::InvalidInput("text document snapshot must contain string 'id'".to_string())
    })?;
    if id != ROOT_ENTITY_PK {
        return Err(PluginError::InvalidInput(format!(
            "text document snapshot id '{id}' does not match expected '{ROOT_ENTITY_PK}'"
        )));
    }

    let line_endings = parse_line_endings_snapshot(object.get("line_endings"))?;

    Ok(TextDocumentSnapshot { line_endings })
}

fn parse_line_endings_snapshot(value: Option<&Value>) -> Result<TextLineEndings, PluginError> {
    let raw = value.and_then(Value::as_str).ok_or_else(|| {
        PluginError::InvalidInput("text document field 'line_endings' must be a string".to_string())
    })?;
    TextLineEndings::from_snapshot(raw).ok_or_else(|| {
        PluginError::InvalidInput(
            "text document field 'line_endings' must be one of '\\n', '\\r\\n', or '\\r'"
                .to_string(),
        )
    })
}

pub(crate) fn render_projection(projection: &Projection) -> Vec<u8> {
    let lines = projection.to_lines();
    let line_endings = projection.document.line_endings.as_str();
    let mut output = String::new();
    for (index, line) in lines.iter().enumerate() {
        if index != 0 {
            output.push_str(line_endings);
        }
        output.push_str(&line.line);
    }
    output.into_bytes()
}
