use crate::exports::lix::plugin::api::PluginError;
use crate::{DetectedChange, File};
use crate::{Projection, ROOT_ENTITY_PK, TABLE_SCHEMA_KEY, reject_unknown_fields};
use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use csv::{ByteRecord, QuoteStyle, ReaderBuilder, Terminator, WriterBuilder};
use csv_nose::{Quote, Sniffer};
use encoding_rs::{CoderResult, Encoding};
use serde_json::Value;
use std::borrow::Cow;
use std::path::Path;
use std::str;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct CsvDialect {
    delimiter: u8,
    quote: Quote,
    terminator: CsvTerminator,
}

impl Default for CsvDialect {
    fn default() -> Self {
        Self {
            delimiter: b',',
            quote: Quote::Some(b'"'),
            terminator: CsvTerminator::default(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
enum CsvTerminator {
    #[default]
    Lf,
    CrLf,
    Cr,
}

impl CsvTerminator {
    fn detect(decoded: &str) -> Self {
        let mut crlf_count = 0;
        let mut lf_count = 0;
        let mut cr_count = 0;
        let bytes = decoded.as_bytes();
        let mut index = 0;

        while index < bytes.len() {
            if bytes[index] == b'\r' {
                if index + 1 < bytes.len() && bytes[index + 1] == b'\n' {
                    crlf_count += 1;
                    index += 2;
                    continue;
                }
                cr_count += 1;
            } else if bytes[index] == b'\n' {
                lf_count += 1;
            }
            index += 1;
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

    fn as_str(self) -> &'static str {
        match self {
            Self::Lf => "\n",
            Self::CrLf => "\r\n",
            Self::Cr => "\r",
        }
    }

    fn to_csv_terminator(self) -> Terminator {
        match self {
            Self::Lf => Terminator::Any(b'\n'),
            Self::CrLf => Terminator::CRLF,
            Self::Cr => Terminator::Any(b'\r'),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TableSnapshot {
    pub(crate) dialect: CsvDialect,
}

pub(crate) fn table_upsert_change(dialect: CsvDialect) -> Result<DetectedChange, PluginError> {
    let snapshot_content = serde_json::to_string(&serde_json::json!({
        "id": ROOT_ENTITY_PK,
        "dialect": dialect_snapshot_content(dialect),
    }))
    .map_err(|error| PluginError::Internal(format!("failed to serialize CSV table: {error}")))?;

    Ok(DetectedChange {
        entity_pk: vec![ROOT_ENTITY_PK.to_string()],
        schema_key: TABLE_SCHEMA_KEY.to_string(),
        snapshot_content: Some(snapshot_content),
        metadata: None,
    })
}

fn dialect_snapshot_content(dialect: CsvDialect) -> Value {
    serde_json::json!({
        "delimiter": byte_to_latin1_string(dialect.delimiter),
        "quote": match dialect.quote {
            Quote::None => Value::Null,
            Quote::Some(quote) => Value::from(byte_to_latin1_string(quote)),
        },
        "terminator": dialect.terminator.as_str(),
    })
}

pub(crate) fn parse_table_snapshot(raw: &str) -> Result<TableSnapshot, PluginError> {
    let value: Value = serde_json::from_str(raw).map_err(|error| {
        PluginError::InvalidInput(format!("invalid csv table snapshot_content: {error}"))
    })?;
    let object = value.as_object().ok_or_else(|| {
        PluginError::InvalidInput("csv table snapshot_content must be an object".to_string())
    })?;
    reject_unknown_fields(object.keys(), &["id", "dialect"], "csv table")?;

    let id = object.get("id").and_then(Value::as_str).ok_or_else(|| {
        PluginError::InvalidInput("csv table snapshot must contain string 'id'".to_string())
    })?;
    if id != ROOT_ENTITY_PK {
        return Err(PluginError::InvalidInput(format!(
            "csv table snapshot id '{id}' does not match expected '{ROOT_ENTITY_PK}'"
        )));
    }

    let dialect = parse_dialect_snapshot(object.get("dialect").ok_or_else(|| {
        PluginError::InvalidInput("csv table snapshot must contain object 'dialect'".to_string())
    })?)?;

    Ok(TableSnapshot { dialect })
}

fn parse_dialect_snapshot(value: &Value) -> Result<CsvDialect, PluginError> {
    let object = value.as_object().ok_or_else(|| {
        PluginError::InvalidInput("csv table dialect must be an object".to_string())
    })?;
    reject_unknown_fields(
        object.keys(),
        &["delimiter", "quote", "terminator"],
        "csv table dialect",
    )?;

    let delimiter = parse_dialect_byte_string(object.get("delimiter"), "delimiter")?;
    let quote = match object.get("quote") {
        Some(Value::Null) => Quote::None,
        Some(value) => Quote::Some(parse_dialect_byte_string(Some(value), "quote")?),
        None => {
            return Err(PluginError::InvalidInput(
                "csv table dialect must contain 'quote'".to_string(),
            ));
        }
    };
    let terminator = parse_dialect_terminator(object.get("terminator"))?;

    Ok(CsvDialect {
        delimiter,
        quote,
        terminator,
    })
}

fn byte_to_latin1_string(byte: u8) -> String {
    char::from(byte).to_string()
}

fn parse_dialect_terminator(value: Option<&Value>) -> Result<CsvTerminator, PluginError> {
    let raw = value.and_then(Value::as_str).ok_or_else(|| {
        PluginError::InvalidInput(
            "csv table dialect field 'terminator' must be a string".to_string(),
        )
    })?;
    CsvTerminator::from_snapshot(raw).ok_or_else(|| {
        PluginError::InvalidInput(
            "csv table dialect field 'terminator' must be one of '\\n', '\\r\\n', or '\\r'"
                .to_string(),
        )
    })
}

fn parse_dialect_byte_string(value: Option<&Value>, field: &str) -> Result<u8, PluginError> {
    let raw = value.and_then(Value::as_str).ok_or_else(|| {
        PluginError::InvalidInput(format!(
            "csv table dialect field '{field}' must be a single-byte string"
        ))
    })?;
    let mut chars = raw.chars();
    let Some(ch) = chars.next() else {
        return Err(PluginError::InvalidInput(format!(
            "csv table dialect field '{field}' must not be empty"
        )));
    };
    if chars.next().is_some() {
        return Err(PluginError::InvalidInput(format!(
            "csv table dialect field '{field}' must contain exactly one character"
        )));
    }
    u8::try_from(u32::from(ch)).map_err(|_| {
        PluginError::InvalidInput(format!(
            "csv table dialect field '{field}' must be in the range U+0000 through U+00FF"
        ))
    })
}

pub(crate) fn parse_file(file: &File) -> Result<(Vec<Vec<String>>, CsvDialect), PluginError> {
    let decoded = decode(&file.data)?;
    let dialect = dialect_for_filename(None, &decoded);
    let rows = parse_rows(&decoded, dialect)?;
    Ok((rows, dialect))
}

fn decode(csv: &[u8]) -> Result<Cow<'_, str>, PluginError> {
    let (buf, encoding) = buffer_with_encoding(csv);
    if encoding == encoding_rs::UTF_8 {
        return Ok(String::from_utf8_lossy(buf));
    }
    let mut decoder = encoding.new_decoder_without_bom_handling();
    let capacity = decoder.max_utf8_buffer_length(buf.len()).ok_or_else(|| {
        PluginError::Internal("CSV input is too large to decode as UTF-8".to_string())
    })?;
    let mut decoded = String::with_capacity(capacity);
    let (result, read, _replaced) = decoder.decode_to_string(buf, &mut decoded, true);
    if result != CoderResult::InputEmpty || read != buf.len() {
        return Err(PluginError::InvalidInput(
            "failed to decode complete CSV input".to_string(),
        ));
    }
    Ok(Cow::Owned(decoded))
}

fn parse_rows(csv: &str, dialect: CsvDialect) -> Result<Vec<Vec<String>>, PluginError> {
    let mut reader_builder = ReaderBuilder::new();
    reader_builder
        .flexible(true)
        .has_headers(false)
        .delimiter(dialect.delimiter);
    match dialect.quote {
        Quote::None => {
            reader_builder.quoting(false);
        }
        Quote::Some(quote) => {
            reader_builder.quoting(true).quote(quote);
        }
    }
    let mut reader = reader_builder.from_reader(csv.as_bytes());

    let mut rows = Vec::new();
    let mut record = ByteRecord::new();
    while reader.read_byte_record(&mut record).map_err(|error| {
        PluginError::InvalidInput(format!("failed to parse CSV input as records: {error}"))
    })? {
        let mut row = Vec::with_capacity(record.len());
        for field in &record {
            let field = str::from_utf8(field).map_err(|error| {
                PluginError::InvalidInput(format!("CSV field is not valid UTF-8: {error}"))
            })?;
            row.push(field.to_string());
        }
        rows.push(row);
    }
    Ok(rows)
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

fn dialect_for_filename(filename: Option<&str>, decoded: &str) -> CsvDialect {
    let sniffer = Sniffer::new();
    if let Ok(metadata) = sniffer.sniff_bytes(decoded.as_bytes()) {
        return CsvDialect {
            delimiter: metadata.dialect.delimiter,
            quote: metadata.dialect.quote,
            terminator: CsvTerminator::detect(decoded),
        };
    }

    CsvDialect {
        delimiter: fallback_delimiter(filename, decoded),
        quote: Quote::Some(b'"'),
        terminator: CsvTerminator::detect(decoded),
    }
}

fn fallback_delimiter(filename: Option<&str>, decoded: &str) -> u8 {
    match filename.and_then(|f| Path::new(f).extension().and_then(|ext| ext.to_str())) {
        Some(extension) if extension.eq_ignore_ascii_case("tsv") => b'\t',
        Some(extension) if extension.eq_ignore_ascii_case("csv") => b',',
        _ => {
            let buf = decoded.as_bytes();
            let sample = &buf[..buf.len().min(8 * 1024)];
            let comma_count = bytecount::count(sample, b',');
            let tab_count = bytecount::count(sample, b'\t');
            if tab_count > comma_count { b'\t' } else { b',' }
        }
    }
}

pub(crate) fn render_projection(projection: &Projection) -> Result<Vec<u8>, PluginError> {
    let mut writer_builder = WriterBuilder::new();
    writer_builder
        .flexible(true)
        .has_headers(false)
        .delimiter(projection.dialect.delimiter)
        .terminator(projection.dialect.terminator.to_csv_terminator());
    match projection.dialect.quote {
        Quote::None => {
            writer_builder.quote_style(QuoteStyle::Never);
        }
        Quote::Some(quote) => {
            writer_builder.quote(quote);
        }
    }
    let mut writer = writer_builder.from_writer(Vec::new());

    for row in projection.to_rows() {
        writer.write_record(&row.cells).map_err(|error| {
            PluginError::Internal(format!("failed to render CSV row '{}': {error}", row.id))
        })?;
    }

    writer.into_inner().map_err(|error| {
        PluginError::Internal(format!("failed to finish rendering CSV: {}", error.error()))
    })
}
