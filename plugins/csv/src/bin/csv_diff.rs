use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use csv::{ByteRecord, ReaderBuilder};
use csv_nose::{Quote, Sniffer};
use encoding_rs::{CoderResult, Encoding};
use itertools::Itertools;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::error::Error;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::str;
use std::{cmp, env, iter};

fn main() -> Result<(), Box<dyn Error>> {
    if true {
        test();
        return Ok(());
    }

    let [left_path, right_path] = parse_args()?;

    let left_csv = fs::read(&left_path)?;
    let left_csv = decode(&left_csv);
    let right_csv = fs::read(&right_path)?;
    let right_csv = decode(&right_csv);

    let left_rows = parse(left_path.file_name().unwrap().to_str(), &left_csv);
    let right_rows = parse(right_path.file_name().unwrap().to_str(), &right_csv);

    let _ = (left_rows, right_rows);

    Ok(())
}

fn test() {
    let mut rng = SmallRng::seed_from_u64(0);
    for _ in 0..1_000_000 {
        let mut base = random_csv(&mut rng)
            .into_iter()
            .map(|row| (RowId(rng.random()), row))
            .collect_vec();
        let file = random_csv(&mut rng);
        let changes = detect_changes(&base, &file);
        assert_eq!(
            changes.is_empty(),
            base.iter().map(|(_, row)| row).eq(file.iter())
        );
        for change in changes {
            apply(&mut base, change);
        }
        assert!(base.iter().map(|(_, row)| row).eq(file.iter()));
    }
}

fn random_csv<'a>(rng: &mut (impl Rng + ?Sized)) -> Vec<Vec<Cow<'a, str>>> {
    let random_cell_alphabet_len: u8 = rng.random_range(1..=6);
    let width = rng.random_range(1..=10);
    let height = rng.random_range(1..=10);

    (0..height)
        .map(|_| {
            (0..width)
                .map(|_| {
                    let offset = rng.random_range(0..random_cell_alphabet_len);
                    Cow::Owned(char::from(b'a' + offset).to_string())
                })
                .collect()
        })
        .collect()
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct RowId([u8; 8]);

#[expect(clippy::enum_variant_names)]
enum Change {
    RowInsert {
        id: RowId,
        index: usize,
        row: Vec<Cow<'static, str>>,
    },
    RowDelete {
        id: RowId,
    },
    RowUpdate {
        id: RowId,
        row: Vec<Cow<'static, str>>,
    },
}

fn detect_changes(base: &[(RowId, Vec<Cow<'_, str>>)], file: &[Vec<Cow<'_, str>>]) -> Vec<Change> {
    let ops = diff_by(base, file, |(_, row), file_row| row == file_row).collect_vec();
    let mut changes = Vec::new();
    let mut used_ids = base.iter().map(|(id, _)| *id).collect::<BTreeSet<_>>();
    let mut base_index = 0;
    let mut file_index = 0;

    for op in ops {
        match op {
            Op::Equal => {
                base_index += 1;
                file_index += 1;
            }
            Op::Replace => {
                let (id, _) = base[base_index];
                changes.push(Change::RowUpdate {
                    id,
                    row: owned_row(&file[file_index]),
                });
                base_index += 1;
                file_index += 1;
            }
            Op::Delete => {
                let (id, _) = base[base_index];
                changes.push(Change::RowDelete { id });
                base_index += 1;
            }
            Op::Insert => {
                changes.push(Change::RowInsert {
                    id: fresh_row_id(&mut used_ids),
                    index: file_index,
                    row: owned_row(&file[file_index]),
                });
                file_index += 1;
            }
        }
    }

    changes
}
fn apply(base: &mut Vec<(RowId, Vec<Cow<'_, str>>)>, change: Change) {
    match change {
        Change::RowInsert { id, index, row } => {
            assert!(
                !base.iter().any(|(existing_id, _)| *existing_id == id),
                "inserted row id already exists: {id:?}"
            );
            base.insert(index, (id, row));
        }
        Change::RowDelete { id } => {
            let index = base
                .iter()
                .position(|(existing_id, _)| *existing_id == id)
                .unwrap_or_else(|| panic!("deleted row id does not exist: {id:?}"));
            base.remove(index);
        }
        Change::RowUpdate { id, row } => {
            let (_, existing_row) = base
                .iter_mut()
                .find(|(existing_id, _)| *existing_id == id)
                .unwrap_or_else(|| panic!("updated row id does not exist: {id:?}"));
            *existing_row = row;
        }
    }
}

fn owned_row(row: &[Cow<'_, str>]) -> Vec<Cow<'static, str>> {
    row.iter()
        .map(|cell| Cow::Owned(cell.as_ref().to_owned()))
        .collect()
}

fn fresh_row_id(used_ids: &mut BTreeSet<RowId>) -> RowId {
    for id in (0_u64..).map(|n| RowId(n.to_be_bytes())) {
        if used_ids.insert(id) {
            return id;
        }
    }
    unreachable!("u64 row id space exhausted")
}

fn parse_args() -> Result<[PathBuf; 2], Box<dyn Error>> {
    let mut args = env::args_os();
    let program = args.next().unwrap_or_else(|| OsString::from("csv_diff"));
    let Some(left_path) = args.next() else {
        return Err(usage(&program).into());
    };
    let Some(right_path) = args.next() else {
        return Err(usage(&program).into());
    };
    if args.next().is_some() {
        return Err(usage(&program).into());
    }

    Ok([PathBuf::from(left_path), PathBuf::from(right_path)])
}

fn usage(program: &OsString) -> String {
    format!(
        "usage: {} <left.csv|tsv> <right.csv|tsv>",
        program.to_string_lossy()
    )
}

fn decode(csv: &[u8]) -> Cow<'_, str> {
    let (buf, encoding) = buffer_with_encoding(csv);
    if encoding == encoding_rs::UTF_8 {
        return String::from_utf8_lossy(buf);
    }
    let mut decoder = encoding.new_decoder_without_bom_handling();
    let mut decoded = String::with_capacity(decoder.max_utf8_buffer_length(buf.len()).unwrap());
    let (result, read, _replaced) = decoder.decode_to_string(buf, &mut decoded, true);
    assert_eq!(result, CoderResult::InputEmpty);
    assert_eq!(read, buf.len());
    Cow::Owned(decoded)
}

fn parse<'a>(filename: Option<&'_ str>, csv: &'a str) -> Vec<Vec<Cow<'a, str>>> {
    let dialect = dialect_for_filename(filename, csv);
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
    while reader.read_byte_record(&mut record).unwrap() {
        let mut row = Vec::with_capacity(record.len());
        for field in &record {
            let field = str::from_utf8(field).unwrap();
            row.push(Cow::Owned(field.to_owned()));
        }
        rows.push(row);
    }
    rows
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct CsvDialect {
    delimiter: u8,
    quote: Quote,
}

fn dialect_for_filename(filename: Option<&str>, decoded: &str) -> CsvDialect {
    let sniffer = Sniffer::new();
    if let Ok(metadata) = sniffer.sniff_bytes(decoded.as_bytes()) {
        return CsvDialect {
            delimiter: metadata.dialect.delimiter,
            quote: metadata.dialect.quote,
        };
    }

    CsvDialect {
        delimiter: fallback_delimiter(filename, decoded),
        quote: Quote::Some(b'"'),
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

pub fn diff_by<'a, T, U>(
    a: &'a [T],
    b: &'a [U],
    mut eq: impl FnMut(&T, &U) -> bool + 'a,
) -> impl Iterator<Item = Op> + 'a {
    let prefix = a.iter().zip(b.iter()).take_while(|(a, b)| eq(a, b)).count();

    let a_rest = &a[prefix..];
    let b_rest = &b[prefix..];
    let suffix = a_rest
        .iter()
        .rev()
        .zip(b_rest.iter().rev())
        .take_while(|(a, b)| eq(a, b))
        .count()
        .min(a_rest.len())
        .min(b_rest.len());

    let a_mid = a.len() - prefix - suffix;
    let b_mid = b.len() - prefix - suffix;
    let replace = cmp::min(a_mid, b_mid);

    iter::empty()
        .chain((0..prefix).map(|_| Op::Equal))
        .chain(
            a[prefix..prefix + replace]
                .iter()
                .zip_eq(&b[prefix..prefix + replace])
                .map(move |(a, b)| if eq(a, b) { Op::Equal } else { Op::Replace }),
        )
        .chain((replace..a_mid).map(|_| Op::Delete))
        .chain((replace..b_mid).map(|_| Op::Insert))
        .chain((0..suffix).map(|_| Op::Equal))
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Op {
    Equal,
    Replace,
    Insert,
    Delete,
}
