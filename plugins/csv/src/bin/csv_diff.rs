use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use csv::{ByteRecord, ReaderBuilder};
use csv_nose::{Quote, Sniffer};
use encoding_rs::{CoderResult, Encoding};
use itertools::Itertools;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::borrow::Cow;
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
        let random_base = random_csv(&mut rng);
        let base_len = random_base.len();
        let mut base = random_base
            .into_iter()
            .enumerate()
            .map(|(offset, cells)| Row {
                id: RowId::random(&mut rng),
                index: evenly_spaced_fractional_index(offset, base_len),
                cells,
            })
            .collect_vec();
        let file = random_csv(&mut rng);
        let changes = detect_changes(&base, &file);
        assert_eq!(
            changes.is_empty(),
            base.iter().map(|row| &row.cells).eq(file.iter())
        );
        for change in changes {
            apply(&mut base, change);
        }
        assert!(base.iter().map(|row| &row.cells).eq(file.iter()));
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

impl RowId {
    fn random(rng: &mut impl Rng) -> Self {
        let mut bytes = [0_u8; 8];
        rng.fill(&mut bytes);
        Self(bytes)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct FractionalIndex(u128);

#[derive(Clone, PartialEq, Eq, Debug)]
struct Row<'a> {
    id: RowId,
    index: FractionalIndex,
    cells: Vec<Cow<'a, str>>,
}

#[expect(clippy::enum_variant_names)]
enum Change {
    RowInsert {
        id: RowId,
        index: FractionalIndex,
        row: Vec<Cow<'static, str>>,
    },
    RowDelete {
        id: RowId,
    },
    RowUpdate {
        id: RowId,
        index: FractionalIndex,
        row: Vec<Cow<'static, str>>,
    },
}

fn detect_changes(base: &[Row<'_>], file: &[Vec<Cow<'_, str>>]) -> Vec<Change> {
    let ops = diff_by(base, file, |row, file_row| row.cells == *file_row).collect_vec();
    let mut changes = Vec::new();
    let mut rng = rand::rng();
    let mut base_index = 0;
    let mut file_index = 0;
    let mut previous_fractional_index = None;

    for op in ops {
        match op {
            Op::Equal => {
                previous_fractional_index = Some(base[base_index].index);
                base_index += 1;
                file_index += 1;
            }
            Op::Replace => {
                let row = &base[base_index];
                changes.push(Change::RowUpdate {
                    id: row.id,
                    index: row.index,
                    row: owned_row(&file[file_index]),
                });
                previous_fractional_index = Some(row.index);
                base_index += 1;
                file_index += 1;
            }
            Op::Delete => {
                changes.push(Change::RowDelete {
                    id: base[base_index].id,
                });
                base_index += 1;
            }
            Op::Insert => {
                let next_fractional_index = base.get(base_index).map(|row| row.index);
                let index =
                    fractional_index_between(previous_fractional_index, next_fractional_index);
                changes.push(Change::RowInsert {
                    id: RowId::random(&mut rng),
                    index,
                    row: owned_row(&file[file_index]),
                });
                previous_fractional_index = Some(index);
                file_index += 1;
            }
        }
    }

    changes
}

fn apply(base: &mut Vec<Row<'_>>, change: Change) {
    match change {
        Change::RowInsert { id, index, row } => {
            assert!(
                !base.iter().any(|existing| existing.id == id),
                "inserted row id already exists: {id:?}"
            );
            insert_row_sorted(
                base,
                Row {
                    id,
                    index,
                    cells: row,
                },
            );
        }
        Change::RowDelete { id } => {
            let index = base
                .iter()
                .position(|existing| existing.id == id)
                .unwrap_or_else(|| panic!("deleted row id does not exist: {id:?}"));
            base.remove(index);
        }
        Change::RowUpdate {
            id,
            index: new_index,
            row,
        } => {
            let existing_index = base
                .iter()
                .position(|existing| existing.id == id)
                .unwrap_or_else(|| panic!("updated row id does not exist: {id:?}"));
            let mut existing = base.remove(existing_index);
            existing.index = new_index;
            existing.cells = row;
            insert_row_sorted(base, existing);
        }
    }
}

fn insert_row_sorted<'a>(base: &mut Vec<Row<'a>>, row: Row<'a>) {
    let insertion_index = base.partition_point(|existing| {
        existing
            .index
            .cmp(&row.index)
            .then_with(|| existing.cells.cmp(&row.cells))
            .is_lt()
    });
    base.insert(insertion_index, row);
}

fn evenly_spaced_fractional_index(offset: usize, len: usize) -> FractionalIndex {
    let step = u128::MAX / (len as u128 + 1);
    FractionalIndex(step * (offset as u128 + 1))
}
fn fractional_index_between(
    previous: Option<FractionalIndex>,
    next: Option<FractionalIndex>,
) -> FractionalIndex {
    let lower = previous.map_or(0, |index| index.0);
    let upper = next.map_or(u128::MAX, |index| index.0);
    assert!(
        lower <= upper,
        "fractional index bounds are out of order: previous={previous:?}, next={next:?}"
    );
    assert_ne!(
        lower, upper,
        "cannot generate fractional index between identical indexes: {previous:?}"
    );
    let gap = upper - lower;
    assert!(
        gap > 1,
        "fractional index space exhausted between previous={previous:?} and next={next:?}"
    );
    FractionalIndex(lower + gap / 2)
}

fn owned_row(row: &[Cow<'_, str>]) -> Vec<Cow<'static, str>> {
    row.iter()
        .map(|cell| Cow::Owned(cell.as_ref().to_owned()))
        .collect()
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
