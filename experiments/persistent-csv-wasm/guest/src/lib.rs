//! Minimal core-Wasm CSV experiment.
//!
//! This deliberately does not use the production plugin or its WIT. It tests one
//! architectural variable: reparsing/rendering complete blobs per call versus
//! retaining a parsed document in the guest and exchanging a byte splice.

use std::cell::UnsafeCell;
use std::mem::size_of;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const PATCH_MAGIC: u64 = 0x4c49_585f_4353_5631; // LIX_CSV1
const PATCH_WORDS: usize = 8;
const PATCH_HEADER_LEN: usize = PATCH_WORDS * size_of::<u64>();

#[derive(Clone, Copy, Debug)]
struct RowMeta {
    start: usize,
    end: usize,
    field_start: usize,
    field_len: usize,
    hash: u64,
}

#[derive(Debug, Default)]
struct ParsedDocument {
    rows: Vec<RowMeta>,
    field_hashes: Vec<u64>,
}

#[derive(Debug)]
struct Document {
    data: Vec<u8>,
    parsed: ParsedDocument,
}

#[derive(Debug, Default)]
struct GuestState {
    document: Option<Document>,
    result: Vec<u8>,
    changed_rows: u64,
    changed_cells: u64,
}

/// The benchmark ABI is single-threaded. Wasm instances cannot call these
/// exports concurrently, so an `UnsafeCell` avoids pulling a mutex into the
/// guest and models per-instance plugin state.
struct StateCell(UnsafeCell<GuestState>);

unsafe impl Sync for StateCell {}

static STATE: StateCell = StateCell(UnsafeCell::new(GuestState {
    document: None,
    result: Vec::new(),
    changed_rows: 0,
    changed_cells: 0,
}));

fn state() -> &'static mut GuestState {
    // SAFETY: a core Wasm instance is single-threaded and every exported call
    // completes synchronously before the host starts another one.
    unsafe { &mut *STATE.0.get() }
}

fn hash_byte(hash: u64, byte: u8) -> u64 {
    (hash ^ u64::from(byte)).wrapping_mul(FNV_PRIME)
}

fn row_hash(fields: &[u64]) -> u64 {
    let mut hash = FNV_OFFSET;
    for field in fields {
        hash ^= field.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        hash = hash.rotate_left(11).wrapping_mul(FNV_PRIME);
    }
    hash ^ fields.len() as u64
}

/// Parse RFC-4180-relevant structure: commas, CR/LF/CRLF records, quoted
/// fields, escaped quotes, and newlines embedded in quoted fields. Hashes are
/// over decoded cell bytes rather than CSV syntax, so this does semantic cell
/// identification rather than merely scanning for newlines.
fn parse_document(input: &[u8]) -> Result<ParsedDocument, u32> {
    if input.is_empty() {
        return Ok(ParsedDocument::default());
    }

    let estimated_rows = input.len() / 96 + 1;
    let mut parsed = ParsedDocument {
        rows: Vec::with_capacity(estimated_rows),
        field_hashes: Vec::with_capacity(estimated_rows * 6),
    };
    let mut row_start = 0usize;
    let mut row_field_start = 0usize;
    let mut field_hash = FNV_OFFSET;
    let mut at_field_start = true;
    let mut in_quotes = false;
    let mut just_closed_quote = false;
    let mut index = 0usize;

    let finish_field = |parsed: &mut ParsedDocument, hash: &mut u64| {
        parsed.field_hashes.push(*hash);
        *hash = FNV_OFFSET;
    };
    let finish_row = |parsed: &mut ParsedDocument, start: usize, end: usize, field_start: usize| {
        let field_len = parsed.field_hashes.len() - field_start;
        let hash = row_hash(&parsed.field_hashes[field_start..]);
        parsed.rows.push(RowMeta {
            start,
            end,
            field_start,
            field_len,
            hash,
        });
    };

    while index < input.len() {
        let byte = input[index];
        if in_quotes {
            if byte == b'"' {
                if input.get(index + 1) == Some(&b'"') {
                    field_hash = hash_byte(field_hash, b'"');
                    index += 2;
                } else {
                    in_quotes = false;
                    just_closed_quote = true;
                    index += 1;
                }
            } else {
                field_hash = hash_byte(field_hash, byte);
                index += 1;
            }
            continue;
        }

        if at_field_start && byte == b'"' {
            in_quotes = true;
            at_field_start = false;
            index += 1;
            continue;
        }

        match byte {
            b',' => {
                finish_field(&mut parsed, &mut field_hash);
                at_field_start = true;
                just_closed_quote = false;
                index += 1;
            }
            b'\n' => {
                finish_field(&mut parsed, &mut field_hash);
                index += 1;
                finish_row(&mut parsed, row_start, index, row_field_start);
                row_start = index;
                row_field_start = parsed.field_hashes.len();
                at_field_start = true;
                just_closed_quote = false;
            }
            b'\r' => {
                finish_field(&mut parsed, &mut field_hash);
                index += 1;
                if input.get(index) == Some(&b'\n') {
                    index += 1;
                }
                finish_row(&mut parsed, row_start, index, row_field_start);
                row_start = index;
                row_field_start = parsed.field_hashes.len();
                at_field_start = true;
                just_closed_quote = false;
            }
            _ => {
                // After a closing quote, only a delimiter or terminator is legal.
                if just_closed_quote {
                    return Err(2);
                }
                field_hash = hash_byte(field_hash, byte);
                at_field_start = false;
                index += 1;
            }
        }
    }

    if in_quotes {
        return Err(3);
    }
    if row_start < input.len() {
        finish_field(&mut parsed, &mut field_hash);
        finish_row(&mut parsed, row_start, input.len(), row_field_start);
    }
    Ok(parsed)
}

fn count_changes(old: &ParsedDocument, new: &ParsedDocument) -> (u64, u64) {
    let common_rows = old.rows.len().min(new.rows.len());
    let mut changed_rows = old.rows.len().abs_diff(new.rows.len()) as u64;
    let mut changed_cells = 0u64;

    for index in 0..common_rows {
        let old_row = old.rows[index];
        let new_row = new.rows[index];
        if old_row.hash == new_row.hash && old_row.field_len == new_row.field_len {
            continue;
        }
        changed_rows += 1;
        let old_fields =
            &old.field_hashes[old_row.field_start..old_row.field_start + old_row.field_len];
        let new_fields =
            &new.field_hashes[new_row.field_start..new_row.field_start + new_row.field_len];
        changed_cells += old_fields
            .iter()
            .zip(new_fields)
            .filter(|(left, right)| left != right)
            .count() as u64;
        changed_cells += old_fields.len().abs_diff(new_fields.len()) as u64;
    }
    for row in old.rows.iter().skip(common_rows) {
        changed_cells += row.field_len as u64;
    }
    for row in new.rows.iter().skip(common_rows) {
        changed_cells += row.field_len as u64;
    }
    (changed_rows, changed_cells)
}

fn checked_input(pointer: u32, length: u32) -> Result<&'static [u8], u32> {
    let pointer = pointer as usize;
    let length = length as usize;
    if length == 0 {
        return Ok(&[]);
    }
    if pointer == 0 && length != 0 {
        return Err(10);
    }
    // SAFETY: input pointers are returned by `guest_alloc`, initialized by the
    // host, and retained until the synchronous export returns.
    Ok(unsafe { std::slice::from_raw_parts(pointer as *const u8, length) })
}

#[unsafe(no_mangle)]
pub extern "C" fn guest_alloc(length: u32) -> u32 {
    if length == 0 {
        return 0;
    }
    let mut bytes = Vec::<u8>::with_capacity(length as usize);
    let pointer = bytes.as_mut_ptr();
    std::mem::forget(bytes);
    pointer as u32
}

#[unsafe(no_mangle)]
pub extern "C" fn guest_dealloc(pointer: u32, length: u32) {
    if pointer == 0 || length == 0 {
        return;
    }
    // SAFETY: the pair came from `guest_alloc` and is deallocated exactly once.
    unsafe {
        drop(Vec::from_raw_parts(pointer as *mut u8, 0, length as usize));
    }
}

/// Stateless lower bound for the current shape: receive both complete views,
/// parse both semantically, identify changed rows/cells, and return a complete
/// rendered blob. The real component ABI also serializes entity JSON, so this
/// intentionally favors the baseline.
#[unsafe(no_mangle)]
pub extern "C" fn stateless_diff_and_render(
    old_pointer: u32,
    old_length: u32,
    new_pointer: u32,
    new_length: u32,
) -> u32 {
    let Ok(old_input) = checked_input(old_pointer, old_length) else {
        return 10;
    };
    let Ok(new_input) = checked_input(new_pointer, new_length) else {
        return 10;
    };
    let Ok(old) = parse_document(old_input) else {
        return 20;
    };
    let Ok(new) = parse_document(new_input) else {
        return 21;
    };
    let (changed_rows, changed_cells) = count_changes(&old, &new);
    let state = state();
    state.changed_rows = changed_rows;
    state.changed_cells = changed_cells;
    state.result.clear();
    state.result.extend_from_slice(new_input);
    0
}

/// Cold-load a complete CSV and retain compact row/cell metadata in this Wasm
/// instance. This is the cost paid on session/document cache miss.
#[unsafe(no_mangle)]
pub extern "C" fn hydrate(pointer: u32, length: u32) -> u32 {
    let Ok(input) = checked_input(pointer, length) else {
        return 10;
    };
    let Ok(parsed) = parse_document(input) else {
        return 20;
    };
    state().document = Some(Document {
        data: input.to_vec(),
        parsed,
    });
    0
}

fn find_row(rows: &[RowMeta], offset: usize) -> Option<usize> {
    let insertion = rows.partition_point(|row| row.end <= offset);
    let row = *rows.get(insertion)?;
    (row.start <= offset && offset < row.end).then_some(insertion)
}

fn put_word(output: &mut Vec<u8>, word: u64) {
    output.extend_from_slice(&word.to_le_bytes());
}

/// Apply a byte splice to the retained document, reparse only the touched CSV
/// row, compare decoded cell hashes, and emit a splice response. Multi-row
/// patches return 32; the production design would use a rope and parse a small
/// affected window rather than this intentionally small prototype.
#[unsafe(no_mangle)]
pub extern "C" fn apply_splice(
    offset: u32,
    delete_length: u32,
    insert_pointer: u32,
    insert_length: u32,
) -> u32 {
    let Ok(insert) = checked_input(insert_pointer, insert_length) else {
        return 10;
    };
    let insert = insert.to_vec();
    let state = state();
    let Some(document) = state.document.as_mut() else {
        return 30;
    };
    let offset = offset as usize;
    let delete_length = delete_length as usize;
    let Some(row_index) = find_row(&document.parsed.rows, offset) else {
        return 31;
    };
    let old_row = document.parsed.rows[row_index];
    let Some(delete_end) = offset.checked_add(delete_length) else {
        return 31;
    };
    if delete_end > old_row.end {
        return 32;
    }
    let old_fields = document.parsed.field_hashes
        [old_row.field_start..old_row.field_start + old_row.field_len]
        .to_vec();

    if delete_length == insert.len() {
        document.data[offset..delete_end].copy_from_slice(&insert);
    } else {
        document
            .data
            .splice(offset..delete_end, insert.iter().copied());
    }
    let delta = insert.len() as isize - delete_length as isize;
    let Some(new_row_end) = old_row.end.checked_add_signed(delta) else {
        return 33;
    };
    let Ok(reparsed) = parse_document(&document.data[old_row.start..new_row_end]) else {
        return 34;
    };
    if reparsed.rows.len() != 1 || reparsed.rows[0].end != new_row_end - old_row.start {
        return 32;
    }
    let new_fields = reparsed.field_hashes;
    let changed_cells = old_fields
        .iter()
        .zip(&new_fields)
        .filter(|(left, right)| left != right)
        .count()
        + old_fields.len().abs_diff(new_fields.len());

    let old_field_end = old_row.field_start + old_row.field_len;
    document.parsed.field_hashes.splice(
        old_row.field_start..old_field_end,
        new_fields.iter().copied(),
    );
    let field_delta = new_fields.len() as isize - old_row.field_len as isize;
    document.parsed.rows[row_index] = RowMeta {
        start: old_row.start,
        end: new_row_end,
        field_start: old_row.field_start,
        field_len: new_fields.len(),
        hash: row_hash(&new_fields),
    };
    for row in document.parsed.rows.iter_mut().skip(row_index + 1) {
        row.start = row.start.checked_add_signed(delta).unwrap_or(usize::MAX);
        row.end = row.end.checked_add_signed(delta).unwrap_or(usize::MAX);
        row.field_start = row
            .field_start
            .checked_add_signed(field_delta)
            .unwrap_or(usize::MAX);
    }

    state.changed_rows = 1;
    state.changed_cells = changed_cells as u64;
    state.result.clear();
    state.result.reserve(PATCH_HEADER_LEN + insert.len());
    put_word(&mut state.result, PATCH_MAGIC);
    put_word(&mut state.result, offset as u64);
    put_word(&mut state.result, delete_length as u64);
    put_word(&mut state.result, insert.len() as u64);
    put_word(&mut state.result, row_index as u64);
    put_word(&mut state.result, old_row.hash);
    put_word(&mut state.result, row_hash(&new_fields));
    put_word(&mut state.result, changed_cells as u64);
    state.result.extend_from_slice(&insert);
    0
}

#[unsafe(no_mangle)]
pub extern "C" fn result_pointer() -> u32 {
    state().result.as_ptr() as u32
}

#[unsafe(no_mangle)]
pub extern "C" fn result_length() -> u32 {
    state().result.len() as u32
}

#[unsafe(no_mangle)]
pub extern "C" fn changed_rows() -> u64 {
    state().changed_rows
}

#[unsafe(no_mangle)]
pub extern "C" fn changed_cells() -> u64 {
    state().changed_cells
}

#[unsafe(no_mangle)]
pub extern "C" fn logical_document_bytes() -> u64 {
    let state = state();
    let document_bytes = state.document.as_ref().map_or(0, |document| {
        document.data.capacity()
            + document.parsed.rows.capacity() * size_of::<RowMeta>()
            + document.parsed.field_hashes.capacity() * size_of::<u64>()
    });
    (document_bytes + state.result.capacity()) as u64
}
