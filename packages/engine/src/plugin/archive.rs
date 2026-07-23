use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read};

use serde_json::Value as JsonValue;
use zip::CompressionMethod;
use zip::read::{ArchiveOffset, Config, ZipArchive};

use crate::LixError;
use crate::binary_cas::BlobHash;
use crate::schema::{schema_key_from_definition, validate_lix_schema_definition};

#[cfg(test)]
use super::InstalledPluginMetadata;
use super::{InstalledPlugin, PluginManifest, parse_plugin_manifest_json};

/// Fully validated plugin package data needed by the install transaction.
///
/// The original ZIP remains the immutable filesystem artifact. This value is
/// the parse-once install view: callers can write schema and registry rows and
/// stage the extracted component in the binary CAS without reopening the ZIP.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ParsedPluginArchive {
    pub manifest: PluginManifest,
    pub normalized_manifest_json: String,
    pub schemas: Vec<JsonValue>,
    pub schema_keys: Vec<String>,
    pub wasm_bytes: Vec<u8>,
    pub wasm_hash: BlobHash,
}

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

#[derive(Debug, Clone, Copy)]
struct PluginArchiveLimits {
    archive_bytes: u64,
    entries: u64,
    entry_bytes: u64,
    expanded_bytes: u64,
    manifest_bytes: u64,
    schema_bytes: u64,
    path_bytes: u64,
}

impl PluginArchiveLimits {
    const DEFAULT: Self = Self {
        archive_bytes: 32 * MIB,
        entries: 128,
        entry_bytes: 32 * MIB,
        expanded_bytes: 64 * MIB,
        manifest_bytes: 64 * KIB,
        schema_bytes: MIB,
        path_bytes: 512,
    };
}

#[derive(Debug)]
struct LoadedPluginArchive {
    manifest: PluginManifest,
    normalized_manifest_json: String,
    schemas: Vec<JsonValue>,
    schema_keys: Vec<String>,
    wasm: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Copy)]
struct PluginArchiveEntry {
    index: usize,
    declared_size: u64,
}

#[derive(Debug)]
struct BoundedEntryRead {
    bytes: Vec<u8>,
    exceeded_limit: bool,
}

#[derive(Debug, Clone, Copy)]
enum PluginArchiveReadKind {
    Manifest,
    Schema,
    Wasm,
}

impl PluginArchiveReadKind {
    fn limit(self, limits: PluginArchiveLimits) -> u64 {
        match self {
            Self::Manifest => limits.manifest_bytes,
            Self::Schema => limits.schema_bytes,
            Self::Wasm => limits.entry_bytes,
        }
    }

    fn resource_name(self) -> &'static str {
        match self {
            Self::Manifest => "manifest bytes",
            Self::Schema => "schema bytes",
            Self::Wasm => "entry bytes",
        }
    }
}

#[derive(Debug)]
struct BoundedPluginArchive<'a> {
    archive: ZipArchive<Cursor<&'a [u8]>>,
    entries: BTreeMap<String, PluginArchiveEntry>,
    expanded_bytes: u64,
    limits: PluginArchiveLimits,
}

pub(crate) fn parse_plugin_archive_for_install(
    archive_bytes: &[u8],
) -> Result<ParsedPluginArchive, LixError> {
    let loaded = load_plugin_archive(archive_bytes, true, PluginArchiveLimits::DEFAULT)?;
    let wasm_bytes = loaded
        .wasm
        .expect("full plugin archive load should include WASM bytes");
    let wasm_hash = BlobHash::from_content(&wasm_bytes);
    Ok(ParsedPluginArchive {
        manifest: loaded.manifest,
        normalized_manifest_json: loaded.normalized_manifest_json,
        schemas: loaded.schemas,
        schema_keys: loaded.schema_keys,
        wasm_bytes,
        wasm_hash,
    })
}

pub(crate) fn load_installed_plugin_from_archive_bytes(
    plugin_key: &str,
    archive_path: &str,
    archive_bytes: &[u8],
) -> Result<InstalledPlugin, LixError> {
    let loaded = load_plugin_archive(archive_bytes, true, PluginArchiveLimits::DEFAULT)?;
    if loaded.manifest.key != plugin_key {
        return Err(invalid_plugin(format!(
            "plugin materialization: archive '{archive_path}' key mismatch: file id key '{plugin_key}' vs manifest key '{}'",
            loaded.manifest.key
        )));
    }
    let wasm = loaded
        .wasm
        .expect("full plugin archive load should include WASM bytes");
    let wasm_hash = BlobHash::from_content(&wasm);

    Ok(InstalledPlugin {
        key: loaded.manifest.key,
        runtime: loaded.manifest.runtime,
        api_version: loaded.manifest.api_version,
        path_glob: loaded.manifest.file_match.path_glob,
        content_type: loaded.manifest.file_match.content_type,
        entry: loaded.manifest.entry,
        schema_keys: loaded.schema_keys,
        manifest_json: loaded.normalized_manifest_json,
        wasm_hash,
        wasm,
    })
}

#[cfg(test)]
pub(crate) fn load_installed_plugin_metadata_from_archive_bytes(
    plugin_key: &str,
    archive_path: &str,
    archive_blob_hash: &str,
    archive_bytes: &[u8],
) -> Result<InstalledPluginMetadata, LixError> {
    let loaded = load_plugin_archive(archive_bytes, false, PluginArchiveLimits::DEFAULT)?;
    if loaded.manifest.key != plugin_key {
        return Err(invalid_plugin(format!(
            "plugin metadata discovery: archive '{archive_path}' key mismatch: file id key '{plugin_key}' vs manifest key '{}'",
            loaded.manifest.key
        )));
    }

    Ok(InstalledPluginMetadata {
        key: loaded.manifest.key,
        archive_path: archive_path.to_string(),
        archive_blob_hash: archive_blob_hash.to_string(),
        path_glob: loaded.manifest.file_match.path_glob,
        content_type: loaded.manifest.file_match.content_type,
        schema_keys: loaded.schema_keys,
    })
}

fn load_plugin_archive(
    archive_bytes: &[u8],
    include_wasm: bool,
    limits: PluginArchiveLimits,
) -> Result<LoadedPluginArchive, LixError> {
    let mut archive = BoundedPluginArchive::open(archive_bytes, limits)?;
    let manifest_bytes = archive.read_file("manifest.json", PluginArchiveReadKind::Manifest)?;
    let manifest_raw = std::str::from_utf8(&manifest_bytes).map_err(|error| {
        invalid_plugin(format!(
            "Plugin archive manifest.json must be UTF-8: {error}"
        ))
    })?;
    let validated_manifest = parse_plugin_manifest_json(manifest_raw)?;

    let entry_path = parse_plugin_archive_path_with_limit(
        &validated_manifest.manifest.entry,
        "Plugin manifest entry",
        limits.path_bytes,
    )?;
    archive.require_file(&entry_path, PluginArchiveReadKind::Wasm)?;
    let wasm = if include_wasm {
        let wasm = archive.read_file(&entry_path, PluginArchiveReadKind::Wasm)?;
        ensure_valid_plugin_wasm(&wasm)?;
        Some(wasm)
    } else {
        // Reserved plugin paths can only be written through the full install
        // validator. Metadata discovery therefore checks package structure and
        // referenced files without paying to inflate an already-validated WASM.
        None
    };

    let mut schemas = Vec::with_capacity(validated_manifest.manifest.schemas.len());
    let mut schema_keys = Vec::with_capacity(validated_manifest.manifest.schemas.len());
    let mut seen_schema_keys = BTreeSet::<String>::new();
    for schema_path in &validated_manifest.manifest.schemas {
        let schema_entry_path = parse_plugin_archive_path_with_limit(
            schema_path,
            "Plugin manifest schema",
            limits.path_bytes,
        )?;
        let schema_bytes = archive.read_file(&schema_entry_path, PluginArchiveReadKind::Schema)?;
        let schema_json: JsonValue = serde_json::from_slice(&schema_bytes).map_err(|error| {
            LixError::new(
                LixError::CODE_SCHEMA_DEFINITION,
                format!("Plugin archive schema '{schema_path}' is invalid JSON: {error}"),
            )
        })?;
        validate_lix_schema_definition(&schema_json)?;
        let schema_key = schema_key_from_definition(&schema_json)?.schema_key;
        if !seen_schema_keys.insert(schema_key.clone()) {
            return Err(invalid_plugin(format!(
                "Plugin archive declares duplicate schema '{schema_key}'"
            )));
        }
        schema_keys.push(schema_key);
        schemas.push(schema_json);
    }

    Ok(LoadedPluginArchive {
        manifest: validated_manifest.manifest,
        normalized_manifest_json: validated_manifest.normalized_json,
        schemas,
        schema_keys,
        wasm,
    })
}

impl<'a> BoundedPluginArchive<'a> {
    fn open(archive_bytes: &'a [u8], limits: PluginArchiveLimits) -> Result<Self, LixError> {
        let declared_entry_count = declared_zip_entry_count(archive_bytes, limits)?;
        let config = Config {
            archive_offset: ArchiveOffset::Known(0),
        };
        let mut archive =
            ZipArchive::with_config(config, Cursor::new(archive_bytes)).map_err(|error| {
                invalid_plugin(format!("Plugin archive is not a valid ZIP file: {error}"))
            })?;
        if usize_to_u64(archive.len()) != declared_entry_count {
            return Err(invalid_plugin(format!(
                "Plugin archive declares {declared_entry_count} entries but contains {} unique entries",
                archive.len()
            )));
        }

        let mut entries = BTreeMap::new();
        let mut logical_paths = BTreeSet::new();
        let mut expanded_bytes = 0u64;
        for index in 0..archive.len() {
            let (path, is_dir, declared_size) = {
                let entry = archive.by_index_raw(index).map_err(|error| {
                    invalid_plugin(format!(
                        "Plugin archive entry at index {index} could not be opened: {error}"
                    ))
                })?;
                let raw_path = std::str::from_utf8(entry.name_raw()).map_err(|error| {
                    invalid_plugin(format!("Plugin archive entry path must be UTF-8: {error}"))
                })?;
                let path_bytes = usize_to_u64(entry.name_raw().len());
                if path_bytes > limits.path_bytes {
                    return Err(plugin_limit_error(
                        "entry path bytes",
                        path_bytes,
                        limits.path_bytes,
                        Some(raw_path),
                    ));
                }
                let is_dir = entry.is_dir();
                let logical_path = if is_dir {
                    raw_path.strip_suffix('/').unwrap_or(raw_path)
                } else {
                    raw_path
                };
                let path = parse_plugin_archive_path_with_limit(
                    logical_path,
                    "Plugin archive entry",
                    limits.path_bytes,
                )?;
                if entry.encrypted() {
                    return Err(invalid_plugin(format!(
                        "Plugin archive entry '{path}' must not be encrypted"
                    )));
                }
                if entry.is_symlink() || is_symlink_mode(entry.unix_mode()) {
                    return Err(invalid_plugin(format!(
                        "Plugin archive entry '{path}' must not be a symlink"
                    )));
                }
                if !matches!(
                    entry.compression(),
                    CompressionMethod::Stored | CompressionMethod::Deflated
                ) {
                    return Err(invalid_plugin(format!(
                        "Plugin archive entry '{path}' uses unsupported compression {:?}",
                        entry.compression()
                    )));
                }
                (path, is_dir, entry.size())
            };

            if !logical_paths.insert(path.clone()) {
                return Err(invalid_plugin(format!(
                    "Plugin archive contains duplicate entry '{path}'"
                )));
            }
            if declared_size > limits.entry_bytes {
                return Err(plugin_limit_error(
                    "entry bytes",
                    declared_size,
                    limits.entry_bytes,
                    Some(&path),
                ));
            }
            expanded_bytes = expanded_bytes
                .checked_add(declared_size)
                .ok_or_else(|| invalid_plugin("Plugin archive expanded byte count overflowed"))?;
            if expanded_bytes > limits.expanded_bytes {
                return Err(plugin_limit_error(
                    "total expanded bytes",
                    expanded_bytes,
                    limits.expanded_bytes,
                    Some(&path),
                ));
            }
            if path == "manifest.json" && declared_size > limits.manifest_bytes {
                return Err(plugin_limit_error(
                    "manifest bytes",
                    declared_size,
                    limits.manifest_bytes,
                    Some(&path),
                ));
            }

            if !is_dir {
                entries.insert(
                    path,
                    PluginArchiveEntry {
                        index,
                        declared_size,
                    },
                );
            }
        }

        Ok(Self {
            archive,
            entries,
            expanded_bytes: 0,
            limits,
        })
    }

    fn require_file(
        &self,
        path: &str,
        kind: PluginArchiveReadKind,
    ) -> Result<PluginArchiveEntry, LixError> {
        let entry = self.entries.get(path).copied().ok_or_else(|| {
            invalid_plugin(format!("Plugin archive is missing declared file '{path}'"))
        })?;
        let limit = kind.limit(self.limits);
        if entry.declared_size > limit {
            return Err(plugin_limit_error(
                kind.resource_name(),
                entry.declared_size,
                limit,
                Some(path),
            ));
        }
        Ok(entry)
    }

    fn read_file(&mut self, path: &str, kind: PluginArchiveReadKind) -> Result<Vec<u8>, LixError> {
        let metadata = self.require_file(path, kind)?;
        let remaining_total = self
            .limits
            .expanded_bytes
            .saturating_sub(self.expanded_bytes);
        let role_limit = kind.limit(self.limits);
        let (read_limit, resource_name, resource_limit) = if remaining_total < role_limit {
            (
                remaining_total,
                "total expanded bytes",
                self.limits.expanded_bytes,
            )
        } else {
            (role_limit, kind.resource_name(), role_limit)
        };

        let mut entry = self.archive.by_index(metadata.index).map_err(|error| {
            invalid_plugin(format!(
                "Plugin archive entry '{path}' could not be decoded: {error}"
            ))
        })?;
        let bounded_read = read_entry_with_limit(&mut entry, read_limit).map_err(|error| {
            invalid_plugin(format!(
                "Plugin archive entry '{path}' could not be read: {error}"
            ))
        })?;
        if bounded_read.exceeded_limit {
            let actual = read_limit.saturating_add(1);
            let aggregate_actual = self.expanded_bytes.saturating_add(actual);
            let reported_actual = if resource_name == "total expanded bytes" {
                aggregate_actual
            } else {
                actual
            };
            return Err(plugin_limit_error(
                resource_name,
                reported_actual,
                resource_limit,
                Some(path),
            ));
        }
        let bytes = bounded_read.bytes;
        let actual = usize_to_u64(bytes.len());
        if actual != metadata.declared_size {
            return Err(invalid_plugin(format!(
                "Plugin archive entry '{path}' expanded to {actual} bytes but declared {} bytes",
                metadata.declared_size
            )));
        }
        self.expanded_bytes = self
            .expanded_bytes
            .checked_add(actual)
            .ok_or_else(|| invalid_plugin("Plugin archive expanded byte count overflowed"))?;
        Ok(bytes)
    }
}

fn read_entry_with_limit(
    entry: &mut impl Read,
    limit: u64,
) -> Result<BoundedEntryRead, std::io::Error> {
    let capacity = usize::try_from(limit.min(64 * KIB)).unwrap_or(64 * 1024);
    let mut output = Vec::with_capacity(capacity);
    let mut chunk = [0u8; 16 * 1024];
    loop {
        let output_len = usize_to_u64(output.len());
        if output_len >= limit {
            break;
        }
        let remaining = limit - output_len;
        let read_len = usize::try_from(remaining.min(usize_to_u64(chunk.len())))
            .expect("bounded plugin archive read length should fit usize");
        let count = entry.read(&mut chunk[..read_len])?;
        if count == 0 {
            return Ok(BoundedEntryRead {
                bytes: output,
                exceeded_limit: false,
            });
        }
        output.extend_from_slice(&chunk[..count]);
    }

    let mut probe = [0u8; 1];
    let exceeded_limit = entry.read(&mut probe)? != 0;
    Ok(BoundedEntryRead {
        bytes: output,
        exceeded_limit,
    })
}

fn declared_zip_entry_count(
    archive_bytes: &[u8],
    limits: PluginArchiveLimits,
) -> Result<u64, LixError> {
    const EOCD_LEN: usize = 22;
    const MAX_EOCD_CANDIDATES: u64 = 8;
    const EOCD_SIGNATURE: &[u8; 4] = b"PK\x05\x06";
    const CENTRAL_SIGNATURE: &[u8; 4] = b"PK\x01\x02";
    const ZIP64_LOCATOR_SIGNATURE: &[u8; 4] = b"PK\x06\x07";

    let archive_len = usize_to_u64(archive_bytes.len());
    if archive_len > limits.archive_bytes {
        return Err(plugin_limit_error(
            "archive bytes",
            archive_len,
            limits.archive_bytes,
            None,
        ));
    }
    if archive_bytes.len() < EOCD_LEN {
        return Err(invalid_plugin("Plugin archive is not a valid ZIP file"));
    }

    let first_offset = archive_bytes
        .len()
        .saturating_sub(EOCD_LEN + usize::from(u16::MAX));
    let eocd_offset = memchr::memmem::rfind_iter(&archive_bytes[first_offset..], EOCD_SIGNATURE)
        .map(|relative_offset| first_offset + relative_offset)
        .find(|offset| {
            let Some(fixed_footer) = archive_bytes.get(*offset..offset.saturating_add(EOCD_LEN))
            else {
                return false;
            };
            let comment_length =
                usize::from(u16::from_le_bytes([fixed_footer[20], fixed_footer[21]]));
            offset
                .checked_add(EOCD_LEN + comment_length)
                .is_some_and(|end| end == archive_bytes.len())
        })
        .ok_or_else(|| invalid_plugin("Plugin archive is not a valid ZIP file"))?;

    // zip-rs allocates from footer counts before returning a ZipArchive and can
    // fall back to an earlier footer. Cap both count fields for every candidate
    // that can reach its central-directory reader, and reject an earlier such
    // candidate. ArchiveOffset::Known(0) below makes the exact central-header
    // check match zip-rs without interpreting incidental magic in payload or
    // comment bytes as a directory record.
    let mut eocd_candidates = 0u64;
    for offset in memchr::memmem::find_iter(archive_bytes, EOCD_SIGNATURE) {
        let Some(fixed_footer) = archive_bytes.get(offset..offset.saturating_add(EOCD_LEN)) else {
            continue;
        };
        // zip-rs allocates and reads the declared comment before deciding
        // whether this footer can identify a central directory. Bounding all
        // complete candidates therefore bounds fallback work even for malformed
        // comments and unusable directory offsets.
        eocd_candidates = eocd_candidates.saturating_add(1);
        if eocd_candidates > MAX_EOCD_CANDIDATES {
            return Err(invalid_plugin(format!(
                "Plugin archive contains more than {MAX_EOCD_CANDIDATES} ZIP footer candidates"
            )));
        }
        let comment_length = usize::from(u16::from_le_bytes([fixed_footer[20], fixed_footer[21]]));
        if offset
            .checked_add(EOCD_LEN + comment_length)
            .is_none_or(|end| end > archive_bytes.len())
        {
            continue;
        }
        let entries_on_disk = u64::from(u16::from_le_bytes([fixed_footer[8], fixed_footer[9]]));
        let total_entries = u64::from(u16::from_le_bytes([fixed_footer[10], fixed_footer[11]]));
        let central_size = u32::from_le_bytes([
            fixed_footer[12],
            fixed_footer[13],
            fixed_footer[14],
            fixed_footer[15],
        ]);
        let central_offset_raw = u32::from_le_bytes([
            fixed_footer[16],
            fixed_footer[17],
            fixed_footer[18],
            fixed_footer[19],
        ]);
        let has_zip64_locator = offset >= 20
            && archive_bytes.get(offset - 20..offset - 16) == Some(ZIP64_LOCATOR_SIGNATURE);
        let may_be_zip64 = total_entries == u64::from(u16::MAX)
            || central_size == u32::MAX
            || central_offset_raw == u32::MAX;

        let central_offset = usize::try_from(central_offset_raw).unwrap_or(usize::MAX);
        let points_to_central_directory = total_entries != 0
            && central_offset < offset
            && archive_bytes.get(central_offset..central_offset.saturating_add(4))
                == Some(CENTRAL_SIGNATURE);
        let can_reach_central_reader = offset == eocd_offset
            || (may_be_zip64 && has_zip64_locator)
            || (total_entries == 0 && entries_on_disk != 0)
            || points_to_central_directory;
        if !can_reach_central_reader {
            continue;
        }

        if may_be_zip64 && has_zip64_locator {
            return Err(invalid_plugin(
                "Plugin archives with ZIP64 central directories are unsupported",
            ));
        }
        if entries_on_disk > limits.entries {
            return Err(plugin_limit_error(
                "archive entries",
                entries_on_disk,
                limits.entries,
                None,
            ));
        }
        if total_entries > limits.entries {
            return Err(plugin_limit_error(
                "archive entries",
                total_entries,
                limits.entries,
                None,
            ));
        }
        if offset != eocd_offset {
            return Err(invalid_plugin(
                "Plugin archive contains multiple parseable ZIP footers",
            ));
        }
    }

    let field = |relative_offset: usize| {
        u16::from_le_bytes([
            archive_bytes[eocd_offset + relative_offset],
            archive_bytes[eocd_offset + relative_offset + 1],
        ])
    };
    let disk = field(4);
    let directory_disk = field(6);
    let entries_on_disk = field(8);
    let entry_count = field(10);
    if disk != 0 || directory_disk != 0 || entries_on_disk != entry_count {
        return Err(invalid_plugin(
            "Plugin archive must be a single-disk ZIP file",
        ));
    }

    let entry_count = u64::from(entry_count);
    if entry_count == 0 {
        return Err(invalid_plugin(
            "Plugin archive must contain at least one entry",
        ));
    }
    if entry_count > limits.entries {
        return Err(plugin_limit_error(
            "archive entries",
            entry_count,
            limits.entries,
            None,
        ));
    }
    let central_offset = usize::try_from(u32::from_le_bytes([
        archive_bytes[eocd_offset + 16],
        archive_bytes[eocd_offset + 17],
        archive_bytes[eocd_offset + 18],
        archive_bytes[eocd_offset + 19],
    ]))
    .unwrap_or(usize::MAX);
    if archive_bytes.get(central_offset..central_offset.saturating_add(4))
        != Some(CENTRAL_SIGNATURE)
    {
        return Err(invalid_plugin(
            "Plugin archive central directory offset is invalid",
        ));
    }
    let central_size = usize::try_from(u32::from_le_bytes([
        archive_bytes[eocd_offset + 12],
        archive_bytes[eocd_offset + 13],
        archive_bytes[eocd_offset + 14],
        archive_bytes[eocd_offset + 15],
    ]))
    .map_err(|_| invalid_plugin("Plugin archive central directory size is invalid"))?;
    let observed_entries = count_central_directory_entries(
        archive_bytes,
        central_offset,
        central_size,
        eocd_offset,
        limits.entries,
    )?;
    if observed_entries != entry_count {
        return Err(invalid_plugin(format!(
            "Plugin archive footer declares {entry_count} entries but its central directory contains {observed_entries}"
        )));
    }
    Ok(observed_entries)
}

fn count_central_directory_entries(
    archive_bytes: &[u8],
    central_offset: usize,
    central_size: usize,
    eocd_offset: usize,
    entry_limit: u64,
) -> Result<u64, LixError> {
    const CENTRAL_HEADER_LEN: usize = 46;
    const CENTRAL_SIGNATURE: &[u8; 4] = b"PK\x01\x02";

    let central_end = central_offset
        .checked_add(central_size)
        .ok_or_else(|| invalid_plugin("Plugin archive central directory range overflowed"))?;
    if central_end != eocd_offset {
        return Err(invalid_plugin(
            "Plugin archive central directory range is invalid",
        ));
    }

    let mut cursor = central_offset;
    let mut entry_count = 0u64;
    while cursor < central_end {
        let fixed_header = archive_bytes
            .get(cursor..cursor.saturating_add(CENTRAL_HEADER_LEN))
            .ok_or_else(|| invalid_plugin("Plugin archive central directory is truncated"))?;
        if fixed_header.get(..4) != Some(CENTRAL_SIGNATURE) {
            return Err(invalid_plugin(
                "Plugin archive central directory contains an invalid entry header",
            ));
        }
        entry_count = entry_count.saturating_add(1);
        if entry_count > entry_limit {
            return Err(plugin_limit_error(
                "archive entries",
                entry_count,
                entry_limit,
                None,
            ));
        }
        let field = |offset: usize| {
            usize::from(u16::from_le_bytes([
                fixed_header[offset],
                fixed_header[offset + 1],
            ]))
        };
        let variable_size = field(28)
            .checked_add(field(30))
            .and_then(|size| size.checked_add(field(32)))
            .ok_or_else(|| invalid_plugin("Plugin archive entry range overflowed"))?;
        cursor = cursor
            .checked_add(CENTRAL_HEADER_LEN)
            .and_then(|value| value.checked_add(variable_size))
            .ok_or_else(|| invalid_plugin("Plugin archive entry range overflowed"))?;
        if cursor > central_end {
            return Err(invalid_plugin(
                "Plugin archive central directory entry is truncated",
            ));
        }
    }
    Ok(entry_count)
}

fn invalid_plugin(message: impl Into<String>) -> LixError {
    LixError::new(LixError::CODE_INVALID_PLUGIN, message)
}

fn plugin_limit_error(resource: &str, actual: u64, limit: u64, path: Option<&str>) -> LixError {
    let path = path.map_or_else(String::new, |path| format!(" for entry '{path}'"));
    invalid_plugin(format!(
        "Plugin archive {resource}{path} is {actual}, exceeding the maximum {limit}"
    ))
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn parse_plugin_archive_path_with_limit(
    path: &str,
    context: &str,
    max_path_bytes: u64,
) -> Result<String, LixError> {
    let path_bytes = usize_to_u64(path.len());
    if path_bytes > max_path_bytes {
        return Err(plugin_limit_error(
            "entry path bytes",
            path_bytes,
            max_path_bytes,
            Some(path),
        ));
    }
    if path.is_empty() {
        return Err(invalid_plugin(format!("{context} path must not be empty")));
    }
    if path.starts_with('/') || path.starts_with('\\') {
        return Err(invalid_plugin(format!(
            "{context} path '{path}' must be relative"
        )));
    }
    if path.contains('\\') {
        return Err(invalid_plugin(format!(
            "{context} path '{path}' must use forward slash separators"
        )));
    }
    if path.contains('\0') {
        return Err(invalid_plugin(format!(
            "{context} path must not contain NUL bytes"
        )));
    }

    for segment in path.split('/') {
        if segment.is_empty() {
            return Err(invalid_plugin(format!(
                "{context} path '{path}' is invalid"
            )));
        }
        if matches!(segment, "." | "..") {
            return Err(invalid_plugin(format!(
                "{context} path '{path}' must not contain traversal or dot components"
            )));
        }
    }

    Ok(path.to_string())
}

fn ensure_valid_plugin_wasm(bytes: &[u8]) -> Result<(), LixError> {
    const WASM_MAGIC: &[u8; 4] = b"\0asm";
    const WASM_HEADER_LEN: usize = 8;
    if bytes.len() < WASM_HEADER_LEN || !bytes.starts_with(WASM_MAGIC) {
        return Err(invalid_plugin(
            "Plugin archive entry file must start with a valid WebAssembly header",
        ));
    }

    Ok(())
}

fn is_symlink_mode(mode: Option<u32>) -> bool {
    const MODE_FILE_TYPE_MASK: u32 = 0o170_000;
    const MODE_SYMLINK: u32 = 0o120_000;
    mode.is_some_and(|value| (value & MODE_FILE_TYPE_MASK) == MODE_SYMLINK)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use zip::write::SimpleFileOptions;
    use zip::{CompressionMethod, ZipWriter};

    use crate::LixError;
    use crate::binary_cas::BlobHash;

    use super::{
        BoundedPluginArchive, PluginArchiveLimits, PluginArchiveReadKind, declared_zip_entry_count,
        load_installed_plugin_from_archive_bytes,
        load_installed_plugin_metadata_from_archive_bytes, load_plugin_archive,
        parse_plugin_archive_for_install, parse_plugin_archive_path_with_limit,
        read_entry_with_limit,
    };

    const MANIFEST: &[u8] = br#"{
        "key":"plugin_test",
        "runtime":"wasm-component-v1",
        "api_version":"0.1.0",
        "match":{"path_glob":"*.test"},
        "entry":"plugin.wasm",
        "schemas":["schema/plugin_test_note.json"]
    }"#;
    const SCHEMA: &[u8] = br#"{
        "x-lix-key":"plugin_test_note",
        "x-lix-primary-key":["/id"],
        "type":"object",
        "properties":{"id":{"type":"string"}},
        "required":["id"],
        "additionalProperties":false
    }"#;
    const WASM: &[u8] = b"\0asm\x01\0\0\0";

    #[test]
    fn archive_path_parsing_is_slash_based() {
        let parse = |path| parse_plugin_archive_path_with_limit(path, "Plugin archive", 512);
        assert_eq!(
            parse("schemas/table.json").as_deref(),
            Ok("schemas/table.json")
        );
        assert!(
            parse("schemas\\table.json")
                .expect_err("backslash must not be accepted as a portable archive separator")
                .message
                .contains("forward slash")
        );
        assert!(
            parse("schemas//table.json")
                .expect_err("empty slash segments must be rejected")
                .message
                .contains("invalid")
        );
        assert!(
            parse("schemas/../table.json")
                .expect_err("archive paths must not traverse")
                .message
                .contains("traversal")
        );
        assert!(
            parse("schemas/./table.json")
                .expect_err("archive paths must not contain dot segments")
                .message
                .contains("dot")
        );
    }

    #[test]
    fn accepts_stored_and_deflated_plugin_archives() {
        for method in [CompressionMethod::Stored, CompressionMethod::Deflated] {
            let archive = plugin_archive(method);
            let parsed = parse_plugin_archive_for_install(&archive)
                .expect("canonical plugin archive should parse");
            assert_eq!(parsed.manifest.key, "plugin_test");
            assert_eq!(parsed.schemas.len(), 1);
            assert_eq!(parsed.schema_keys, ["plugin_test_note"]);
            assert_eq!(parsed.wasm_bytes, WASM);
            assert_eq!(parsed.wasm_hash, BlobHash::from_content(WASM));
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&parsed.normalized_manifest_json)
                    .expect("normalized manifest should remain JSON")["key"],
                "plugin_test"
            );
            let installed = load_installed_plugin_from_archive_bytes(
                "plugin_test",
                "/.lix/plugins/plugin_test.lixplugin",
                &archive,
            )
            .expect("canonical plugin archive should materialize");
            assert_eq!(installed.wasm_hash, BlobHash::from_content(WASM));
        }
    }

    #[test]
    fn embedded_schema_errors_keep_the_schema_error_code() {
        let archive = zip_entries(
            &[
                ("manifest.json", MANIFEST),
                ("schema/plugin_test_note.json", b"{"),
                ("plugin.wasm", WASM),
            ],
            CompressionMethod::Stored,
        );
        let error = parse_plugin_archive_for_install(&archive)
            .expect_err("malformed embedded schema JSON must fail");
        assert_eq!(error.code, LixError::CODE_SCHEMA_DEFINITION);
    }

    #[test]
    fn enforces_plugin_archive_limits_at_the_boundary() {
        let archive = plugin_archive(CompressionMethod::Stored);
        let payloads = [MANIFEST, SCHEMA, WASM];
        let paths = [
            "manifest.json",
            "schema/plugin_test_note.json",
            "plugin.wasm",
        ];
        let exact = PluginArchiveLimits {
            archive_bytes: to_u64(archive.len()),
            entries: to_u64(paths.len()),
            entry_bytes: payloads
                .iter()
                .map(|payload| to_u64(payload.len()))
                .max()
                .expect("plugin archive has entries"),
            expanded_bytes: payloads.iter().map(|payload| to_u64(payload.len())).sum(),
            manifest_bytes: to_u64(MANIFEST.len()),
            schema_bytes: to_u64(SCHEMA.len()),
            path_bytes: paths
                .iter()
                .map(|path| to_u64(path.len()))
                .max()
                .expect("plugin archive has paths"),
        };
        load_plugin_archive(&archive, true, exact)
            .expect("every exact plugin archive bound should be inclusive");

        let cases = [
            (
                PluginArchiveLimits {
                    archive_bytes: exact.archive_bytes - 1,
                    ..exact
                },
                "archive bytes",
            ),
            (
                PluginArchiveLimits {
                    entries: exact.entries - 1,
                    ..exact
                },
                "archive entries",
            ),
            (
                PluginArchiveLimits {
                    entry_bytes: exact.entry_bytes - 1,
                    ..exact
                },
                "entry bytes",
            ),
            (
                PluginArchiveLimits {
                    expanded_bytes: exact.expanded_bytes - 1,
                    ..exact
                },
                "total expanded bytes",
            ),
            (
                PluginArchiveLimits {
                    manifest_bytes: exact.manifest_bytes - 1,
                    ..exact
                },
                "manifest bytes",
            ),
            (
                PluginArchiveLimits {
                    schema_bytes: exact.schema_bytes - 1,
                    ..exact
                },
                "schema bytes",
            ),
            (
                PluginArchiveLimits {
                    path_bytes: exact.path_bytes - 1,
                    ..exact
                },
                "entry path bytes",
            ),
        ];
        for (limits, expected_resource) in cases {
            assert_invalid_limit(&archive, limits, expected_resource);
        }
    }

    #[test]
    fn entry_count_guard_does_not_trust_a_forged_footer_count() {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        for index in 0..129 {
            writer
                .start_file(format!("entry-{index}"), options)
                .expect("entry-count fixture should start");
        }
        let mut archive = writer
            .finish()
            .expect("entry-count fixture should finish")
            .into_inner();
        let eocd_offset = archive.len() - 22;
        archive[eocd_offset + 8..eocd_offset + 12].copy_from_slice(&[1, 0, 1, 0]);

        let error = declared_zip_entry_count(&archive, PluginArchiveLimits::DEFAULT)
            .expect_err("central headers must enforce the cap before zip-rs parses the footer");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(error.message.contains("archive entries"), "{error:?}");

        let mut forged_disk_count = zip_entries(&[("entry", b"")], CompressionMethod::Stored);
        let eocd_offset = forged_disk_count.len() - 22;
        forged_disk_count[eocd_offset + 8..eocd_offset + 10].copy_from_slice(&129u16.to_le_bytes());
        let error = declared_zip_entry_count(&forged_disk_count, PluginArchiveLimits::DEFAULT)
            .expect_err("the on-disk footer count must be capped before zip-rs parses it");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(error.message.contains("archive entries"), "{error:?}");
    }

    #[test]
    fn accepts_bounded_standard_zip_variants() {
        let mut comment_writer = ZipWriter::new(Cursor::new(Vec::new()));
        comment_writer
            .set_comment("comment")
            .expect("ZIP comment should be accepted by the fixture writer");
        comment_writer
            .start_file("entry", SimpleFileOptions::default())
            .expect("comment fixture entry should start");
        comment_writer
            .write_all(b"data")
            .expect("comment fixture entry should write");
        let comment = comment_writer
            .finish()
            .expect("comment archive should finish")
            .into_inner();

        let mut zip64_writer = ZipWriter::new(Cursor::new(Vec::new()));
        zip64_writer
            .start_file("entry", SimpleFileOptions::default().large_file(true))
            .expect("ZIP64 fixture entry should start");
        zip64_writer
            .write_all(b"data")
            .expect("ZIP64 fixture entry should write");
        let zip64 = zip64_writer
            .finish()
            .expect("ZIP64 archive should finish")
            .into_inner();

        let mut stream_writer = ZipWriter::new_stream(Vec::new());
        stream_writer
            .start_file("entry", SimpleFileOptions::default())
            .expect("stream fixture entry should start");
        stream_writer
            .write_all(b"data")
            .expect("stream fixture entry should write");
        let descriptor = stream_writer
            .finish()
            .expect("stream archive should finish")
            .into_inner();

        for (label, archive) in [
            ("comment", comment),
            ("small ZIP64", zip64),
            ("data descriptor", descriptor),
        ] {
            BoundedPluginArchive::open(&archive, PluginArchiveLimits::DEFAULT)
                .unwrap_or_else(|error| panic!("{label} archive should be accepted: {error:?}"));
        }
    }

    #[test]
    fn ignores_incidental_eocd_magic_in_entry_data_and_comments() {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        writer
            .set_comment("comment ending in PK\u{5}\u{6}")
            .expect("ZIP comment should be accepted by the fixture writer");
        writer
            .start_file("entry", SimpleFileOptions::default())
            .expect("incidental-magic fixture entry should start");
        writer
            .write_all(b"payload PK\x05\x06\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0")
            .expect("incidental-magic fixture entry should write");
        let archive = writer
            .finish()
            .expect("incidental-magic archive should finish")
            .into_inner();

        BoundedPluginArchive::open(&archive, PluginArchiveLimits::DEFAULT)
            .expect("incidental EOCD magic must not be treated as a ZIP footer");
    }

    #[test]
    fn bounds_zip_footer_fallback_candidates() {
        const FAKE_EOCD: &[u8] = b"PK\x05\x06\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";

        let accepted_payload = FAKE_EOCD.repeat(7);
        let accepted = zip_entries(
            &[("entry", accepted_payload.as_slice())],
            CompressionMethod::Stored,
        );
        BoundedPluginArchive::open(&accepted, PluginArchiveLimits::DEFAULT)
            .expect("seven incidental candidates plus the real footer should fit the bound");

        let rejected_payload = FAKE_EOCD.repeat(8);
        let rejected = zip_entries(
            &[("entry", rejected_payload.as_slice())],
            CompressionMethod::Stored,
        );
        let error = BoundedPluginArchive::open(&rejected, PluginArchiveLimits::DEFAULT)
            .expect_err("eight incidental candidates plus the real footer must exceed the bound");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(error.message.contains("ZIP footer candidates"), "{error:?}");
    }

    #[test]
    fn rejects_unsafe_archive_entries() {
        let mut duplicate = zip_entries(
            &[("entry-a", b"a"), ("entry-b", b"b")],
            CompressionMethod::Stored,
        );
        assert_eq!(
            replace_all(&mut duplicate, b"entry-b", b"entry-a"),
            2,
            "entry name should occur in its local and central headers"
        );

        let mut symlink_writer = ZipWriter::new(Cursor::new(Vec::new()));
        symlink_writer
            .add_symlink("link", "target", SimpleFileOptions::default())
            .expect("symlink entry should write");
        let symlink = symlink_writer
            .finish()
            .expect("symlink archive should finish")
            .into_inner();

        let traversal = zip_entries(&[("../entry", b"data")], CompressionMethod::Stored);
        for (label, archive, expected_message) in [
            ("duplicate", duplicate, "unique entries"),
            ("symlink", symlink, "symlink"),
            ("traversal", traversal, "traversal"),
        ] {
            let error = BoundedPluginArchive::open(&archive, PluginArchiveLimits::DEFAULT)
                .expect_err("unsafe ZIP fixture must be rejected");
            assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN, "{label}");
            assert!(
                error.message.contains(expected_message),
                "{label}: {}",
                error.message
            );
        }
    }

    #[test]
    fn exact_limit_reads_still_validate_crc() {
        let mut archive = zip_entries(&[("plugin.wasm", WASM)], CompressionMethod::Stored);
        corrupt_first_entry_crc(&mut archive);
        let limits = PluginArchiveLimits {
            entry_bytes: to_u64(WASM.len()),
            expanded_bytes: to_u64(WASM.len()),
            ..PluginArchiveLimits::DEFAULT
        };
        let mut bounded = BoundedPluginArchive::open(&archive, limits)
            .expect("header-consistent CRC fixture should pass preflight");
        let error = bounded
            .read_file("plugin.wasm", PluginArchiveReadKind::Wasm)
            .expect_err("an exact-limit read must continue through CRC validation");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(
            error.message.to_ascii_lowercase().contains("checksum"),
            "{error:?}"
        );
    }

    #[test]
    fn bounded_reader_stops_after_the_limit_probe() {
        let mut input = Cursor::new(vec![7u8; 64]);
        let output = read_entry_with_limit(&mut input, 8).expect("bounded read should succeed");
        assert_eq!(output.bytes.len(), 8);
        assert!(output.exceeded_limit);
        assert_eq!(input.position(), 9);
    }

    #[test]
    fn metadata_loading_does_not_inflate_wasm() {
        let mut archive = plugin_archive(CompressionMethod::Stored);
        assert_eq!(
            replace_all(&mut archive, WASM, b"\0asm\x02\0\0\0"),
            1,
            "WASM payload should occur exactly once"
        );

        let metadata = load_installed_plugin_metadata_from_archive_bytes(
            "plugin_test",
            "/.lix/plugins/plugin_test.lixplugin",
            "test-blob",
            &archive,
        )
        .expect("metadata loading should not decode the WASM entry");
        assert_eq!(metadata.key, "plugin_test");

        let error = load_installed_plugin_from_archive_bytes(
            "plugin_test",
            "/.lix/plugins/plugin_test.lixplugin",
            &archive,
        )
        .expect_err("materialization must validate the WASM entry CRC");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(
            error.message.to_ascii_lowercase().contains("checksum"),
            "{error:?}"
        );
    }

    fn plugin_archive(method: CompressionMethod) -> Vec<u8> {
        zip_entries(
            &[
                ("manifest.json", MANIFEST),
                ("schema/plugin_test_note.json", SCHEMA),
                ("plugin.wasm", WASM),
            ],
            method,
        )
    }

    fn zip_entries(entries: &[(&str, &[u8])], method: CompressionMethod) -> Vec<u8> {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default().compression_method(method);
        for (path, bytes) in entries {
            writer
                .start_file(*path, options)
                .expect("ZIP fixture entry should start");
            writer
                .write_all(bytes)
                .expect("ZIP fixture entry should write");
        }
        writer
            .finish()
            .expect("ZIP fixture should finish")
            .into_inner()
    }

    fn assert_invalid_limit(archive: &[u8], limits: PluginArchiveLimits, expected_resource: &str) {
        let error = load_plugin_archive(archive, true, limits)
            .expect_err("a bound lowered by one must reject the archive");
        assert_eq!(error.code, LixError::CODE_INVALID_PLUGIN);
        assert!(
            error.message.contains(expected_resource),
            "expected {expected_resource:?} in {:?}",
            error.message
        );
    }

    fn replace_all(bytes: &mut [u8], from: &[u8], to: &[u8]) -> usize {
        assert_eq!(from.len(), to.len());
        let mut replacements = 0;
        let mut cursor = 0;
        while cursor + from.len() <= bytes.len() {
            if &bytes[cursor..cursor + from.len()] == from {
                bytes[cursor..cursor + to.len()].copy_from_slice(to);
                replacements += 1;
                cursor += from.len();
            } else {
                cursor += 1;
            }
        }
        replacements
    }

    fn corrupt_first_entry_crc(archive: &mut [u8]) {
        let central_offset = first_central_offset(archive);
        let local_offset_bytes: [u8; 4] = archive[central_offset + 42..central_offset + 46]
            .try_into()
            .expect("central entry local offset should be four bytes");
        let local_offset = usize::try_from(u32::from_le_bytes(local_offset_bytes))
            .expect("fixture local offset should fit usize");
        for offset in [local_offset + 14, central_offset + 16] {
            archive[offset] ^= 1;
        }
    }

    fn first_central_offset(archive: &[u8]) -> usize {
        let eocd_offset = archive.len() - 22;
        let offset: [u8; 4] = archive[eocd_offset + 16..eocd_offset + 20]
            .try_into()
            .expect("EOCD central offset should be four bytes");
        usize::try_from(u32::from_le_bytes(offset))
            .expect("fixture central offset should fit usize")
    }

    fn to_u64(value: usize) -> u64 {
        u64::try_from(value).expect("fixture size should fit u64")
    }
}

#[cfg(test)]
mod benchmark_probe {
    use std::hint::black_box;
    use std::io::{Cursor, Write};
    use std::time::{Duration, Instant};

    use super::{
        load_installed_plugin_from_archive_bytes,
        load_installed_plugin_metadata_from_archive_bytes, parse_plugin_archive_for_install,
    };

    #[derive(Clone, Copy)]
    enum Operation {
        Install,
        Metadata,
        Materialize,
    }

    #[test]
    #[ignore = "release-only plugin archive parser benchmark probe"]
    fn plugin_archive_parse_benchmark_probe() {
        let operation = match std::env::var("LIX_PLUGIN_ARCHIVE_BENCH_OPERATION")
            .unwrap_or_else(|_| "install".to_string())
            .as_str()
        {
            "install" => Operation::Install,
            "metadata" => Operation::Metadata,
            "materialize" => Operation::Materialize,
            value => panic!(
                "LIX_PLUGIN_ARCHIVE_BENCH_OPERATION must be install, metadata, or materialize, got {value:?}"
            ),
        };
        let wasm_bytes = env_usize("LIX_PLUGIN_ARCHIVE_BENCH_WASM_BYTES", 2 * 1024 * 1024);
        let rounds = env_usize("LIX_PLUGIN_ARCHIVE_BENCH_ROUNDS", 200);
        let warmups = env_usize("LIX_PLUGIN_ARCHIVE_BENCH_WARMUPS", 20);
        assert!(wasm_bytes >= 8, "benchmark WASM must include its header");
        assert!(rounds > 0, "benchmark needs at least one measured round");

        let archive = benchmark_archive(wasm_bytes);
        for _ in 0..warmups {
            run_operation(operation, &archive);
        }

        let mut samples = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            let started = Instant::now();
            run_operation(operation, &archive);
            samples.push(started.elapsed());
        }
        samples.sort_unstable();
        println!(
            "plugin_archive_parse_probe operation={} archive_bytes={} wasm_bytes={} rounds={} p50_us={} p95_us={}",
            operation_name(operation),
            archive.len(),
            wasm_bytes,
            rounds,
            percentile(&samples, 50, 100).as_micros(),
            percentile(&samples, 95, 100).as_micros(),
        );
    }

    fn run_operation(operation: Operation, archive: &[u8]) {
        match operation {
            Operation::Install => {
                black_box(parse_plugin_archive_for_install(black_box(archive)))
                    .expect("benchmark archive should parse");
            }
            Operation::Metadata => {
                black_box(load_installed_plugin_metadata_from_archive_bytes(
                    "plugin_bench",
                    "/.lix/plugins/plugin_bench.lixplugin",
                    "bench-blob",
                    black_box(archive),
                ))
                .expect("benchmark archive metadata should load");
            }
            Operation::Materialize => {
                black_box(load_installed_plugin_from_archive_bytes(
                    "plugin_bench",
                    "/.lix/plugins/plugin_bench.lixplugin",
                    black_box(archive),
                ))
                .expect("benchmark archive should materialize");
            }
        }
    }

    fn operation_name(operation: Operation) -> &'static str {
        match operation {
            Operation::Install => "install",
            Operation::Metadata => "metadata",
            Operation::Materialize => "materialize",
        }
    }

    fn env_usize(name: &str, default: usize) -> usize {
        match std::env::var(name) {
            Ok(value) => value
                .parse()
                .unwrap_or_else(|error| panic!("{name} must be an unsigned integer: {error}")),
            Err(std::env::VarError::NotPresent) => default,
            Err(error) => panic!("{name} must be valid Unicode: {error}"),
        }
    }

    fn percentile(samples: &[Duration], numerator: usize, denominator: usize) -> Duration {
        let rank = samples
            .len()
            .checked_mul(numerator)
            .expect("sample count should fit percentile arithmetic")
            .div_ceil(denominator);
        samples[rank - 1]
    }

    fn benchmark_archive(wasm_bytes: usize) -> Vec<u8> {
        let manifest = br#"{
            "key":"plugin_bench",
            "runtime":"wasm-component-v1",
            "api_version":"0.1.0",
            "match":{"path_glob":"*.bench"},
            "entry":"plugin.wasm",
            "schemas":["schema/plugin_bench_note.json"]
        }"#;
        let schema = br#"{
            "x-lix-key":"plugin_bench_note",
            "x-lix-primary-key":["/id"],
            "type":"object",
            "properties":{"id":{"type":"string"}},
            "required":["id"],
            "additionalProperties":false
        }"#;
        let mut wasm = vec![0u8; wasm_bytes];
        wasm[..8].copy_from_slice(b"\0asm\x01\0\0\0");
        let mut state = 0x9e37_79b9_u32;
        for byte in &mut wasm[8..] {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            *byte = state.to_le_bytes()[0];
        }

        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        for (path, bytes) in [
            ("manifest.json", manifest.as_slice()),
            ("schema/plugin_bench_note.json", schema.as_slice()),
            ("plugin.wasm", wasm.as_slice()),
        ] {
            writer
                .start_file(path, options)
                .expect("benchmark ZIP entry should start");
            writer
                .write_all(bytes)
                .expect("benchmark ZIP entry should write");
        }
        writer
            .finish()
            .expect("benchmark ZIP should finish")
            .into_inner()
    }
}
