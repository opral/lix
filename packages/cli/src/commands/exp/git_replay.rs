use crate::cli::exp::ExpGitReplayArgs;
use crate::db;
use crate::error::CliError;
use lix_rs_sdk::{BootKeyValue, Lix, LixConfig, SqliteBackend, Value, WasmtimeRuntime};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;

const NULL_OID: &str = "0000000000000000000000000000000000000000";
const PROGRESS_EVERY: usize = 10;
const DEFAULT_INSERT_BATCH_ROWS: usize = 100;

#[derive(Debug, Clone)]
struct Change {
    status: char,
    old_mode: String,
    new_mode: String,
    new_oid: String,
    old_path: Option<String>,
    new_path: Option<String>,
}

impl Change {
    fn new_is_blob(&self) -> bool {
        mode_is_blob(&self.new_mode)
    }
}

#[derive(Debug)]
struct PatchSet {
    changes: Vec<Change>,
    blob_by_oid: HashMap<String, Vec<u8>>,
}

#[derive(Default)]
struct ReplayState {
    path_to_file_id: HashMap<String, String>,
    known_file_ids: HashSet<String>,
}

#[derive(Debug, Clone)]
struct WriteRow {
    id: String,
    path: String,
    data: Vec<u8>,
}

#[derive(Debug)]
struct PreparedBatch {
    deletes: Vec<String>,
    inserts: Vec<WriteRow>,
    updates: Vec<WriteRow>,
}

#[derive(Debug)]
struct SqlStatement {
    sql: String,
    params: Vec<Value>,
}

#[derive(Debug, Clone)]
struct ExpectedFile {
    path: String,
    sha256: String,
}

pub fn run(args: ExpGitReplayArgs) -> Result<(), CliError> {
    let repo_path = absolutize_from_cwd(&args.repo_path)?;
    validate_repo_dir(&repo_path)?;
    validate_git_repo(&repo_path)?;
    let output_lix_path = absolutize_from_cwd(&args.output_lix_path)?;
    prepare_output_path(&output_lix_path)?;
    let replay_ref = normalize_replay_ref(&args.branch)?;
    let from_commit = args
        .from_commit
        .as_deref()
        .map(|raw| resolve_commit_oid(&repo_path, raw))
        .transpose()?;
    let commits = list_linear_commits(
        &repo_path,
        &replay_ref,
        from_commit.as_deref(),
        args.num_commits,
    )?;

    if commits.is_empty() {
        return Err(CliError::msg(format!(
            "no commits found in {} for ref '{}'",
            repo_path.display(),
            args.branch
        )));
    }

    let lix = open_lix_at_path(&output_lix_path)?;

    let mut state = ReplayState::default();
    let mut expected_state_by_id = HashMap::<String, ExpectedFile>::new();
    let mut applied = 0usize;
    let mut noop = 0usize;
    let mut changed_paths = 0usize;
    let mut verified = 0usize;

    println!(
        "[git-replay] replaying {} commits from {}",
        commits.len(),
        repo_path.display()
    );

    for (index, commit_sha) in commits.iter().enumerate() {
        let patch_set = read_commit_patch_set(&repo_path, commit_sha)?;
        changed_paths += patch_set.changes.len();

        let prepared =
            prepare_commit_changes(&mut state, &patch_set.changes, &patch_set.blob_by_oid)?;
        let statements = build_replay_commit_statements(&prepared, DEFAULT_INSERT_BATCH_ROWS);

        if statements.is_empty() {
            noop += 1;
        } else {
            execute_statements_as_transaction(&lix, &statements, commit_sha)?;
            applied += 1;
        }

        if args.verify_state {
            apply_prepared_to_expected_state(&mut expected_state_by_id, &prepared);
            verify_commit_state_hashes(&lix, &expected_state_by_id, commit_sha)?;
            verified += 1;
        }

        if index == 0 || (index + 1) % PROGRESS_EVERY == 0 || index + 1 == commits.len() {
            println!(
                "[git-replay] {}/{} commits (applied={}, noop={}, changedPaths={})",
                index + 1,
                commits.len(),
                applied,
                noop,
                changed_paths
            );
        }
    }

    println!("[git-replay] done");
    println!("[git-replay] ref: {}", args.branch);
    println!("[git-replay] output: {}", output_lix_path.display());
    println!("[git-replay] commits replayed: {}", commits.len());
    println!("[git-replay] commits applied: {}", applied);
    println!("[git-replay] commits noop: {}", noop);
    println!("[git-replay] changed paths total: {}", changed_paths);
    if args.verify_state {
        println!(
            "[git-replay] verified commits: {verified}/{}",
            commits.len()
        );
    }

    Ok(())
}

fn open_lix_at_path(path: &Path) -> Result<Lix, CliError> {
    db::init_lix_at(path)?;

    let backend = SqliteBackend::from_path(path).map_err(|err| {
        CliError::msg(format!(
            "failed to open sqlite backend at {}: {}",
            path.display(),
            err
        ))
    })?;

    let config = LixConfig {
        backend: Box::new(backend),
        wasm_runtime: default_wasm_runtime()?,
        key_values: vec![BootKeyValue {
            key: "lix_deterministic_mode".to_string(),
            value: json!({ "enabled": true }),
            version_id: Some("global".to_string()),
            untracked: None,
        }],
    };

    pollster::block_on(Lix::open(config)).map_err(|err| {
        CliError::msg(format!(
            "failed to open lix database at {}: {}",
            path.display(),
            err
        ))
    })
}

fn execute_statements_as_transaction(
    lix: &Lix,
    statements: &[SqlStatement],
    commit_sha: &str,
) -> Result<(), CliError> {
    let script = build_transaction_script(statements);
    let params = statements
        .iter()
        .flat_map(|statement| statement.params.iter().cloned())
        .collect::<Vec<_>>();

    pollster::block_on(lix.execute(&script, &params)).map_err(|error| {
        let sql_preview = script.chars().take(160).collect::<String>();
        CliError::msg(format!(
            "failed at commit {commit_sha} while executing replay SQL '{sql_preview}': {error}"
        ))
    })?;

    Ok(())
}

fn build_transaction_script(statements: &[SqlStatement]) -> String {
    let mut script = String::from("BEGIN;");
    let mut next_param_index = 1usize;

    for statement in statements {
        script.push(' ');
        script.push_str(&number_sql_parameters(
            &statement.sql,
            &mut next_param_index,
        ));
        script.push(';');
    }

    script.push_str(" COMMIT;");
    script
}

fn number_sql_parameters(sql: &str, next_param_index: &mut usize) -> String {
    let mut numbered = String::with_capacity(sql.len() + 16);
    for ch in sql.chars() {
        if ch == '?' {
            numbered.push('?');
            numbered.push_str(&next_param_index.to_string());
            *next_param_index += 1;
        } else {
            numbered.push(ch);
        }
    }
    numbered
}

fn list_linear_commits(
    repo_path: &Path,
    replay_ref: &str,
    from_commit: Option<&str>,
    limit: Option<u32>,
) -> Result<Vec<String>, CliError> {
    let mut args = vec![
        "rev-list".to_string(),
        "--reverse".to_string(),
        "--first-parent".to_string(),
    ];
    if replay_ref == "--all" {
        args.push("--all".to_string());
    } else {
        args.push(replay_ref.to_string());
    }

    let output = run_git_text(repo_path, &args, None)?;
    let commits = output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    select_replay_commits(commits, from_commit, limit)
}

fn select_replay_commits(
    mut commits: Vec<String>,
    from_commit: Option<&str>,
    limit: Option<u32>,
) -> Result<Vec<String>, CliError> {
    if let Some(from_commit) = from_commit {
        let from_index = commits
            .iter()
            .position(|commit| commit == from_commit)
            .ok_or_else(|| {
                CliError::msg(format!(
                    "--from-commit {} is not reachable from selected ref",
                    from_commit
                ))
            })?;
        commits = commits.split_off(from_index);
    }

    if let Some(limit) = limit {
        commits.truncate(limit as usize);
    }

    Ok(commits)
}

fn resolve_commit_oid(repo_path: &Path, raw: &str) -> Result<String, CliError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(CliError::InvalidArgs("from_commit must not be empty"));
    }

    let args = vec![
        "rev-parse".to_string(),
        "--verify".to_string(),
        format!("{trimmed}^{{commit}}"),
    ];
    let output = run_git_text(repo_path, &args, None).map_err(|error| {
        CliError::msg(format!(
            "failed to resolve --from-commit {}: {}",
            raw, error
        ))
    })?;
    let oid = output.trim();
    if oid.is_empty() {
        return Err(CliError::msg(format!(
            "failed to resolve --from-commit {}: empty rev-parse output",
            raw
        )));
    }
    Ok(oid.to_string())
}

fn read_commit_patch_set(repo_path: &Path, commit_sha: &str) -> Result<PatchSet, CliError> {
    let raw_args = vec![
        "diff-tree".to_string(),
        "--root".to_string(),
        "--raw".to_string(),
        "-r".to_string(),
        "-z".to_string(),
        "-m".to_string(),
        "--first-parent".to_string(),
        "--find-renames".to_string(),
        "--no-commit-id".to_string(),
        commit_sha.to_string(),
    ];
    let raw = run_git_bytes(repo_path, &raw_args, None)?;
    let changes = parse_raw_diff_tree(&raw)?;

    let wanted_blob_ids = collect_wanted_blob_ids(&changes);
    let blob_by_oid = read_blobs(repo_path, &wanted_blob_ids)?;
    Ok(PatchSet {
        changes,
        blob_by_oid,
    })
}

fn parse_raw_diff_tree(raw: &[u8]) -> Result<Vec<Change>, CliError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }

    let mut tokens = raw.split(|byte| *byte == 0).collect::<Vec<_>>();
    if tokens.last().is_some_and(|token| token.is_empty()) {
        tokens.pop();
    }

    let mut changes = Vec::new();
    let mut index = 0usize;

    while index < tokens.len() {
        let header_token = tokens[index];
        index += 1;

        if header_token.is_empty() || !header_token.starts_with(b":") {
            continue;
        }

        let header_text = String::from_utf8_lossy(header_token);
        let fields = header_text[1..].split_whitespace().collect::<Vec<_>>();
        if fields.len() < 5 {
            continue;
        }

        let old_mode = fields[0].to_string();
        let new_mode = fields[1].to_string();
        let new_oid = fields[3].to_string();
        let status = fields[4].chars().next().unwrap_or('M');

        let first_path =
            token_to_string(tokens.get(index).ok_or_else(|| {
                CliError::msg("malformed git diff-tree output: missing path token")
            })?);
        index += 1;

        if status == 'R' || status == 'C' {
            let second_path = token_to_string(tokens.get(index).ok_or_else(|| {
                CliError::msg("malformed git diff-tree output: missing rename destination")
            })?);
            index += 1;

            changes.push(Change {
                status,
                old_mode,
                new_mode,
                new_oid,
                old_path: Some(first_path),
                new_path: Some(second_path),
            });
            continue;
        }

        let old_path = if status == 'A' {
            None
        } else {
            Some(first_path.clone())
        };
        let new_path = if status == 'D' {
            None
        } else {
            Some(first_path)
        };

        changes.push(Change {
            status,
            old_mode,
            new_mode,
            new_oid,
            old_path,
            new_path,
        });
    }

    Ok(changes)
}

fn collect_wanted_blob_ids(changes: &[Change]) -> Vec<String> {
    let mut wanted_blob_ids = BTreeSet::<String>::new();
    for change in changes {
        if change.new_path.is_none() || !change.new_is_blob() {
            continue;
        }
        if !change.new_oid.is_empty() && change.new_oid != NULL_OID {
            wanted_blob_ids.insert(change.new_oid.clone());
        }
    }
    wanted_blob_ids.into_iter().collect()
}

fn read_blobs(repo_path: &Path, blob_ids: &[String]) -> Result<HashMap<String, Vec<u8>>, CliError> {
    if blob_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let mut request_body = String::new();
    for blob_id in blob_ids {
        request_body.push_str(blob_id);
        request_body.push('\n');
    }

    let args = vec!["cat-file".to_string(), "--batch".to_string()];
    let stdout = run_git_bytes(repo_path, &args, Some(request_body.as_bytes()))?;
    let mut blobs = HashMap::<String, Vec<u8>>::new();
    let mut offset = 0usize;

    while offset < stdout.len() {
        let line_end = stdout[offset..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|relative| offset + relative)
            .ok_or_else(|| {
                CliError::msg("malformed git cat-file output: missing header newline")
            })?;

        let header = String::from_utf8_lossy(&stdout[offset..line_end])
            .trim()
            .to_string();
        offset = line_end + 1;

        if header.is_empty() {
            continue;
        }

        let fields = header.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 2 {
            return Err(CliError::msg(format!(
                "malformed git cat-file header: {header}"
            )));
        }

        let oid = fields[0];
        let object_type = fields[1];
        if object_type == "missing" {
            return Err(CliError::msg(format!(
                "missing blob object in git repository: {oid}"
            )));
        }

        if fields.len() < 3 {
            return Err(CliError::msg(format!(
                "malformed git cat-file header (missing size): {header}"
            )));
        }

        let size = fields[2].parse::<usize>().map_err(|_| {
            CliError::msg(format!(
                "invalid blob size '{}' in git cat-file output for {oid}",
                fields[2]
            ))
        })?;
        let data_start = offset;
        let data_end = data_start.saturating_add(size);
        if data_end > stdout.len() {
            return Err(CliError::msg(format!(
                "git cat-file output truncated while reading blob {oid}"
            )));
        }

        blobs.insert(oid.to_string(), stdout[data_start..data_end].to_vec());
        offset = data_end;
        if offset < stdout.len() && stdout[offset] == b'\n' {
            offset += 1;
        }
    }

    for blob_id in blob_ids {
        if !blobs.contains_key(blob_id) {
            return Err(CliError::msg(format!(
                "blob {blob_id} was requested but not returned by git cat-file"
            )));
        }
    }

    Ok(blobs)
}

fn prepare_commit_changes(
    state: &mut ReplayState,
    changes: &[Change],
    blob_by_oid: &HashMap<String, Vec<u8>>,
) -> Result<PreparedBatch, CliError> {
    let mut delete_ids = BTreeSet::<String>::new();
    let mut inserts_by_id = BTreeMap::<String, WriteRow>::new();
    let mut updates_by_id = BTreeMap::<String, WriteRow>::new();

    for change in changes {
        let status = normalize_status(change.status);

        if should_delete_old_entry(change, status) {
            if let Some(deleted_id) = resolve_delete_path(state, change) {
                delete_ids.insert(deleted_id.clone());
                inserts_by_id.remove(&deleted_id);
                updates_by_id.remove(&deleted_id);
            }
        }

        if status == 'D' || !change.new_is_blob() {
            continue;
        }

        let new_path = match &change.new_path {
            Some(path) => path,
            None => continue,
        };

        let target = resolve_write_target(state, change, status)?;
        let bytes = blob_by_oid.get(&change.new_oid).ok_or_else(|| {
            CliError::msg(format!(
                "missing blob {} while applying {} {}",
                change.new_oid, status, new_path
            ))
        })?;

        let row = WriteRow {
            id: target.id.clone(),
            path: to_lix_path(new_path),
            data: bytes.clone(),
        };

        if delete_ids.contains(&row.id) {
            delete_ids.remove(&row.id);
        }

        if target.is_insert {
            inserts_by_id.insert(row.id.clone(), row);
            updates_by_id.remove(&target.id);
            state.known_file_ids.insert(target.id);
            continue;
        }

        if inserts_by_id.contains_key(&row.id) {
            inserts_by_id.insert(row.id.clone(), row);
            continue;
        }

        updates_by_id.insert(row.id.clone(), row);
    }

    Ok(PreparedBatch {
        deletes: delete_ids.into_iter().collect(),
        inserts: inserts_by_id.into_values().collect(),
        updates: updates_by_id.into_values().collect(),
    })
}

fn should_delete_old_entry(change: &Change, status: char) -> bool {
    if change.old_path.is_none() || !mode_is_blob(&change.old_mode) {
        return false;
    }

    match status {
        'D' | 'R' => true,
        'A' | 'C' => false,
        _ => !change.new_is_blob(),
    }
}

struct WriteTarget {
    id: String,
    is_insert: bool,
}

fn resolve_delete_path(state: &mut ReplayState, change: &Change) -> Option<String> {
    let old_path = change.old_path.as_ref()?;
    let id = state.path_to_file_id.remove(old_path)?;
    state.known_file_ids.remove(&id);
    Some(id)
}

fn resolve_write_target(
    state: &mut ReplayState,
    change: &Change,
    status: char,
) -> Result<WriteTarget, CliError> {
    let new_path = change
        .new_path
        .as_ref()
        .ok_or(CliError::InvalidArgs("write target requires new path"))?;

    if status == 'R' {
        if let Some(old_path) = change.old_path.as_ref() {
            if let Some(existing_id) = state.path_to_file_id.get(old_path).cloned() {
                state.path_to_file_id.remove(old_path);
                state
                    .path_to_file_id
                    .insert(new_path.clone(), existing_id.clone());
                return Ok(WriteTarget {
                    id: existing_id,
                    is_insert: false,
                });
            }
        }
    }

    if let Some(existing_id) = state.path_to_file_id.get(new_path).cloned() {
        return Ok(WriteTarget {
            id: existing_id,
            is_insert: false,
        });
    }

    let generated = stable_file_id(new_path);
    let is_insert = !state.known_file_ids.contains(&generated);
    state
        .path_to_file_id
        .insert(new_path.clone(), generated.clone());
    Ok(WriteTarget {
        id: generated,
        is_insert,
    })
}

fn build_replay_commit_statements(
    batch: &PreparedBatch,
    max_insert_rows: usize,
) -> Vec<SqlStatement> {
    if batch.deletes.is_empty() && batch.inserts.is_empty() && batch.updates.is_empty() {
        return Vec::new();
    }

    let mut statements = Vec::<SqlStatement>::new();

    for delete_chunk in batch.deletes.chunks(500) {
        if delete_chunk.is_empty() {
            continue;
        }

        let placeholders = vec!["?"; delete_chunk.len()].join(", ");
        let sql = format!("DELETE FROM lix_file WHERE id IN ({placeholders})");
        let params = delete_chunk
            .iter()
            .cloned()
            .map(Value::Text)
            .collect::<Vec<_>>();
        statements.push(SqlStatement { sql, params });
    }

    let insert_batch_size = max_insert_rows.max(1);
    for insert_chunk in batch.inserts.chunks(insert_batch_size) {
        if insert_chunk.is_empty() {
            continue;
        }

        let mut params = Vec::<Value>::with_capacity(insert_chunk.len() * 3);
        let values_sql = insert_chunk
            .iter()
            .map(|row| {
                params.push(Value::Text(row.id.clone()));
                params.push(Value::Text(row.path.clone()));
                params.push(Value::Blob(row.data.clone()));
                "(?, ?, ?)"
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("INSERT INTO lix_file (id, path, data) VALUES {values_sql}");
        statements.push(SqlStatement { sql, params });
    }

    for row in &batch.updates {
        statements.push(SqlStatement {
            sql: "UPDATE lix_file SET path = ?, data = ? WHERE id = ?".to_string(),
            params: vec![
                Value::Text(row.path.clone()),
                Value::Blob(row.data.clone()),
                Value::Text(row.id.clone()),
            ],
        });
    }

    statements
}

fn apply_prepared_to_expected_state(
    expected_state_by_id: &mut HashMap<String, ExpectedFile>,
    prepared: &PreparedBatch,
) {
    for id in &prepared.deletes {
        expected_state_by_id.remove(id);
    }

    for row in prepared.inserts.iter().chain(prepared.updates.iter()) {
        expected_state_by_id.insert(
            row.id.clone(),
            ExpectedFile {
                path: row.path.clone(),
                sha256: sha256_hex(&row.data),
            },
        );
    }
}

fn verify_commit_state_hashes(
    lix: &Lix,
    expected_state_by_id: &HashMap<String, ExpectedFile>,
    commit_sha: &str,
) -> Result<(), CliError> {
    let result =
        pollster::block_on(lix.execute("SELECT id, path, data FROM lix_file", &[] as &[Value]))
            .map_err(|err| {
                CliError::msg(format!(
                    "failed to query replay state for verification: {err}"
                ))
            })?;
    let row_result = result.statements.first().ok_or_else(|| {
        CliError::msg("failed to query replay state for verification: no statement result returned")
    })?;
    if result.statements.len() != 1 {
        return Err(CliError::msg(format!(
            "failed to query replay state for verification: expected exactly 1 statement result, got {}",
            result.statements.len()
        )));
    }

    if row_result.rows.len() != expected_state_by_id.len() {
        return Err(CliError::msg(format!(
            "state mismatch at {commit_sha}: row count differs (lix={}, expected={})",
            row_result.rows.len(),
            expected_state_by_id.len()
        )));
    }

    let mut seen = HashSet::<String>::new();
    for (index, row) in row_result.rows.iter().enumerate() {
        if row.len() < 3 {
            return Err(CliError::msg(format!(
                "state mismatch at {commit_sha}: row {index} has fewer than 3 columns"
            )));
        }

        let id = value_to_string(&row[0], &format!("verify.id[{index}]"))?;
        let path = value_to_string(&row[1], &format!("verify.path[{index}]"))?;
        let data = value_to_blob(&row[2], &format!("verify.data[{index}]"))?;
        let hash = sha256_hex(data);

        let expected = expected_state_by_id.get(&id).ok_or_else(|| {
            CliError::msg(format!(
                "state mismatch at {commit_sha}: unexpected file id in lix state: {id}"
            ))
        })?;
        if expected.path != path {
            return Err(CliError::msg(format!(
                "state mismatch at {commit_sha}: path differs for id {id} (lix={path}, expected={})",
                expected.path
            )));
        }
        if expected.sha256 != hash {
            return Err(CliError::msg(format!(
                "state mismatch at {commit_sha}: hash differs for id {id}"
            )));
        }

        seen.insert(id);
    }

    if seen.len() != expected_state_by_id.len() {
        return Err(CliError::msg(format!(
            "state mismatch at {commit_sha}: missing rows (lix={}, expected={})",
            seen.len(),
            expected_state_by_id.len()
        )));
    }

    Ok(())
}

fn value_to_string(value: &Value, context: &str) -> Result<String, CliError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        Value::Integer(number) => Ok(number.to_string()),
        Value::Real(number) => Ok(number.to_string()),
        Value::Boolean(flag) => Ok(flag.to_string()),
        _ => Err(CliError::msg(format!(
            "unexpected scalar type for {context}"
        ))),
    }
}

fn value_to_blob<'a>(value: &'a Value, context: &str) -> Result<&'a [u8], CliError> {
    match value {
        Value::Blob(bytes) => Ok(bytes),
        _ => Err(CliError::msg(format!("unexpected blob type for {context}"))),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        out.push(hex_digit_lower(byte >> 4));
        out.push(hex_digit_lower(byte & 0x0f));
    }
    out
}

fn hex_digit_lower(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => '0',
    }
}

fn hex_digit_upper(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'A' + (value - 10)) as char,
        _ => '0',
    }
}

fn normalize_status(value: char) -> char {
    value.to_ascii_uppercase()
}

fn stable_file_id(path: &str) -> String {
    to_lix_path(path)
}

fn to_lix_path(path: &str) -> String {
    let normalized = path.replace('\\', "/");
    let without_leading_slash = normalized.strip_prefix('/').unwrap_or(&normalized);
    let encoded = without_leading_slash
        .split('/')
        .map(encode_path_segment)
        .collect::<Vec<_>>()
        .join("/");
    format!("/{encoded}")
}

fn encode_path_segment(segment: &str) -> String {
    let mut encoded = String::new();
    for byte in segment.as_bytes() {
        let is_alpha_num = byte.is_ascii_alphanumeric();
        let is_safe = matches!(*byte, b'.' | b'_' | b'~' | b'-');
        if is_alpha_num || is_safe {
            encoded.push(*byte as char);
        } else {
            encoded.push('%');
            encoded.push(hex_digit_upper(byte >> 4));
            encoded.push(hex_digit_upper(byte & 0x0f));
        }
    }
    encoded
}

fn mode_is_blob(mode: &str) -> bool {
    mode.starts_with("100") || mode == "120000"
}

fn token_to_string(token: &[u8]) -> String {
    String::from_utf8_lossy(token).to_string()
}

fn run_git_text(
    repo_path: &Path,
    args: &[String],
    stdin: Option<&[u8]>,
) -> Result<String, CliError> {
    let output = run_git_bytes(repo_path, args, stdin)?;
    Ok(String::from_utf8_lossy(&output).to_string())
}

fn run_git_bytes(
    repo_path: &Path,
    args: &[String],
    stdin: Option<&[u8]>,
) -> Result<Vec<u8>, CliError> {
    let mut command = Command::new("git");
    command.arg("-C").arg(repo_path);
    for arg in args {
        command.arg(arg);
    }
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    if stdin.is_some() {
        command.stdin(Stdio::piped());
    } else {
        command.stdin(Stdio::null());
    }

    let mut child = command
        .spawn()
        .map_err(|source| CliError::io("failed to spawn git command", source))?;

    if let Some(input) = stdin {
        let mut child_stdin = child
            .stdin
            .take()
            .ok_or_else(|| CliError::msg("failed to open stdin for git command"))?;
        child_stdin
            .write_all(input)
            .map_err(|source| CliError::io("failed to write stdin for git command", source))?;
    }

    let output = child
        .wait_with_output()
        .map_err(|source| CliError::io("failed to wait for git command", source))?;

    if output.status.success() {
        return Ok(output.stdout);
    }

    let args_preview = args.join(" ");
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let status = output
        .status
        .code()
        .map(|code| format!("exit code {code}"))
        .unwrap_or_else(|| "terminated by signal".to_string());
    Err(CliError::msg(format!(
        "git -C {} {} failed with {}: {}",
        repo_path.display(),
        args_preview,
        status,
        stderr
    )))
}

fn default_wasm_runtime() -> Result<Arc<WasmtimeRuntime>, CliError> {
    WasmtimeRuntime::new()
        .map(Arc::new)
        .map_err(|err| CliError::msg(format!("failed to initialize wasmtime runtime: {err}")))
}

fn prepare_output_path(path: &Path) -> Result<(), CliError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|source| CliError::io("failed to create output directory", source))?;
    }

    if path.exists() {
        if path.is_dir() {
            return Err(CliError::msg(format!(
                "output path points to a directory, expected a file: {}",
                path.display()
            )));
        }
        return Err(CliError::msg(format!(
            "output path already exists: {}",
            path.display()
        )));
    }

    Ok(())
}

fn validate_repo_dir(path: &Path) -> Result<(), CliError> {
    if path.is_dir() {
        return Ok(());
    }

    Err(CliError::msg(format!(
        "repo path does not exist or is not a directory: {}",
        path.display()
    )))
}

fn validate_git_repo(path: &Path) -> Result<(), CliError> {
    let args = vec!["rev-parse".to_string(), "--is-inside-work-tree".to_string()];
    let output = run_git_text(path, &args, None)?;
    if output.trim() == "true" {
        return Ok(());
    }
    Err(CliError::msg(format!(
        "repo path is not a git work tree: {}",
        path.display()
    )))
}

fn normalize_replay_ref(raw: &str) -> Result<String, CliError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(CliError::InvalidArgs("branch must not be empty"));
    }

    if trimmed == "*" {
        return Ok("--all".to_string());
    }

    Ok(trimmed.to_string())
}

fn absolutize_from_cwd(path: &Path) -> Result<PathBuf, CliError> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }

    let cwd = std::env::current_dir()
        .map_err(|source| CliError::io("failed to read current directory", source))?;
    Ok(cwd.join(path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn collect_wanted_blob_ids_skips_gitlink_oids() {
        let changes = vec![
            Change {
                status: 'A',
                old_mode: "000000".to_string(),
                new_mode: "100644".to_string(),
                new_oid: "1111111111111111111111111111111111111111".to_string(),
                old_path: None,
                new_path: Some("regular.txt".to_string()),
            },
            Change {
                status: 'A',
                old_mode: "000000".to_string(),
                new_mode: "160000".to_string(),
                new_oid: "4c9431adbd4a24aed1d9afdecbfe4eaac3a6bba9".to_string(),
                old_path: None,
                new_path: Some("submodule".to_string()),
            },
        ];

        let wanted = collect_wanted_blob_ids(&changes);
        assert_eq!(
            wanted,
            vec!["1111111111111111111111111111111111111111".to_string()]
        );
    }

    #[test]
    fn select_replay_commits_starts_from_specific_commit_inclusive() {
        let commits = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let selected = select_replay_commits(commits, Some("c"), None)
            .expect("select_replay_commits should succeed");
        assert_eq!(selected, vec!["c".to_string(), "d".to_string()]);
    }

    #[test]
    fn select_replay_commits_applies_limit_after_from_commit() {
        let commits = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
        ];
        let selected = select_replay_commits(commits, Some("b"), Some(2))
            .expect("select_replay_commits should succeed");
        assert_eq!(selected, vec!["b".to_string(), "c".to_string()]);
    }

    #[test]
    fn select_replay_commits_errors_when_from_commit_missing() {
        let commits = vec!["a".to_string(), "b".to_string()];
        let result = select_replay_commits(commits, Some("missing"), None);
        assert!(result.is_err(), "expected error for missing from-commit");
        let message = format!(
            "{}",
            result.expect_err("expected missing from-commit error")
        );
        assert!(
            message.contains("not reachable from selected ref"),
            "unexpected error message: {message}"
        );
    }

    #[test]
    fn prepare_commit_changes_typechange_blob_to_gitlink_deletes_file() {
        let mut state = ReplayState::default();
        state.path_to_file_id.insert(
            "artifact/spa-prerender-repro".to_string(),
            "/artifact/spa-prerender-repro".to_string(),
        );
        state
            .known_file_ids
            .insert("/artifact/spa-prerender-repro".to_string());

        let changes = vec![Change {
            status: 'T',
            old_mode: "100644".to_string(),
            new_mode: "160000".to_string(),
            new_oid: "4c9431adbd4a24aed1d9afdecbfe4eaac3a6bba9".to_string(),
            old_path: Some("artifact/spa-prerender-repro".to_string()),
            new_path: Some("artifact/spa-prerender-repro".to_string()),
        }];

        let prepared = prepare_commit_changes(&mut state, &changes, &HashMap::new())
            .expect("gitlink typechange should not error");

        assert_eq!(
            prepared.deletes,
            vec!["/artifact/spa-prerender-repro".to_string()]
        );
        assert!(prepared.inserts.is_empty());
        assert!(prepared.updates.is_empty());
        assert!(!state
            .path_to_file_id
            .contains_key("artifact/spa-prerender-repro"));
    }

    #[test]
    fn prepare_output_path_rejects_existing_file() {
        let temp_dir = unique_temp_dir();
        fs::create_dir_all(&temp_dir).expect("temp dir should be created");
        let output_path = temp_dir.join("existing.lix");
        fs::write(&output_path, b"existing").expect("seed file should be written");

        let result = prepare_output_path(&output_path);
        assert!(result.is_err(), "expected error when output file exists");
        let message = format!("{}", result.expect_err("expected output path error"));
        assert!(
            message.contains("output path already exists"),
            "unexpected error message: {message}"
        );

        fs::remove_file(&output_path).expect("seed file should be removable");
        fs::remove_dir_all(&temp_dir).expect("temp dir should be removable");
    }

    #[test]
    fn prepare_output_path_allows_nonexistent_file_and_creates_parent() {
        let temp_dir = unique_temp_dir();
        let nested_parent = temp_dir.join("nested").join("output");
        let output_path = nested_parent.join("new.lix");

        let result = prepare_output_path(&output_path);
        assert!(result.is_ok(), "expected success for absent output file");
        assert!(
            nested_parent.is_dir(),
            "expected parent directories to be created"
        );

        fs::remove_dir_all(&temp_dir).expect("temp dir should be removable");
    }

    fn unique_temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "lix-cli-git-replay-test-{}-{nanos}",
            std::process::id()
        ))
    }
}
