use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Instant;
use std::{collections::HashMap, ffi::OsStr};

use lix::storage_adapter::{StorageAdapter, StorageReadOptions};
use lix::storage_bench::{
    binary_cas_payload_inventory, current_image_cas_oracle_accounting, layout_accounting,
};
use lix_storage_slatedb::SlateDB;

#[tokio::main]
async fn main() {
    let current_image_only = std::env::args_os()
        .nth(2)
        .is_some_and(|argument| argument == "--current-image-only");
    let path = std::env::args_os()
        .nth(1)
        .expect("usage: revision_pack_oracle <slatedb-path>");
    let storage = StorageAdapter::new(SlateDB::open(&path).expect("open SlateDB"));
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open storage snapshot");

    let layout = layout_accounting(&read).await;
    let current_cas_row_bytes = layout
        .iter()
        .filter(|entry| entry.space.starts_with("binary_cas."))
        .map(|entry| entry.key_bytes + entry.value_bytes)
        .sum::<u64>();
    let started = Instant::now();
    let payloads = binary_cas_payload_inventory(&read)
        .await
        .expect("reconstruct binary CAS inventory");
    let reconstruction_ms = started.elapsed().as_millis();
    let current_image = current_image_cas_oracle_accounting(&read)
        .await
        .expect("compute current-image CAS oracle");
    println!(
        "CURRENT_IMAGE_ORACLE\tcurrent_file_images={}\tretained_manifests={}\tremoved_manifests={}\tcurrent_cas_row_bytes={}\tretained_cas_row_bytes={}\treclaimable_cas_row_bytes={}",
        current_image.current_file_images,
        current_image.retained_manifests,
        current_image.removed_manifests,
        current_image.current_cas_row_bytes,
        current_image.retained_cas_row_bytes,
        current_image.reclaimable_cas_row_bytes,
    );
    if current_image_only {
        return;
    }

    let logical_bytes = payloads
        .iter()
        .map(|entry| entry.bytes.len() as u64)
        .sum::<u64>();
    let text_bytes = payloads
        .iter()
        .filter(|entry| is_likely_text(&entry.bytes))
        .map(|entry| entry.bytes.len() as u64)
        .sum::<u64>();
    let binary_bytes = logical_bytes - text_bytes;
    let text_values = payloads
        .iter()
        .filter(|entry| is_likely_text(&entry.bytes))
        .count();

    let temporary = tempfile::tempdir().expect("create pack oracle directory");
    run(Command::new("git")
        .arg("init")
        .arg("--bare")
        .arg(temporary.path()));
    let import_started = Instant::now();
    import_payloads(temporary.path(), &payloads);
    let import_ms = import_started.elapsed().as_millis();
    let pack_started = Instant::now();
    run(Command::new("git").arg("-C").arg(temporary.path()).args([
        "repack",
        "-adf",
        "--depth=1",
        "--window=4096",
    ]));
    let pack_ms = pack_started.elapsed().as_millis();
    let (pack_data_bytes, pack_index_bytes) = pack_bytes(temporary.path());
    let packed_bytes = pack_data_bytes + pack_index_bytes;
    let hybrid_zstd_3_bytes = hybrid_pack_bytes(temporary.path(), &payloads, 3);
    let hybrid_zstd_9_bytes = hybrid_pack_bytes(temporary.path(), &payloads, 9);
    let chained_pack_started = Instant::now();
    run(Command::new("git").arg("-C").arg(temporary.path()).args([
        "repack",
        "-adf",
        "--depth=50",
        "--window=4096",
    ]));
    let chained_pack_ms = chained_pack_started.elapsed().as_millis();
    let (chained_pack_data_bytes, chained_pack_index_bytes) = pack_bytes(temporary.path());
    let chained_packed_bytes = chained_pack_data_bytes + chained_pack_index_bytes;

    println!(
        "REVISION_PACK_ORACLE\tvalues={}\tlogical_bytes={logical_bytes}\ttext_values={text_values}\ttext_bytes={text_bytes}\tbinary_values={}\tbinary_bytes={binary_bytes}\tcurrent_cas_row_bytes={current_cas_row_bytes}\tone_hop_pack_data_bytes={pack_data_bytes}\tone_hop_pack_index_bytes={pack_index_bytes}\tone_hop_packed_bytes={packed_bytes}\thybrid_zstd_3_bytes={hybrid_zstd_3_bytes}\thybrid_zstd_9_bytes={hybrid_zstd_9_bytes}\tchained_pack_data_bytes={chained_pack_data_bytes}\tchained_pack_index_bytes={chained_pack_index_bytes}\tchained_packed_bytes={chained_packed_bytes}\treconstruction_ms={reconstruction_ms}\timport_ms={import_ms}\tone_hop_pack_ms={pack_ms}\tchained_pack_ms={chained_pack_ms}",
        payloads.len(),
        payloads.len() - text_values,
    );
}

fn is_likely_text(bytes: &[u8]) -> bool {
    !bytes.iter().take(8 * 1024).any(|byte| *byte == 0) && std::str::from_utf8(bytes).is_ok()
}

fn import_payloads(
    repository: &Path,
    payloads: &[lix::storage_bench::BinaryCasPayloadInventoryEntry],
) {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args(["fast-import", "--quiet"])
        .stdin(Stdio::piped())
        .spawn()
        .expect("start git fast-import");
    let mut input = child.stdin.take().expect("fast-import stdin");
    for (index, payload) in payloads.iter().enumerate() {
        writeln!(
            input,
            "blob\nmark :{}\ndata {}",
            index + 1,
            payload.bytes.len()
        )
        .expect("write blob header");
        input.write_all(&payload.bytes).expect("write blob bytes");
        input.write_all(b"\n").expect("terminate blob bytes");
    }
    input
        .write_all(
            b"commit refs/heads/oracle\ncommitter Lix Oracle <oracle@lix.dev> 0 +0000\ndata 0\n",
        )
        .expect("write oracle commit");
    for (index, payload) in payloads.iter().enumerate() {
        let class = if is_likely_text(&payload.bytes) {
            "text"
        } else {
            "binary"
        };
        let extension = if class == "text" { "txt" } else { "bin" };
        writeln!(
            input,
            "M 100644 :{} {class}/{index:08}/payload.{extension}",
            index + 1
        )
        .expect("write tree entry");
    }
    input.write_all(b"\n").expect("finish oracle commit");
    drop(input);
    assert!(child.wait().expect("wait for git fast-import").success());
}

fn run(command: &mut Command) {
    let status = command.status().expect("run git command");
    assert!(status.success(), "git command failed with {status}");
}

fn pack_bytes(repository: &Path) -> (u64, u64) {
    let pack_dir = repository.join("objects/pack");
    let mut data = 0;
    let mut index = 0;
    for entry in fs::read_dir(pack_dir).expect("read Git pack directory") {
        let entry = entry.expect("read Git pack entry");
        let bytes = entry.metadata().expect("read Git pack metadata").len();
        match entry.path().extension().and_then(|value| value.to_str()) {
            Some("pack") => data += bytes,
            Some("idx") => index += bytes,
            _ => {}
        }
    }
    (data, index)
}

/// Estimates a hard-cut pack that retains Git's one-hop delta choice but uses
/// Zstd for full anchors. Each object independently keeps the smaller encoding,
/// so already-compressed media cannot make the candidate larger.
fn hybrid_pack_bytes(
    repository: &Path,
    payloads: &[lix::storage_bench::BinaryCasPayloadInventoryEntry],
    zstd_level: i32,
) -> u64 {
    let pack_dir = repository.join("objects/pack");
    let index_path = fs::read_dir(&pack_dir)
        .expect("read Git pack directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.extension() == Some(OsStr::new("idx")))
        .expect("Git pack index exists");
    let tree = output(Command::new("git").arg("-C").arg(repository).args([
        "ls-tree",
        "-r",
        "refs/heads/oracle",
    ]));
    let payload_by_git_hash = tree
        .lines()
        .map(|line| {
            let (metadata, path) = line.split_once('\t').expect("Git tree row has path");
            let hash = metadata
                .split_whitespace()
                .nth(2)
                .expect("Git tree row has object hash");
            let index = path
                .split('/')
                .nth(1)
                .expect("oracle path has index")
                .parse::<usize>()
                .expect("oracle path index is numeric");
            (hash, &payloads[index].bytes)
        })
        .collect::<HashMap<_, _>>();
    let verify = output(
        Command::new("git")
            .arg("verify-pack")
            .arg("-v")
            .arg(&index_path),
    );
    let mut object_bytes = 0u64;
    for line in verify.lines() {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 5 && fields.len() != 7 {
            continue;
        }
        let Some(payload) = payload_by_git_hash.get(fields[0]) else {
            continue;
        };
        let git_object_bytes = fields[3]
            .parse::<u64>()
            .expect("Git packed object size is numeric");
        if fields.len() == 7 {
            object_bytes += git_object_bytes;
            continue;
        }
        let zstd_bytes = zstd::bulk::compress(payload, zstd_level)
            .expect("compress full oracle anchor")
            .len() as u64
            + 8;
        object_bytes += git_object_bytes.min(zstd_bytes);
    }
    let (_, index_bytes) = pack_bytes(repository);
    object_bytes + index_bytes + 32
}

fn output(command: &mut Command) -> String {
    let output = command.output().expect("run Git inspection command");
    assert!(
        output.status.success(),
        "git command failed: {}",
        output.status
    );
    String::from_utf8(output.stdout).expect("Git inspection output is UTF-8")
}
