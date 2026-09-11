//! Manual local-only canonical Memory authority for the OPFS browser profile.
use super::*;
use crate::server_protocol::{ServerProtocolBody, ServerProtocolContext};
use http_body_util::BodyExt;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
#[ignore = "serves canonical browser profile fixtures until stop file exists"]
async fn partial_browser_profile_authority() {
    let manifest_path =
        std::env::var("LIX_PARTIAL_PROFILE_MANIFEST").expect("manifest path required");
    let stop_path = std::env::var("LIX_PARTIAL_PROFILE_STOP").expect("stop path required");
    assert!(
        !std::path::Path::new(&stop_path).exists(),
        "remove stale stop file"
    );
    let stop = Arc::new(AtomicBool::new(false));
    let mut servers = Vec::new();
    let mut manifest = Vec::new();
    let mut fixtures = vec![
        ("rows_16", 16, 0, 0, 0),
        ("rows_16000", 16000, 0, 0, 0),
        ("branches_128", 16, 128, 0, 0),
        ("history_256", 16, 0, 256, 0),
        ("blob_8mib", 16, 0, 0, 8 * 1024 * 1024),
    ];
    // Opt-in only: ordinary unit/profile invocations retain the small matrix.
    if std::env::var("LIX_PARTIAL_PROFILE_LARGE_BLOBS").as_deref() == Ok("1") {
        fixtures.push(("blob_320mib", 16, 0, 0, 320 * 1024 * 1024));
    }
    for (fixture, rows, branches, history, blob_bytes) in fixtures {
        eprintln!("seeding public browser partial replica profile {fixture}");
        let backing = Memory::new();
        let authority = open_lix().with_storage(backing.clone()).await.unwrap();
        for start in (0..rows).step_by(256) {
            let stop = (start + 256).min(rows);
            let values = (start..stop)
                .map(|row| {
                    format!(
                        "('partial-open-{row:06}', 'payload-{row:06}-{}')",
                        "x".repeat(128)
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            authority
                .execute(
                    &format!("INSERT INTO lix_key_value (key, value) VALUES {values}"),
                    &[],
                )
                .await
                .unwrap();
        }
        for branch in 0..branches {
            authority
                .create_branch(CreateBranchOptions {
                    id: None,
                    name: format!("profile-{branch}"),
                    from_commit_id: None,
                })
                .await
                .unwrap();
        }
        for commit in 0..history {
            authority
                .execute(
                    "UPDATE lix_key_value SET value = $1 WHERE key = 'partial-open-000000'",
                    &[Value::Text(format!("history-{commit}"))],
                )
                .await
                .unwrap();
        }
        let large_blob_fixture = fixture == "blob_320mib";
        if large_blob_fixture {
            let files = 32;
            let bytes_per_file = blob_bytes / files;
            for file in 0..files {
                // SplitMix64 stream, unique across files and chunks. Avoid
                // compressible/repeated payloads and single-file size limits.
                let mut seed = 0x1234_5678_9abc_def0u64 ^ (file as u64);
                let mut bytes = Vec::with_capacity(bytes_per_file);
                while bytes.len() < bytes_per_file {
                    seed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
                    let mut value = seed;
                    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
                    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
                    value ^= value >> 31;
                    let take = (bytes_per_file - bytes.len()).min(8);
                    bytes.extend_from_slice(&value.to_le_bytes()[..take]);
                }
                authority
                    .upsert_file_content(&format!("/unrelated/profile-{file:02}.bin"), bytes)
                    .await
                    .unwrap();
            }
        }
        if blob_bytes != 0 && !large_blob_fixture {
            // Vary every chunk's payload, avoiding a misleading fully deduped
            // large logical file composed of one repeated physical chunk.
            let bytes = (0..blob_bytes)
                .map(|offset| {
                    ((offset as u64)
                        .wrapping_mul(6364136223846793005)
                        .rotate_left((offset % 61) as u32)
                        >> 29) as u8
                })
                .collect::<Vec<_>>();
            authority
                .upsert_file_content("/profile.bin", bytes)
                .await
                .unwrap();
        }
        // Verify actual stored CAS values before any browser opening timing.
        // Iterate bounded pages, never retain another whole repository copy.
        let mut physical_blob_bytes = 0u64;
        let mut physical_blob_chunks = 0u64;
        if large_blob_fixture {
            use crate::storage_adapter::StorageAdapterRead as _;
            let adapter = authority.storage_adapter();
            let read = adapter.begin_read(Default::default()).await.unwrap();
            let mut cursor = read
                .begin_scan(
                    crate::binary_cas::BINARY_CAS_CHUNK_SPACE,
                    crate::storage_adapter::StorageKeyRange {
                        lower: std::ops::Bound::Unbounded,
                        upper: std::ops::Bound::Unbounded,
                    },
                    Default::default(),
                )
                .await
                .unwrap();
            loop {
                let (entries, more) = cursor.next_page(4).await.unwrap().into_parts();
                for entry in entries {
                    let crate::storage_adapter::StorageProjectedValue::FullValue(bytes) =
                        entry.value
                    else {
                        panic!("physical CAS measurement requires stored values");
                    };
                    physical_blob_bytes += bytes.len() as u64;
                    physical_blob_chunks += 1;
                }
                if !more {
                    break;
                }
            }
            if large_blob_fixture {
                assert!(
                    physical_blob_bytes >= 300 * 1024 * 1024,
                    "large fixture was deduplicated/compressed below300MiB: {physical_blob_bytes}"
                );
            }
            eprintln!(
                "PARTIAL_BROWSER_PHYSICAL_BLOBS {fixture} {physical_blob_bytes} bytes {physical_blob_chunks} chunks"
            );
        }
        let expected = if history == 0 {
            format!("payload-000000-{}", "x".repeat(128))
        } else {
            format!("history-{}", history - 1)
        };
        authority.close().await.unwrap();
        drop(authority);
        let server = Arc::new(
            open_lix()
                .with_storage(backing)
                .serve()
                .with_embedded_lix_id()
                .await
                .unwrap(),
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!(
            "http://{}/lix/{}",
            listener.local_addr().unwrap(),
            server.lix_id()
        );
        manifest.push(serde_json::json!({"dimension":fixture,"size":rows.max(branches).max(history).max(blob_bytes),"url":url,"key":"partial-open-000000","expected":expected,"physicalBlobBytes":large_blob_fixture.then_some(physical_blob_bytes),"physicalBlobChunks":large_blob_fixture.then_some(physical_blob_chunks),"logicalBlobBytes":blob_bytes}));
        listener.set_nonblocking(true).unwrap();
        let stop_server = stop.clone();
        servers.push(std::thread::spawn(move || {
            let mut connections = Vec::new();
            while !stop_server.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(value) => value,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => { std::thread::sleep(std::time::Duration::from_millis(2)); continue; }
                    Err(error) => panic!("profile listener: {error}"),
                };
                let server = server.clone();
                let stop = stop_server.clone();
                connections.push(std::thread::spawn(move || {
                    stream.set_read_timeout(Some(std::time::Duration::from_secs(10))).unwrap();
                    let mut header = Vec::new();
                    while !header.ends_with(b"\r\n\r\n") {
                        let mut byte = [0]; if stream.read_exact(&mut byte).is_err() { return; }
                        header.push(byte[0]); assert!(header.len() <= 64 * 1024);
                    }
                    let header = String::from_utf8(header).unwrap();
                    let header_value = |wanted: &str| header.lines().skip(1).find_map(|line| {
                        let (name,value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case(wanted).then(|| value.trim())
                    });
                    let origin = header_value("origin").unwrap_or("null");
                    let allowed_headers = header_value("access-control-request-headers").unwrap_or("content-type");
                    let mut lines = header.lines();
                    let mut first = lines.next().unwrap().split_whitespace();
                    let method = first.next().unwrap(); let path = first.next().unwrap();
                    if method == "OPTIONS" {
                        let _ = write!(stream,"HTTP/1.1 204 No Content\r\nAccess-Control-Allow-Origin: {origin}\r\nAccess-Control-Allow-Credentials: true\r\nAccess-Control-Allow-Methods: GET, POST, DELETE, OPTIONS\r\nAccess-Control-Allow-Headers: {allowed_headers}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"); return;
                    }
                    let mut request = http::Request::builder().method(method).uri(path);
                    let mut length = 0;
                    for line in lines.filter(|line| !line.is_empty()) {
                        let (name,value) = line.split_once(':').unwrap();
                        request = request.header(name,value.trim());
                        if name.eq_ignore_ascii_case("content-length") { length = value.trim().parse::<usize>().unwrap(); }
                        assert!(!name.eq_ignore_ascii_case("transfer-encoding"), "fixture expects browser content-length bodies");
                    }
                    assert!(length <= 64 * 1024 * 1024);
                    let mut body = vec![0;length]; if stream.read_exact(&mut body).is_err() { return; }
                    let request = request.body(ServerProtocolBody::full(body)).unwrap();
                    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                    let result = runtime.block_on(async {
                        tokio::select! {
                            response = server.handle(request, ServerProtocolContext::anonymous()) => {
                                let (parts, body) = response.into_parts();
                                Some((parts, body.collect().await.unwrap().to_bytes()))
                            }
                            _ = async { while !stop.load(Ordering::SeqCst) { tokio::time::sleep(std::time::Duration::from_millis(10)).await; } } => None,
                        }
                    });
                    let Some((parts,body)) = result else { return; };
                    let exposed_headers = parts.headers.keys().map(|name|name.as_str()).collect::<Vec<_>>().join(", ");
                    if write!(stream,"HTTP/1.1 {} Response\r\nAccess-Control-Allow-Origin: {origin}\r\nAccess-Control-Allow-Credentials: true\r\nAccess-Control-Expose-Headers: {exposed_headers}\r\nContent-Length: {}\r\nConnection: close\r\n",parts.status.as_u16(),body.len()).is_err() { return; }
                    for (name,value) in &parts.headers {
                        if !["content-length","connection","access-control-allow-origin","access-control-allow-credentials","access-control-expose-headers"].contains(&name.as_str()) {
                            if let Ok(value) = value.to_str() { if write!(stream,"{name}: {value}\r\n").is_err() { return; } }
                        }
                    }
                    if write!(stream,"\r\n").is_ok() { let _ = stream.write_all(&body); }
                }));
            }
            for connection in connections { connection.join().unwrap(); }
        }));
    }
    std::fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    eprintln!("PARTIAL_BROWSER_AUTHORITY_READY {manifest_path}");
    while !std::path::Path::new(&stop_path).exists() {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    stop.store(true, Ordering::SeqCst);
    for server in servers {
        server.join().unwrap();
    }
}
