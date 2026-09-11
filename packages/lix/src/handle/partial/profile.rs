//! Manual opening profile through native HTTP and the canonical server handler.
//! Repository seeding/server startup are excluded; per-session handshake,
//! descriptor lookup, durable client publication and engine creation are timed.

use super::*;
use crate::server_protocol::{ServerProtocolBody, ServerProtocolContext};
use http_body_util::BodyExt;
use std::io::{Read, Write};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;

#[derive(Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct RequestSample {
    path: String,
    kind: String,
    status: Option<u16>,
    body_bytes: Option<usize>,
    handler_micros: Option<u128>,
}

#[tokio::test]
#[ignore = "manual canonical HTTP partial opening profile across repository dimensions"]
async fn partial_handle_http_opening_profile() {
    for (fixture, rows, branches, history, blob_bytes) in [
        ("rows_16", 16, 0, 0, 0),
        ("rows_1600", 1600, 0, 0, 0),
        ("rows_16000", 16000, 0, 0, 0),
        ("branches_128", 16, 128, 0, 0),
        ("history_256", 16, 0, 256, 0),
        ("blob_8mib", 16, 0, 0, 8 * 1024 * 1024),
        ("blob_32mib", 16, 0, 0, 32 * 1024 * 1024),
    ] {
        eprintln!("seeding private partial HTTP opening profile {fixture}");
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
        if blob_bytes != 0 {
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
        authority.close().await.unwrap();
        drop(authority);
        let server = open_lix()
            .with_storage(backing)
            .serve()
            .with_embedded_lix_id()
            .await
            .unwrap();
        let repository_id = server.lix_id().to_owned();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let locator = format!(
            "http://{}/lix/{repository_id}",
            listener.local_addr().unwrap()
        );
        let samples = Arc::new(Mutex::new(Vec::<RequestSample>::new()));
        let observed = samples.clone();
        let stopped = Arc::new(AtomicBool::new(false));
        let stop_listener = stopped.clone();
        listener.set_nonblocking(true).unwrap();
        let server = Arc::new(server);
        let mut server_thread = Some(std::thread::spawn(move || {
            let mut connections = Vec::new();
            while !stop_listener.load(Ordering::SeqCst) {
                let (mut connection, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(1));
                        continue;
                    }
                    Err(error) => panic!("profile listener: {error}"),
                };
                let server = server.clone();
                let observed = observed.clone();
                let stop = stop_listener.clone();
                connections.push(std::thread::spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                    connection.set_read_timeout(Some(std::time::Duration::from_secs(5))).unwrap();
                    let mut bytes = Vec::new();
                    while !bytes.ends_with(b"\r\n\r\n") {
                        let mut byte = [0u8];
                        if connection.read_exact(&mut byte).is_err() { return; }
                        bytes.push(byte[0]);
                        assert!(bytes.len() < 16 * 1024);
                    }
                    let text = String::from_utf8(bytes).unwrap();
                    let mut lines = text.lines();
                    let mut first = lines.next().unwrap().split_whitespace();
                    let method = first.next().unwrap();
                    let path = first.next().unwrap().to_owned();
                    let mut builder = http::Request::builder().method(method).uri(&path);
                    let mut length = 0;
                    for line in lines.filter(|line| !line.is_empty()) {
                        let (name, value) = line.split_once(':').unwrap();
                        builder = builder.header(name, value.trim());
                        if name.eq_ignore_ascii_case("content-length") { length = value.trim().parse::<usize>().unwrap(); }
                    }
                    assert!(length < 16 * 1024);
                    let mut body = vec![0; length];
                    if connection.read_exact(&mut body).is_err() { return; }
                    let parsed = url::Url::parse(&format!("http://profile.test{path}")).unwrap();
                    let kind = if parsed.path().ends_with("/sync/descriptor") {
                        if parsed.query_pairs().any(|(key, _)| key == "after" || key == "afterCursor") { "background_descriptor_wait" } else { "foreground_descriptor" }
                    } else if method == "DELETE" { "session_close" }
                    else if parsed.path().ends_with("/sync/baseline-lease/renew") { "background_lease_renewal" }
                    else if method == "GET" && !parsed.path().contains("/sync/") { "foreground_handshake" }
                    else { panic!("opening fetched unexpected repository payload: {method} {path}") };
                    // Record arrival before a canonical long-poll handler can
                    // block. In-flight requests remain visible in open samples.
                    let index = {
                        let mut observed = observed.lock().unwrap();
                        let index = observed.len();
                        observed.push(RequestSample { path:path.clone(),kind:kind.into(),status:None,body_bytes:None,handler_micros:None });
                        index
                    };
                    let request = builder.body(ServerProtocolBody::full(body)).unwrap();
                    let started = Instant::now();
                    let result = runtime.block_on(async {
                        tokio::select! {
                            response = server.handle(request, ServerProtocolContext::anonymous()) => {
                                let status=response.status();
                                Some((status,response.into_body().collect().await.unwrap().to_bytes()))
                            }
                            _ = async { while !stop.load(Ordering::SeqCst) { tokio::time::sleep(std::time::Duration::from_millis(5)).await; } } => None,
                        }
                    });
                    let Some((status,body)) = result else { return; };
                    if kind.starts_with("foreground_") {
                        assert!(status.is_success(), "{path}: {status} {}",String::from_utf8_lossy(&body));
                    }
                    assert!(body.len() <= 8192,"opening response must be bounded metadata");
                    {
                        let mut observed=observed.lock().unwrap();
                        observed[index].status=Some(status.as_u16());
                        observed[index].body_bytes=Some(body.len());
                        observed[index].handler_micros=Some(started.elapsed().as_micros());
                    }
                    // Foreground preemption legitimately disconnects a watch.
                    if write!(connection,"HTTP/1.1 {} Response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",status.as_u16(),body.len()).is_ok() { let _=connection.write_all(&body); }
                }));
            }
            drop(listener);
            for connection in connections {
                connection.join().unwrap();
            }
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(server.close()).unwrap();
        }));
        let mut opening_micros = Vec::new();
        let mut reopening_micros = Vec::new();
        for sample in 0..5 {
            let destination = crate::sync::durable_memory_for_test(Memory::new());
            let before = samples.lock().unwrap().len();
            let storage = StorageSession::acquire(destination.clone()).await.unwrap();
            let started = Instant::now();
            let lix = open_partial_lix(storage, None, None, Some(ServerOptions::new(&locator)))
                .await
                .unwrap();
            let open_micros = started.elapsed().as_micros();
            let requests = samples.lock().unwrap()[before..].to_vec();
            let foreground = requests
                .iter()
                .filter(|request| request.kind.starts_with("foreground_"))
                .collect::<Vec<_>>();
            assert_eq!(foreground.len(), 2);
            assert_eq!(foreground[0].kind, "foreground_handshake");
            assert_eq!(foreground[1].kind, "foreground_descriptor");
            let response_bytes = foreground
                .iter()
                .map(|request| {
                    request
                        .body_bytes
                        .expect("foreground response completed before open returned")
                })
                .sum::<usize>();
            assert!(response_bytes <= 8192);
            let background = requests
                .iter()
                .filter(|request| request.kind.starts_with("background_"))
                .count();
            assert!(
                background <= 4,
                "opening started excessive background metadata requests: {}",
                requests.len()
            );
            lix.close().await.unwrap();
            drop(lix);
            // Last reopen has a stopped listener and closed authority, not
            // merely an omitted server option against a still-running server.
            if sample == 4 {
                stopped.store(true, Ordering::SeqCst);
                server_thread.take().unwrap().join().unwrap();
            }
            let network_after_close = samples.lock().unwrap().len();
            let storage = StorageSession::acquire(destination).await.unwrap();
            let started = Instant::now();
            let offline = open_partial_lix(storage, None, None, None).await.unwrap();
            let reopen_micros = started.elapsed().as_micros();
            assert_eq!(samples.lock().unwrap().len(), network_after_close);
            offline.close().await.unwrap();
            opening_micros.push(open_micros);
            reopening_micros.push(reopen_micros);
            eprintln!(
                "{}",
                serde_json::json!({ "profile":"partial_handle_canonical_http", "fixture":fixture, "rows":rows, "branches":branches, "history":history, "blobBytes":blob_bytes, "sample":sample, "openMicros":open_micros, "offlineReopenMicros":reopen_micros, "observableRequestsBeforeReturn":requests.len(), "foregroundRequests":foreground.len(), "foregroundResponseBodyBytes":response_bytes, "backgroundRequestsBeforeReturn":background, "completedResponseBodyBytesBeforeReturn":requests.iter().filter_map(|request|request.body_bytes).sum::<usize>(), "authorityStoppedForReopen":sample==4, "requests":requests })
            );
        }
        stopped.store(true, Ordering::SeqCst);
        if let Some(server_thread) = server_thread {
            server_thread.join().unwrap();
        }
        opening_micros.sort_unstable();
        reopening_micros.sort_unstable();
        eprintln!(
            "{}",
            serde_json::json!({ "profile":"partial_handle_canonical_http_summary", "fixture":fixture, "samples":5, "medianOpenMicros":opening_micros[2], "p95OpenMicros":opening_micros[4], "medianOfflineReopenMicros":reopening_micros[2], "p95OfflineReopenMicros":reopening_micros[4] })
        );
    }
}
