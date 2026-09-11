//! Manual local-only canonical Memory authority for the OPFS browser profile.
use super::*;
use crate::server_protocol::{ServerProtocolBody, ServerProtocolContext};
use http_body_util::BodyExt;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
#[ignore = "serves canonical browser profile fixtures until stop file exists"]
async fn partial_browser_file_profile_authority() {
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
    for rows in [16usize, 1600] {
        let fixture = format!("files_{rows}");
        let backing = Memory::new();
        let authority = open_lix().with_storage(backing.clone()).await.unwrap();
        // Direct children across 100 directories; target directory stays small.
        for start in (0..rows).step_by(128) {
            let values = (start..(start + 128).min(rows))
                .map(|i| format!("('/d{:03}/f{i:05}.txt',$1)", i % 100))
                .collect::<Vec<_>>()
                .join(",");
            authority
                .execute(
                    &format!("INSERT INTO lix_file(path,content) VALUES {values}"),
                    &[Value::Blob(vec![97, 98].into())],
                )
                .await
                .unwrap();
        }
        let target = "/d000/target.bin";
        let bytes = (0..96 * 1024)
            .map(|i| {
                ((i as u64)
                    .wrapping_mul(6364136223846793005)
                    .rotate_left((i % 61) as u32)
                    >> 29) as u8
            })
            .collect::<Vec<_>>();
        authority.upsert_file_content(target, bytes).await.unwrap();
        let unrelated = (0..1024 * 1024)
            .map(|i| {
                ((i as u64)
                    .wrapping_mul(1442695040888963407)
                    .rotate_left((i % 59) as u32)
                    >> 21) as u8
            })
            .collect::<Vec<_>>();
        authority
            .upsert_file_content("/unrelated.bin", unrelated)
            .await
            .unwrap();
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
        manifest.push(serde_json::json!({"dimension":fixture,"size":rows,"url":url,"directory":"/d000","directoryFiles":(rows+99)/100+1,"target":"/d000/target.bin","negative":"/d000/appeared.bin"}));
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
