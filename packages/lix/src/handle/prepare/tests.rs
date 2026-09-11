//! Public SQL preparation through the actual HTTP/native-demand worker.
use super::*;
use crate::server_protocol::{ServerProtocolBody, ServerProtocolContext};
use http_body_util::BodyExt;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, AtomicUsize};

struct AuthorityServer {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    url: String,
    native_requests: Arc<AtomicUsize>,
    pause_native: Arc<AtomicBool>,
}
impl AuthorityServer {
    async fn seeded() -> Self {
        let backing = Memory::new();
        let authority = open_lix().with_storage(backing.clone()).await.unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value(key,value) VALUES('prepared-key','initial')",
                &[],
            )
            .await
            .unwrap();
        // A nontrivial first-parent chain exercises the runtime's production
        // prepare_baseline_write_frontier/Myers path, not a test hydrator.
        for value in 0..64 {
            authority
                .execute(
                    "UPDATE lix_key_value SET value=$1 WHERE key='prepared-key'",
                    &[Value::Text(value.to_string())],
                )
                .await
                .unwrap();
        }
        authority
            .execute(
                "INSERT INTO lix_file(path,content) VALUES('/prepared.bin',$1)",
                &[Value::Blob(vec![17; 96 * 1024].into())],
            )
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
        listener.set_nonblocking(true).unwrap();
        let url = format!(
            "http://{}/lix/{}",
            listener.local_addr().unwrap(),
            server.lix_id()
        );
        let stop = Arc::new(AtomicBool::new(false));
        let native_requests = Arc::new(AtomicUsize::new(0));
        let pause_native = Arc::new(AtomicBool::new(false));
        let paused = pause_native.clone();
        let stopped = stop.clone();
        let requests = native_requests.clone();
        let thread = std::thread::spawn(move || {
            let mut connections = Vec::new();
            while !stopped.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(value) => value,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(2));
                        continue;
                    }
                    Err(e) => panic!("test listener: {e}"),
                };
                let server = server.clone();
                let stopped = stopped.clone();
                let requests = requests.clone();
                let paused = paused.clone();
                connections.push(std::thread::spawn(move || {
                    stream.set_read_timeout(Some(std::time::Duration::from_secs(2))).unwrap();
                    let mut headers=Vec::new();
                    while !headers.ends_with(b"\r\n\r\n") {
                        let mut byte=[0]; if stream.read_exact(&mut byte).is_err() {return;}
                        headers.push(byte[0]); assert!(headers.len()<=64*1024);
                    }
                    let headers=String::from_utf8(headers).unwrap();
                    let mut lines=headers.lines(); let mut first=lines.next().unwrap().split_whitespace();
                    let method=first.next().unwrap(); let path=first.next().unwrap();
                    let route=path.split('?').next().unwrap();
                    let native_input=route.contains("/sync/native-") || (method=="GET" && (route.ends_with("/sync/chunk") || route.ends_with("/sync/blob")));
                    if native_input {
                        requests.fetch_add(1,Ordering::SeqCst);
                        while paused.load(Ordering::SeqCst) && !stopped.load(Ordering::SeqCst) {std::thread::sleep(std::time::Duration::from_millis(2));}
                    }
                    let mut builder=http::Request::builder().method(method).uri(path); let mut length=0;
                    for line in lines.filter(|line|!line.is_empty()) {
                        let (name,value)=line.split_once(':').unwrap(); builder=builder.header(name,value.trim());
                        if name.eq_ignore_ascii_case("content-length") {length=value.trim().parse::<usize>().unwrap();}
                    }
                    assert!(length<=64*1024*1024); let mut body=vec![0;length]; if stream.read_exact(&mut body).is_err() {return;}
                    let request=builder.body(ServerProtocolBody::full(body)).unwrap();
                    let runtime=tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
                    let response=runtime.block_on(async {
                        tokio::select! {
                            response=server.handle(request,ServerProtocolContext::anonymous()) => {
                                let status=response.status(); let bytes=response.into_body().collect().await.unwrap().to_bytes(); Some((status,bytes))
                            }
                            _=async {while !stopped.load(Ordering::SeqCst) {tokio::time::sleep(std::time::Duration::from_millis(5)).await;}} => None,
                        }
                    });
                    if let Some((status,body))=response {
                        let _=write!(stream,"HTTP/1.1 {} {}\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n",status.as_u16(),status.canonical_reason().unwrap_or(""),body.len());
                        let _=stream.write_all(&body);
                    }
                }));
            }
            for connection in connections {
                connection.join().unwrap();
            }
        });
        Self {
            stop,
            thread: Some(thread),
            url,
            native_requests,
            pause_native,
        }
    }
    fn disconnect(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}
impl Drop for AuthorityServer {
    fn drop(&mut self) {
        self.disconnect();
    }
}

#[tokio::test]
async fn public_prepare_preserves_values_and_supports_repeated_offline_writes_and_reopen() {
    let mut server = AuthorityServer::seeded().await;
    let backing = crate::sync::durable_memory_for_test(Memory::new());
    let lix = open_lix()
        .with_storage(backing.clone())
        .with_server(ServerOptions::new(server.url.clone()))
        .await
        .unwrap();
    let state = lix.engine.sync_mode().partial_admission().unwrap();
    let storage = lix.storage_adapter();
    let branches = [
        state.descriptor().selected_branch.branch_id.clone(),
        crate::GLOBAL_BRANCH_ID.to_owned(),
    ];
    let read = storage.begin_read(Default::default()).await.unwrap();
    let before = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load_observed(&branches)
        .await
        .unwrap()
        .into_iter()
        .map(|x| x.raw_token)
        .collect::<Vec<_>>();
    drop(read);
    let kv = "UPDATE lix_key_value SET value=$1 WHERE key='prepared-key'";
    let file = "UPDATE lix_file SET content=$1 WHERE path='/prepared.bin'";
    let proposed = vec![23; 96 * 1024];
    lix.prepare(kv, &[Value::Text("prospective".into())])
        .await
        .unwrap();
    lix.prepare(file, &[Value::Blob(proposed.clone().into())])
        .await
        .unwrap();
    let warmed = server.native_requests.load(Ordering::SeqCst);
    assert!(warmed > 0);
    lix.prepare(kv, &[Value::Text("prospective".into())])
        .await
        .unwrap();
    lix.prepare(file, &[Value::Blob(proposed.into())])
        .await
        .unwrap();
    assert_eq!(
        server.native_requests.load(Ordering::SeqCst),
        warmed,
        "repeated preparation must reuse native inputs"
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert_eq!(
        crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load_observed(&branches)
            .await
            .unwrap()
            .into_iter()
            .map(|x| x.raw_token)
            .collect::<Vec<_>>(),
        before
    );
    drop(read);
    assert_eq!(
        lix.execute(
            "SELECT value FROM lix_key_value WHERE key='prepared-key'",
            &[]
        )
        .await
        .unwrap()
        .rows()[0]
            .get::<serde_json::Value>("value")
            .unwrap(),
        serde_json::json!(63)
    );
    assert_eq!(
        lix.execute(
            "SELECT content FROM lix_file WHERE path='/prepared.bin'",
            &[]
        )
        .await
        .unwrap()
        .rows()[0]
            .get::<Vec<u8>>("content")
            .unwrap(),
        vec![17; 96 * 1024]
    );
    // Checking unchanged original content is a distinct read scope: replacing
    // a blob can prepare without downloading the old blob's bytes.
    let before_disconnect = server.native_requests.load(Ordering::SeqCst);
    server.disconnect();
    for value in 0..3 {
        lix.execute(kv, &[Value::Text(format!("offline-{value}"))])
            .await
            .unwrap();
        lix.execute(file, &[Value::Blob(vec![31 + value; 96 * 1024].into())])
            .await
            .unwrap();
    }
    lix.close().await.unwrap();
    let reopened = open_lix().with_storage(backing).await.unwrap();
    assert_eq!(
        reopened
            .execute(
                "SELECT value FROM lix_key_value WHERE key='prepared-key'",
                &[]
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<serde_json::Value>("value")
            .unwrap(),
        serde_json::json!("offline-2")
    );
    assert_eq!(
        reopened
            .execute(
                "SELECT content FROM lix_file WHERE path='/prepared.bin'",
                &[]
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<Vec<u8>>("content")
            .unwrap(),
        vec![33; 96 * 1024]
    );
    reopened
        .execute(kv, &[Value::Text("offline-after-reopen".into())])
        .await
        .unwrap();
    reopened
        .execute(file, &[Value::Blob(vec![41; 96 * 1024].into())])
        .await
        .unwrap();
    assert_eq!(
        server.native_requests.load(Ordering::SeqCst),
        before_disconnect,
        "offline edits and reopen must perform no native fetches"
    );
    reopened.close().await.unwrap();
}

#[tokio::test]
async fn cancelled_cold_preparation_keeps_user_state_and_releases_transaction_gates() {
    let mut server = AuthorityServer::seeded().await;
    let backing = crate::sync::durable_memory_for_test(Memory::new());
    let lix = open_lix()
        .with_storage(backing)
        .with_server(ServerOptions::new(server.url.clone()))
        .await
        .unwrap();
    let state = lix.engine.sync_mode().partial_admission().unwrap();
    let storage = lix.storage_adapter();
    let branches = [
        state.descriptor().selected_branch.branch_id.clone(),
        crate::GLOBAL_BRANCH_ID.to_owned(),
    ];
    let read = storage.begin_read(Default::default()).await.unwrap();
    let before = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load_observed(&branches)
        .await
        .unwrap()
        .into_iter()
        .map(|x| x.raw_token)
        .collect::<Vec<_>>();
    drop(read);
    server.pause_native.store(true, Ordering::SeqCst);
    let sql = "UPDATE lix_file SET content=$1 WHERE path='/prepared.bin'";
    let params = [Value::Blob(vec![19; 96 * 1024].into())];
    let mut preparing = Box::pin(lix.prepare(sql, &params));
    tokio::select! {
        result=&mut preparing => panic!("cold preparation returned before blocked native demand: {result:?}"),
        _=async {tokio::time::timeout(std::time::Duration::from_secs(10),async {
            while server.native_requests.load(Ordering::SeqCst)==0 {tokio::time::sleep(std::time::Duration::from_millis(2)).await;}
        }).await.unwrap();} => {}
    }
    drop(preparing);
    server.pause_native.store(false, Ordering::SeqCst);
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        lix.prepare(sql, &params),
    )
    .await
    .unwrap()
    .unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert_eq!(
        crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load_observed(&branches)
            .await
            .unwrap()
            .into_iter()
            .map(|x| x.raw_token)
            .collect::<Vec<_>>(),
        before
    );
    drop(read);
    assert_eq!(
        lix.execute(
            "SELECT content FROM lix_file WHERE path='/prepared.bin'",
            &[]
        )
        .await
        .unwrap()
        .rows()[0]
            .get::<Vec<u8>>("content")
            .unwrap(),
        vec![17; 96 * 1024]
    );
    server.disconnect();
    lix.execute(sql, &params).await.unwrap();
    lix.close().await.unwrap();
}
