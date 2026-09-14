//! SQL/Wasm integration scale probe. Pass the compiled CSV component path as argv[1].
use lix::{Lix, Memory, Value, open_lix};
use std::{
    io::{Cursor, Write},
    time::Instant,
};
#[tokio::main]
async fn main() {
    let component = std::fs::read(std::env::args().nth(1).expect("CSV component path")).unwrap();
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for (path, bytes) in [
        (
            "manifest.json",
            include_bytes!("../../manifest.json").as_slice(),
        ),
        (
            "schema/csv_row.json",
            include_bytes!("../../schema/csv_row.json").as_slice(),
        ),
        (
            "schema/csv_table.json",
            include_bytes!("../../schema/csv_table.json").as_slice(),
        ),
        ("plugin.wasm", component.as_slice()),
    ] {
        zip.start_file(path, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(bytes).unwrap();
    }
    let archive = zip.finish().unwrap().into_inner();
    let counts = std::env::var("CSV_QA_ROWS").unwrap_or("100000,1000000".into());
    let mut failures = 0;
    for count in counts.split(',').map(|n| n.parse::<usize>().unwrap()) {
        let storage = Memory::new();
        let lix = open_lix().with_storage(storage.clone()).await.unwrap();
        lix.execute(
            "INSERT INTO lix_file(path,content) VALUES($1,$2)",
            &[
                Value::Text("/.lix/plugins/plugin_csv.lixplugin".into()),
                Value::Blob(archive.clone().into()),
            ],
        )
        .await
        .unwrap();
        let compile_start = Instant::now();
        lix.execute(
            "INSERT INTO lix_file(path,content) VALUES('/warmup.csv',$1)",
            &[Value::Blob(Vec::new().into())],
        )
        .await
        .unwrap();
        println!(
            "component_warmup_ms={:.3}",
            compile_start.elapsed().as_secs_f64() * 1000.0
        );
        let mut bytes = b"0123456789,abcdefghij,0123456789,abcdefghij\r\n".repeat(count);
        println!("rows={count} bytes={}", bytes.len());
        let start = Instant::now();
        lix.execute(
            "INSERT INTO lix_file(path,content) VALUES('/scale.csv',$1)",
            &[Value::Blob(bytes.clone().into())],
        )
        .await
        .unwrap();
        println!("import_ms={:.3}", start.elapsed().as_secs_f64() * 1000.0);
        if std::env::var_os("CSV_QA_SKIP_SPARSE").is_none() {
            let start = Instant::now();
            let rows = lix
                .execute("SELECT id FROM csv_row ORDER BY order_key LIMIT 2", &[])
                .await
                .unwrap();
            println!(
                "select_first_two_ms={:.3}",
                start.elapsed().as_secs_f64() * 1000.0
            );
            let first = rows.rows()[0].get_index(0).unwrap().clone();
            let second = rows.rows()[1].get_index(0).unwrap().clone();
            for iteration in 0..2 {
                let replacement = if iteration == 0 {
                    "zbcdefghij"
                } else {
                    "ybcdefghij"
                };
                let cells = format!(r#"["0123456789","{replacement}","0123456789","abcdefghij"]"#);
                let start = Instant::now();
                lix.execute(
                    "UPDATE csv_row SET cells=$1 WHERE id=$2",
                    &[Value::Text(cells), first.clone()],
                )
                .await
                .unwrap();
                println!(
                    "sql_edit_{}_ms={:.3}",
                    iteration + 1,
                    start.elapsed().as_secs_f64() * 1000.0
                );
                bytes[11] = replacement.as_bytes()[0];
            }
            let start = Instant::now();
            lix.execute(
                "UPDATE csv_row SET cells=$1 WHERE id=$2 OR id=$3",
                &[
                    Value::Text(r#"["0123456789","xbcdefghij","0123456789","abcdefghij"]"#.into()),
                    first.clone(),
                    second,
                ],
            )
            .await
            .unwrap();
            println!(
                "sql_two_edit_ms={:.3}",
                start.elapsed().as_secs_f64() * 1000.0
            );
            bytes[11] = b'x';
            bytes[45 + 11] = b'x';
            let start = Instant::now();
            let read = lix
                .execute("SELECT content FROM lix_file WHERE path='/scale.csv'", &[])
                .await
                .unwrap();
            println!("read_ms={:.3}", start.elapsed().as_secs_f64() * 1000.0);
            assert_eq!(read.rows()[0].get::<Vec<u8>>("content").unwrap(), bytes);
            for replacement in ["expanded, quoted cell with a newline\ninside", "short"] {
                let cells = format!(
                    r#"["0123456789",{},"0123456789","abcdefghij"]"#,
                    serde_json::to_string(replacement).unwrap()
                );
                let start = Instant::now();
                lix.execute(
                    "UPDATE csv_row SET cells=$1 WHERE id=$2",
                    &[Value::Text(cells), first.clone()],
                )
                .await
                .unwrap();
                println!(
                    "sql_variable_edit_ms={:.3}",
                    start.elapsed().as_secs_f64() * 1000.0
                );
                let old_first_row_len = if bytes[11] == b'"' {
                    bytes.windows(2).position(|w| w == b"\r\n").unwrap() + 2
                } else {
                    bytes.iter().position(|b| *b == b'\n').unwrap() + 1
                };
                let encoded = if replacement.contains([',', '\n']) {
                    format!("\"{}\"", replacement.replace('"', "\"\""))
                } else {
                    replacement.to_owned()
                };
                let rendered = format!("0123456789,{encoded},0123456789,abcdefghij\r\n");
                bytes.splice(..old_first_row_len, rendered.bytes());
                let read = lix
                    .execute("SELECT content FROM lix_file WHERE path='/scale.csv'", &[])
                    .await
                    .unwrap();
                assert_eq!(read.rows()[0].get::<Vec<u8>>("content").unwrap(), bytes);
            }
            let third_row = bytes.windows(2).position(|w| w == b"\r\n").unwrap() + 2 + 45;
            bytes[third_row + 11] = b'w';
            let start = Instant::now();
            lix.execute(
                "UPDATE lix_file SET content=$1 WHERE path='/scale.csv'",
                &[Value::Blob(bytes.clone().into())],
            )
            .await
            .unwrap();
            println!(
                "file_edit_after_sql_ms={:.3}",
                start.elapsed().as_secs_f64() * 1000.0
            );
            let read = lix
                .execute("SELECT content FROM lix_file WHERE path='/scale.csv'", &[])
                .await
                .unwrap();
            assert_eq!(read.rows()[0].get::<Vec<u8>>("content").unwrap(), bytes);
        }
        if std::env::var_os("CSV_QA_STRUCTURAL").is_some() {
            failures += structural(&lix, &mut bytes).await;
        }
        lix.close().await.unwrap();
        let reopened = open_lix().with_storage(storage).await.unwrap();
        verify(&reopened, &bytes).await;
        println!("reopen_exact_bytes=pass");
        reopened.close().await.unwrap();
    }
    assert_eq!(
        failures, 0,
        "structural SQL operations failed; see ERROR lines above"
    );
}

async fn verify(lix: &Lix<Memory>, bytes: &[u8]) {
    let read = lix
        .execute("SELECT content FROM lix_file WHERE path='/scale.csv'", &[])
        .await
        .unwrap();
    assert_eq!(read.rows()[0].get::<Vec<u8>>("content").unwrap(), bytes);
}
async fn structural(lix: &Lix<Memory>, bytes: &mut Vec<u8>) -> usize {
    let mut failures = 0;
    let rows = lix
        .execute(
            "SELECT id, order_key FROM csv_row ORDER BY order_key LIMIT 2",
            &[],
        )
        .await
        .unwrap();
    let first = rows.rows()[0].get_index(0).unwrap().clone();
    let second = rows.rows()[1].get_index(0).unwrap().clone();
    let start = Instant::now();
    match lix
        .execute("DELETE FROM csv_row WHERE id=$1", &[first])
        .await
    {
        Ok(_) => {
            let end = bytes.iter().position(|b| *b == b'\n').unwrap() + 1;
            bytes.drain(..end);
            println!(
                "sql_delete_ms={:.3}",
                start.elapsed().as_secs_f64() * 1000.0
            );
        }
        Err(e) => {
            failures += 1;
            println!("sql_delete_ERROR={e:?}");
        }
    }
    verify(lix, bytes).await;
    let before_reorder = lix
        .execute("SELECT id FROM csv_row ORDER BY order_key LIMIT 2", &[])
        .await
        .unwrap();
    let second_index = usize::from(before_reorder.rows()[0].get_index(0).unwrap() != &second);
    let start = Instant::now();
    match lix
        .execute(
            "UPDATE csv_row SET order_key='fffffffffffffffe' WHERE id=$1",
            &[second],
        )
        .await
    {
        Ok(_) => {
            let row_start = if second_index == 0 {
                0
            } else {
                bytes.iter().position(|b| *b == b'\n').unwrap() + 1
            };
            let end = row_start + bytes[row_start..].iter().position(|b| *b == b'\n').unwrap() + 1;
            let row = bytes.drain(row_start..end).collect::<Vec<_>>();
            bytes.extend_from_slice(&row);
            println!(
                "sql_reorder_ms={:.3}",
                start.elapsed().as_secs_f64() * 1000.0
            );
        }
        Err(e) => {
            failures += 1;
            println!("sql_reorder_ERROR={e:?}");
        }
    }
    verify(lix, bytes).await;
    let file = lix
        .execute("SELECT id FROM lix_file WHERE path='/scale.csv'", &[])
        .await
        .unwrap();
    let file_id = file.rows()[0].get_index(0).unwrap().clone();
    let start = Instant::now();
    match lix
        .execute(
            "INSERT INTO csv_row(order_key,cells,lixcol_file_id) VALUES('ffffffffffffffff',$1,$2)",
            &[Value::Text(r#"["appended","row"]"#.into()), file_id],
        )
        .await
    {
        Ok(_) => {
            bytes.extend_from_slice(b"appended,row\r\n");
            println!(
                "sql_insert_ms={:.3}",
                start.elapsed().as_secs_f64() * 1000.0
            );
        }
        Err(e) => {
            failures += 1;
            println!("sql_insert_ERROR={e:?}");
        }
    }
    verify(lix, bytes).await;
    let stride = std::env::var("CSV_QA_BULK_STRIDE")
        .unwrap_or("1".into())
        .parse::<usize>()
        .unwrap();
    assert!(stride > 0);
    println!("bulk_stride={stride}");
    let select_sql = format!(
        "SELECT id FROM csv_row ORDER BY order_key LIMIT {}",
        4097 * stride
    );
    let selected = lix.execute(&select_sql, &[]).await.unwrap();
    let changed_count = selected.rows().len().div_ceil(stride);
    let mut params = vec![Value::Text(r#"["bulk"]"#.into())];
    params.extend(
        selected
            .rows()
            .iter()
            .step_by(stride)
            .map(|row| row.get_index(0).unwrap().clone()),
    );
    let predicate = (2..=params.len())
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!("UPDATE csv_row SET cells=$1 WHERE id IN ({predicate})");
    let start = Instant::now();
    match lix.execute(&sql, &params).await {
        Ok(_) => {
            let elapsed = start.elapsed();
            let mut expected = Vec::with_capacity(bytes.len());
            for (ordinal, row) in bytes.split_inclusive(|b| *b == b'\n').enumerate() {
                if ordinal % stride == 0 && ordinal / stride < changed_count {
                    expected.extend_from_slice(b"bulk\r\n");
                } else {
                    expected.extend_from_slice(row);
                }
            }
            *bytes = expected;
            println!("sql_bulk_4097_ms={:.3}", elapsed.as_secs_f64() * 1000.0);
        }
        Err(e) => {
            failures += 1;
            println!("sql_bulk_4097_ERROR={e:?}");
        }
    }
    verify(lix, bytes).await;
    failures
}
