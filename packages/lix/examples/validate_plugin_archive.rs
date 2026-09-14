//! Validate the exact release archive through Lix's normal installation path.
use lix::{Value, open_lix};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let key = args.next().ok_or("expected plugin key")?;
    let path = args.next().ok_or("expected archive path")?;
    if args.next().is_some() {
        return Err("expected plugin key and archive path only".into());
    }
    let (sample_path, sample) = match key.as_str() {
        "plugin_json" => ("/release-smoke.json", r#"{"name":"Ada","items":[1,2]}"#),
        "plugin_csv" => ("/release-smoke.csv", "name,age\nAda,36\n"),
        "plugin_markdown" => ("/release-smoke.md", "# Release smoke\n\nParagraph.\n"),
        "plugin_text" => ("/release-smoke.txt", "Release smoke\n"),
        "plugin_excalidraw" => (
            "/release-smoke.excalidraw",
            r#"{"type":"excalidraw","version":2,"source":"https://excalidraw.com","elements":[],"appState":{},"files":{}}"#,
        ),
        _ => return Err("unknown plugin key".into()),
    };
    let archive = std::fs::read(path)?;
    let lix = open_lix().await?;
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            Value::Text(format!("/.lix/plugins/{key}.lixplugin")),
            Value::Blob(archive.into()),
        ],
    )
    .await?;
    // Trigger the compiled guest, not only the ZIP/manifest parser.
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            Value::Text(sample_path.into()),
            Value::Blob(sample.as_bytes().to_vec().into()),
        ],
    )
    .await?;
    let table = match key.as_str() {
        "plugin_json" => "json_root",
        "plugin_csv" => "csv_row",
        "plugin_markdown" => "markdown_node",
        "plugin_text" => "text_line",
        "plugin_excalidraw" => "excalidraw_scene",
        _ => unreachable!("plugin key was validated above"),
    };
    if lix
        .execute(&format!("SELECT * FROM {table}"), &[])
        .await?
        .rows()
        .is_empty()
    {
        return Err(format!("{key} did not project the sample file into {table}").into());
    }
    lix.close().await?;
    println!("Validated {key} archive installation and file projection");
    Ok(())
}
