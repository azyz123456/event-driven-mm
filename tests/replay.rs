use std::io::Write;
use std::path::Path;
use std::process::{Command, Output};

const FIXTURE: &str = "tests/fixtures/btc-session.jsonl";
const EXPECTED: &[u8] = b"1700000000520,100.15\n1700000000450,98.5\n";

fn replay(path: &Path) -> std::io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_event-driven-mm"))
        .args(["replay", "--log"])
        .arg(path)
        .output()
}

#[test]
fn replays_snapshots_in_file_order_with_recorded_local_time() -> std::io::Result<()> {
    let result = replay(Path::new(FIXTURE))?;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(result.stdout, EXPECTED);
    Ok(())
}

#[test]
fn replay_reports_a_truncated_record_with_its_line_number() -> std::io::Result<()> {
    let mut input = tempfile::NamedTempFile::new()?;
    input.write_all(include_bytes!("fixtures/btc-session.jsonl"))?;
    input.write_all(b"{\"seq\":8")?;
    let result = replay(input.path())?;

    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("journal line 9"));
    assert_eq!(result.stdout, EXPECTED);
    Ok(())
}

#[test]
fn replay_rejects_bad_journal_metadata_and_sequence() -> std::io::Result<()> {
    let header = include_str!("fixtures/btc-session.jsonl")
        .lines()
        .next()
        .unwrap();
    let invalid_record =
        r#"{"seq":2,"local_time_unix_ms":1,"connection_id":1,"event":"tcp_close","reason":"done"}"#;
    let cases = [
        (String::new(), "journal is missing session_start"),
        ("{\"seq\":0}\n".into(), "decode journal line 1"),
        (
            format!("{}\n", header.replace("\"version\":1", "\"version\":2")),
            "unsupported journal format 2",
        ),
        (
            format!("{header}\n{invalid_record}\n"),
            "journal line 2 has sequence 2, expected 1",
        ),
    ];
    for (journal, expected) in cases {
        let mut input = tempfile::NamedTempFile::new()?;
        input.write_all(journal.as_bytes())?;
        let result = replay(input.path())?;
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(!result.status.success());
        assert!(result.stdout.is_empty());
        assert!(stderr.contains(expected), "{stderr}");
    }
    Ok(())
}

#[test]
fn capture_preserves_an_existing_journal() -> std::io::Result<()> {
    let mut file = tempfile::NamedTempFile::new()?;
    let original = include_bytes!("fixtures/btc-session.jsonl");
    file.write_all(original)?;
    let result = Command::new(env!("CARGO_BIN_EXE_event-driven-mm"))
        .args(["capture", "--coin", "BTC", "--duration-secs", "1", "--log"])
        .arg(file.path())
        .output()?;
    assert!(!result.status.success());
    assert!(result.stdout.is_empty());
    assert!(String::from_utf8_lossy(&result.stderr).contains("create journal"));
    assert_eq!(std::fs::read(file.path())?, original);
    Ok(())
}

#[test]
fn replay_identifies_a_malformed_exchange_message_by_record() -> std::io::Result<()> {
    let mut input = tempfile::NamedTempFile::new()?;
    for line in include_str!("fixtures/btc-session.jsonl").lines().take(2) {
        writeln!(input, "{line}")?;
    }
    let malformed = r#"{"seq":2,"local_time_unix_ms":1,"connection_id":1,"event":"ws_in","message":{"opcode":"text","payload":"{"}}"#;
    writeln!(input, "{malformed}")?;

    let result = replay(input.path())?;
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(!result.status.success());
    assert!(result.stdout.is_empty());
    assert!(stderr.contains("apply journal record 2"), "{stderr}");
    assert!(stderr.contains("decode Hyperliquid message"), "{stderr}");
    Ok(())
}
