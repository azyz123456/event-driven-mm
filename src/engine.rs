use std::io::Write;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, ensure};

use crate::book::Book;
use crate::event::{Event, FORMAT_VERSION, Record};
use crate::journal::JournalReader;

const FILE_POLL_INTERVAL: Duration = Duration::from_millis(5);

pub fn run(
    mut reader: JournalReader,
    writer_is_done: impl Fn() -> bool,
    mut output: impl Write,
) -> Result<()> {
    let mut book = None;
    loop {
        // If the writer is done before this read, EOF is final. Checking after
        // a read could miss a record appended between that read and the check.
        let done = writer_is_done();
        match reader.next_record()? {
            Some(Record {
                event: Event::SessionStart { version, coin, .. },
                seq,
                connection_id,
                ..
            }) => {
                ensure!(seq == 0 && connection_id.is_none(), "invalid session start");
                ensure!(
                    version == FORMAT_VERSION,
                    "unsupported journal format {version}"
                );
                ensure!(!coin.is_empty(), "empty instrument in session start");
                book = Some(Book::new(coin));
            }
            Some(record) => {
                let seq = record.seq;
                let book = book
                    .as_mut()
                    .context("journal must start with session_start")?;
                if let Some(mid) = book
                    .update(record)
                    .with_context(|| format!("apply journal record {seq}"))?
                {
                    writeln!(output, "{},{}", mid.received_at_unix_ms, mid.price)
                        .context("write midpoint output")?;
                }
            }
            None if done => break,
            None => thread::sleep(FILE_POLL_INTERVAL),
        }
    }
    reader.finish()?;
    ensure!(book.is_some(), "journal is missing session_start");
    output.flush().context("flush midpoint output")
}
