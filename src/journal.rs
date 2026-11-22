use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};

use crate::event::{Event, Record};

pub struct JournalWriter {
    file: File,
    next_seq: u64,
    line: Vec<u8>,
    write_failed: bool,
}

impl JournalWriter {
    pub fn new(file: File) -> Self {
        Self {
            file,
            next_seq: 0,
            line: Vec::new(),
            write_failed: false,
        }
    }

    pub fn append(&mut self, connection_id: Option<u64>, event: Event) -> Result<()> {
        self.append_at(SystemTime::now(), connection_id, event)
    }

    pub fn append_at(
        &mut self,
        time: SystemTime,
        connection_id: Option<u64>,
        event: Event,
    ) -> Result<()> {
        ensure!(!self.write_failed, "a journal write already failed");
        let local_time_unix_ms = time.duration_since(UNIX_EPOCH)?.as_millis().try_into()?;
        let record = Record {
            seq: self.next_seq,
            local_time_unix_ms,
            connection_id,
            event,
        };

        self.line.clear();
        serde_json::to_writer(&mut self.line, &record).context("encode journal record")?;
        self.line.push(b'\n');
        if let Err(error) = self.file.write_all(&self.line) {
            self.write_failed = true;
            return Err(error).with_context(|| format!("append journal record {}", self.next_seq));
        }
        self.next_seq += 1;
        Ok(())
    }
}

pub struct JournalReader {
    file: BufReader<File>,
    line: Vec<u8>,
    next_line: u64,
}

impl JournalReader {
    pub fn new(file: File) -> Self {
        Self {
            file: BufReader::new(file),
            line: Vec::new(),
            next_line: 1,
        }
    }

    pub fn next_record(&mut self) -> Result<Option<Record>> {
        self.file
            .read_until(b'\n', &mut self.line)
            .with_context(|| format!("read journal line {}", self.next_line))?;
        if self.line.last() != Some(&b'\n') {
            return Ok(None);
        }

        let record: Record = serde_json::from_slice(&self.line)
            .with_context(|| format!("decode journal line {}", self.next_line))?;
        ensure!(
            record.seq == self.next_line - 1,
            "journal line {} has sequence {}, expected {}",
            self.next_line,
            record.seq,
            self.next_line - 1
        );
        self.line.clear();
        self.next_line += 1;
        Ok(Some(record))
    }

    pub fn finish(&self) -> Result<()> {
        ensure!(
            self.line.is_empty(),
            "journal line {} is missing its terminating newline",
            self.next_line
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reader_preserves_a_partial_record_across_temporary_eof() -> Result<()> {
        let mut output = tempfile::NamedTempFile::new()?;
        let mut reader = JournalReader::new(output.reopen()?);

        assert!(reader.next_record()?.is_none());
        output.write_all(br#"{"seq":0,"local_time_unix_ms":1,"event":"tcp_close""#)?;
        assert!(reader.next_record()?.is_none());
        assert!(reader.finish().is_err());
        output.write_all(b",\"reason\":\"done\"}\n")?;
        assert!(matches!(
            reader.next_record()?.unwrap().event,
            Event::TcpClose { reason } if reason == "done"
        ));
        assert!(reader.next_record()?.is_none());
        reader.finish()
    }

    #[test]
    fn writer_rejects_further_appends_after_an_io_failure() -> Result<()> {
        let output = tempfile::NamedTempFile::new()?;
        let mut writer = JournalWriter::new(File::open(output.path())?);
        let close = || Event::TcpClose {
            reason: "done".into(),
        };

        let first = writer.append(Some(1), close()).unwrap_err();
        assert!(first.to_string().contains("append journal record 0"));
        let second = writer.append(Some(1), close()).unwrap_err();
        assert!(
            second
                .to_string()
                .contains("a journal write already failed")
        );
        assert_eq!(output.as_file().metadata()?.len(), 0);
        Ok(())
    }
}
