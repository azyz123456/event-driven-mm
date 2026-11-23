use std::collections::VecDeque;
use std::fs::File;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use mio::Waker;

use crate::event::{Event, ReceiveTiming, Record};
use crate::journal::{JournalReader, JournalWriter};
use crate::latency::{Latency, Measure, nanos};

const FILE_POLL_INTERVAL: Duration = Duration::from_millis(5);
const RECENT_PUBLICATIONS: usize = 4096;

// TODO: Evaluate a Chronicle-style append queue here for both directions once measured.
pub fn file_queue(output: File, input: File, latency: Arc<Latency>) -> (RecordSink, RecordSource) {
    let publication = Arc::new(Publication {
        recent: Mutex::new(VecDeque::new()),
        latency,
    });
    let sink = RecordSink {
        journal: JournalWriter::new(output),
        publication: Arc::clone(&publication),
        last_timing_sample: None,
    };
    let source = RecordSource {
        journal: JournalReader::new(input),
        publication: Some(publication),
        pending: None,
    };
    (sink, source)
}

struct Publication {
    recent: Mutex<VecDeque<(u64, Instant)>>,
    latency: Arc<Latency>,
}

pub struct RecordSink {
    journal: JournalWriter,
    last_timing_sample: Option<(Option<u64>, u64, u64)>,
    publication: Arc<Publication>,
}

impl RecordSink {
    pub fn latency(&self) -> &Latency {
        &self.publication.latency
    }

    pub fn append(&mut self, connection: Option<u64>, event: Event) -> Result<()> {
        self.append_at(SystemTime::now(), connection, None, event)
    }

    pub fn append_at(
        &mut self,
        wall: SystemTime,
        connection: Option<u64>,
        timing: Option<ReceiveTiming>,
        event: Event,
    ) -> Result<()> {
        self.append_sequenced(wall, connection, timing, event)
            .map(drop)
    }

    pub fn append_sequenced(
        &mut self,
        wall: SystemTime,
        connection: Option<u64>,
        timing: Option<ReceiveTiming>,
        event: Event,
    ) -> Result<u64> {
        let seq = self.journal.append_at(wall, connection, timing, event)?;
        let published = Instant::now();
        let mut recent = self
            .publication
            .recent
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if recent.len() == RECENT_PUBLICATIONS {
            recent.pop_front();
        }
        recent.push_back((seq, published));
        drop(recent);
        if let Some(timing) = timing {
            let key = (connection, timing.batch_id, timing.decoded_elapsed_ns);
            if self.last_timing_sample != Some(key) {
                self.latency().receive(timing);
                self.last_timing_sample = Some(key);
            }
        }
        Ok(seq)
    }

    pub fn sync(&self) -> Result<()> {
        let start = Instant::now();
        let result = self.journal.sync();
        self.latency().observe(
            Measure::JournalSync,
            result.as_ref().ok().map(|_| nanos(start.elapsed())),
        );
        result
    }
}

pub struct RecordSource {
    journal: JournalReader,
    publication: Option<Arc<Publication>>,
    pending: Option<Record>,
}

impl RecordSource {
    pub fn replay(file: File) -> Self {
        Self {
            journal: JournalReader::new(file),
            publication: None,
            pending: None,
        }
    }

    pub fn next(&mut self, done: &impl Fn() -> bool) -> Result<Option<Record>> {
        loop {
            // Check writer completion before the read so a terminal append cannot be missed.
            match self.poll(done())? {
                Poll::Ready(record) => return Ok(record),
                Poll::Pending => thread::sleep(FILE_POLL_INTERVAL),
            }
        }
    }

    pub fn poll(&mut self, writer_done: bool) -> Result<Poll<Option<Record>>> {
        let done = self.publication.is_none() || writer_done;
        let record = match self.pending.take() {
            Some(record) => Some(record),
            None => self.journal.next_record()?,
        };
        let Some(record) = record else {
            return if done {
                self.journal.finish()?;
                Ok(Poll::Ready(None))
            } else {
                Ok(Poll::Pending)
            };
        };

        if let Some(publication) = &self.publication {
            let recent = publication.recent.lock().unwrap_or_else(|e| e.into_inner());
            if !done && recent.back().is_none_or(|(seq, _)| *seq < record.seq) {
                self.pending = Some(record);
                return Ok(Poll::Pending);
            }
            let published = recent
                .front()
                .and_then(|(first, _)| record.seq.checked_sub(*first))
                .and_then(|offset| usize::try_from(offset).ok())
                .and_then(|index| recent.get(index).map(|(_, time)| *time));
            drop(recent);
            publication.latency.observe(
                Measure::AppendToConsume,
                published.map(|time| nanos(time.elapsed())),
            );
        }
        Ok(Poll::Ready(Some(record)))
    }
}

pub struct CommandsOut<T> {
    sender: Option<SyncSender<Queued<T>>>,
    waker: Arc<Waker>,
}

pub struct CommandsIn<T>(Receiver<Queued<T>>);

pub struct Queued<T> {
    pub value: T,
    pub enqueued_at: Instant,
}

pub fn command_queue<T>(capacity: usize, waker: Arc<Waker>) -> (CommandsOut<T>, CommandsIn<T>) {
    let (sender, receiver) = sync_channel(capacity);
    (
        CommandsOut {
            sender: Some(sender),
            waker,
        },
        CommandsIn(receiver),
    )
}

impl<T> CommandsOut<T> {
    pub fn enqueue(&self, value: T) -> Result<()> {
        self.sender
            .as_ref()
            .context("command sender is closed")?
            .try_send(Queued {
                value,
                enqueued_at: Instant::now(),
            })
            .map_err(|error| anyhow::anyhow!("enqueue network command: {error}"))?;
        self.waker
            .wake()
            .context("wake network reactor after enqueue")
    }
}

impl<T> Drop for CommandsOut<T> {
    fn drop(&mut self) {
        drop(self.sender.take());
        let _ = self.waker.wake();
    }
}

impl<T> CommandsIn<T> {
    pub fn recv(&self) -> std::result::Result<Queued<T>, std::sync::mpsc::RecvError> {
        self.0.recv()
    }

    pub fn try_recv(&self) -> std::result::Result<Queued<T>, TryRecvError> {
        self.0.try_recv()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_queue_distinguishes_temporary_empty_and_drained() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let clock = Arc::new(Latency::new());
        let (mut sink, mut source) = file_queue(file.reopen()?, file.reopen()?, Arc::clone(&clock));
        assert!(matches!(source.poll(false)?, Poll::Pending));
        sink.append(
            Some(1),
            Event::TcpClose {
                reason: "done".into(),
            },
        )?;
        assert!(matches!(source.poll(false)?, Poll::Ready(Some(record)) if record.seq == 0));
        assert!(matches!(source.poll(false)?, Poll::Pending));
        drop(sink);
        assert!(matches!(source.poll(true)?, Poll::Ready(None)));
        let mut summary = Vec::new();
        clock.write_summary(&mut summary)?;
        assert!(String::from_utf8(summary)?.contains("append_to_consume samples=1 unavailable=0"));
        Ok(())
    }

    #[test]
    fn command_queue_reports_backpressure() -> Result<()> {
        let reactor = crate::network::reactor::Reactor::new()?;
        let (sender, receiver) = command_queue(1, reactor.waker());
        sender.enqueue(1)?;
        assert!(sender.enqueue(2).is_err());
        assert_eq!(receiver.try_recv()?.value, 1);
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
        Ok(())
    }
}
