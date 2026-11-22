use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::sync::Arc;
use std::time::{Instant, SystemTime};

use anyhow::{Context, Result, ensure};
use mio::{Interest, Token, event::Event as ReadyEvent, net::TcpStream};
use rustls::{ClientConfig, ClientConnection, pki_types::ServerName};

use crate::event::{Event, Message, ReceiveTiming};
use crate::hyperliquid::PATH;
use crate::latency::{Measure, nanos};
use crate::queue::RecordSink;
use crate::websocket::{self, MessageDecoder};

use super::reactor::Reactor;
use super::rx_timestamp::{ReadStamp, Receiver};

const READ_SIZE: usize = 8192;
const IO_BUDGET: usize = 8;
const MESSAGE_BACKLOG: usize = 64;
const MAX_UPGRADE_BYTES: usize = 16 * 1024;
const TLS_HEADER_BYTES: usize = 5;
const MAX_TLS_BODY: usize = (16 * 1024) + 2048;
const MAX_PENDING_WRITES: usize = 256 * 1024;

pub struct Incoming {
    pub message: Message,
    pub wall: SystemTime,
    pub timing: ReceiveTiming,
}

pub struct TxTicket {
    pub intent_id: u64,
    pub enqueued_at: Instant,
}

struct WriteBatch {
    bytes: Vec<u8>,
    offset: usize,
    ticket: Option<TxTicket>,
}

impl WriteBatch {
    fn write_into(&mut self, stream: &mut impl Write) -> io::Result<usize> {
        let count = stream.write(&self.bytes[self.offset..])?;
        if count == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
        self.offset += count;
        Ok(count)
    }
}

pub struct Connection {
    stream: TcpStream,
    token: Token,
    connection_id: u64,
    tls: ClientConnection,
    rx: Receiver,
    records: TlsRecord,
    decoder: MessageDecoder,
    upgrade: Option<(String, Vec<u8>)>,
    upgraded: bool,
    incoming: VecDeque<Incoming>,
    outgoing: VecDeque<WriteBatch>,
    outgoing_bytes: usize,
    read_ready: bool,
    write_ready: bool,
    interested_in_write: bool,
    eof: bool,
    read_error: Option<anyhow::Error>,
}

impl Connection {
    pub fn open(
        mut stream: TcpStream,
        reactor: &mut Reactor,
        id: u64,
        host: &'static str,
        config: Arc<ClientConfig>,
        journal: &mut RecordSink,
        deadline: Instant,
    ) -> Result<Self> {
        journal.append(
            Some(id),
            Event::TcpOpen {
                local_addr: stream.local_addr()?,
                peer_addr: stream.peer_addr()?,
            },
        )?;
        let setup = (|| -> Result<_> {
            stream.set_nodelay(true)?;
            let tls = ClientConnection::new(config, ServerName::try_from(host)?)?;
            let token = reactor.register(&mut stream, Interest::READABLE)?;
            Ok((tls, token))
        })();
        let (tls, token) = match setup {
            Ok(setup) => setup,
            Err(error) => {
                drop(stream);
                journal.append(
                    Some(id),
                    Event::TcpClose {
                        reason: format!("{error:#}"),
                    },
                )?;
                return Err(error);
            }
        };
        let rx = Receiver::new(&stream);
        let mut connection = Self {
            stream,
            token,
            connection_id: id,
            tls,
            rx,
            records: TlsRecord::default(),
            decoder: MessageDecoder::new(),
            upgrade: None,
            upgraded: false,
            incoming: VecDeque::new(),
            outgoing: VecDeque::new(),
            outgoing_bytes: 0,
            read_ready: false,
            write_ready: false,
            interested_in_write: false,
            eof: false,
            read_error: None,
        };
        if let Err(error) = connection.establish(reactor, host, journal, deadline) {
            connection.close(reactor, journal, format!("{error:#}"))?;
            return Err(error);
        }
        Ok(connection)
    }

    fn establish(
        &mut self,
        reactor: &mut Reactor,
        host: &str,
        journal: &mut RecordSink,
        deadline: Instant,
    ) -> Result<()> {
        self.collect_ciphertext(None)?;
        while self.tls.is_handshaking() {
            self.check_setup(deadline, "TLS handshake")?;
            reactor.service(self, deadline, journal)?;
        }
        self.check_setup(deadline, "TLS handshake")?;
        let (request, key) = websocket::upgrade_request(host, PATH)?;
        journal.append(
            Some(self.connection_id),
            Event::HttpRequest {
                raw: request.as_bytes().to_vec().into(),
            },
        )?;
        self.upgrade = Some((key, Vec::new()));
        self.queue_bytes(request.as_bytes(), None)?;
        while !self.upgraded {
            self.check_setup(deadline, "WebSocket upgrade")?;
            reactor.service(self, deadline, journal)?;
        }
        Ok(())
    }

    fn check_setup(&mut self, deadline: Instant, operation: &str) -> Result<()> {
        if let Some(error) = self.read_error.take() {
            return Err(error);
        }
        ensure!(!self.eof, "connection closed during {operation}");
        ensure!(Instant::now() < deadline, "{operation} timed out");
        Ok(())
    }

    pub fn token(&self) -> Token {
        self.token
    }

    pub fn send(&mut self, journal: &mut RecordSink, message: Message) -> Result<()> {
        let bytes = websocket::encode(&message)?;
        journal.append(Some(self.connection_id), Event::WsOut { message })?;
        self.queue_bytes(&bytes, None)
    }

    pub fn queue_bytes(&mut self, bytes: &[u8], ticket: Option<TxTicket>) -> Result<()> {
        ensure!(
            self.outgoing_bytes.saturating_add(bytes.len()) <= MAX_PENDING_WRITES,
            "too much queued socket output"
        );
        self.tls
            .writer()
            .write_all(bytes)
            .context("buffer TLS plaintext")?;
        self.collect_ciphertext(ticket)
    }

    pub fn finish_tls(&mut self) -> Result<()> {
        self.eof = true;
        self.read_ready = false;
        self.incoming.clear();
        self.tls.send_close_notify();
        self.collect_ciphertext(None)
    }

    pub fn has_writes(&self) -> bool {
        !self.outgoing.is_empty()
    }

    pub fn next_message(&mut self) -> Result<Option<Incoming>> {
        if let Some(message) = self.incoming.pop_front() {
            return Ok(Some(message));
        }
        if let Some(error) = self.read_error.take() {
            return Err(error);
        }
        Ok(None)
    }

    pub fn eof(&self) -> bool {
        self.eof && self.incoming.is_empty()
    }

    pub fn close(
        mut self,
        reactor: &mut Reactor,
        journal: &mut RecordSink,
        reason: String,
    ) -> Result<()> {
        let deregister = reactor.deregister(&mut self.stream);
        let _ = self.stream.shutdown(Shutdown::Both);
        drop(self.stream);
        journal.append(Some(self.connection_id), Event::TcpClose { reason })?;
        deregister.context("deregister network socket")
    }

    pub fn refresh_interest(&mut self, reactor: &Reactor) -> Result<()> {
        let write_interest = self.has_writes();
        if write_interest != self.interested_in_write {
            let interest = if write_interest {
                Interest::READABLE | Interest::WRITABLE
            } else {
                Interest::READABLE
            };
            reactor.reregister(&mut self.stream, self.token, interest)?;
            self.interested_in_write = write_interest;
        }
        Ok(())
    }

    pub fn has_ready_work(&self) -> bool {
        self.read_ready || (self.write_ready && self.has_writes()) || !self.incoming.is_empty()
    }

    pub fn note_ready(&mut self, event: &ReadyEvent) {
        if !self.eof && self.read_error.is_none() {
            self.read_ready |= event.is_readable() || event.is_read_closed() || event.is_error();
        }
        self.write_ready |= event.is_writable() || event.is_write_closed() || event.is_error();
    }

    pub fn drive(&mut self, journal: &mut RecordSink) -> Result<()> {
        for _ in 0..IO_BUDGET {
            if !self.read_ready || self.incoming.len() >= MESSAGE_BACKLOG {
                break;
            }
            if let Err(error) = self.read_once(journal) {
                self.read_ready = false;
                self.read_error = Some(error);
            }
        }
        for _ in 0..IO_BUDGET {
            if !self.write_ready || !self.has_writes() {
                break;
            }
            if let Err(error) = self.write_once(journal) {
                self.write_ready = false;
                self.read_ready = false;
                self.read_error.get_or_insert(error);
                self.outgoing.clear();
                self.outgoing_bytes = 0;
            }
        }
        Ok(())
    }

    fn read_once(&mut self, journal: &mut RecordSink) -> Result<()> {
        let mut buffer = [0; READ_SIZE];
        match self
            .rx
            .read(&mut self.stream, &mut buffer, journal.latency())
        {
            Ok((0, _)) => {
                self.read_ready = false;
                anyhow::bail!("TCP stream ended before TLS close notification");
            }
            Ok((count, stamp)) => self.consume_ciphertext(&buffer[..count], stamp, journal),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                self.read_ready = false;
                Ok(())
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => Ok(()),
            Err(error) => Err(error).context("read TCP stream"),
        }
    }

    fn consume_ciphertext(
        &mut self,
        mut bytes: &[u8],
        stamp: ReadStamp,
        journal: &mut RecordSink,
    ) -> Result<()> {
        while !bytes.is_empty() && !self.eof {
            if !self.records.feed(&mut bytes)? {
                break;
            }
            let mut record = std::mem::take(&mut self.records.bytes);
            let result = self.process_tls_record(&record, stamp, journal);
            record.clear();
            self.records.bytes = record;
            result?;
        }
        Ok(())
    }

    fn process_tls_record(
        &mut self,
        mut encrypted: &[u8],
        stamp: ReadStamp,
        journal: &mut RecordSink,
    ) -> Result<()> {
        let mut peer_closed = false;
        while !encrypted.is_empty() {
            let used = self
                .tls
                .read_tls(&mut encrypted)
                .context("buffer TLS record")?;
            ensure!(used > 0, "TLS stopped consuming ciphertext");
            peer_closed = self
                .tls
                .process_new_packets()
                .context("process TLS record")?
                .peer_has_closed();
        }
        self.collect_ciphertext(None)?;
        let mut plaintext = [0; READ_SIZE];
        loop {
            match self.tls.reader().read(&mut plaintext) {
                Ok(0) => break,
                Ok(count) => self.consume_plaintext(&mut plaintext[..count], stamp, journal)?,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => return Err(error).context("read decrypted TLS bytes"),
            }
        }
        if peer_closed {
            self.read_ready = false;
            self.eof = true;
            if self.upgraded {
                self.decoder.finish()?;
            }
        }
        Ok(())
    }

    fn consume_plaintext(
        &mut self,
        bytes: &mut [u8],
        stamp: ReadStamp,
        journal: &mut RecordSink,
    ) -> Result<()> {
        if self.upgraded {
            return self.consume_frames(bytes, stamp, journal);
        }
        let (_, head) = self
            .upgrade
            .as_mut()
            .context("received unexpected TLS plaintext before upgrade")?;
        let start = head.len().saturating_sub(3);
        head.extend_from_slice(bytes);
        let end = head[start..]
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|offset| start + offset + 4);
        ensure!(
            end.unwrap_or(head.len()) <= MAX_UPGRADE_BYTES,
            "WebSocket upgrade header is too large"
        );
        if let Some(end) = end {
            let (key, mut head) = self.upgrade.take().context("missing HTTP upgrade state")?;
            let mut tail = head.split_off(end);
            let timing = receive_timing(stamp, journal);
            journal.append_at(
                SystemTime::now(),
                Some(self.connection_id),
                Some(timing),
                Event::HttpResponse {
                    raw: head.clone().into(),
                },
            )?;
            websocket::validate_upgrade(&head, &key)?;
            self.upgraded = true;
            self.consume_frames(&mut tail, stamp, journal)?;
        }
        Ok(())
    }

    fn consume_frames(
        &mut self,
        mut bytes: &mut [u8],
        stamp: ReadStamp,
        journal: &RecordSink,
    ) -> Result<()> {
        loop {
            let (used, message) = self.decoder.next(bytes)?;
            bytes = &mut bytes[used..];
            let Some(message) = message else {
                break;
            };
            self.incoming.push_back(Incoming {
                message,
                wall: SystemTime::now(),
                timing: receive_timing(stamp, journal),
            });
        }
        Ok(())
    }

    fn collect_ciphertext(&mut self, ticket: Option<TxTicket>) -> Result<()> {
        let mut bytes = Vec::new();
        while self.tls.wants_write() {
            ensure!(
                self.tls.write_tls(&mut bytes)? > 0,
                "TLS stopped producing ciphertext"
            );
        }
        if !bytes.is_empty() {
            if self.outgoing.is_empty() {
                self.write_ready = true;
            }
            self.outgoing_bytes += bytes.len();
            self.outgoing.push_back(WriteBatch {
                bytes,
                offset: 0,
                ticket,
            });
        } else {
            ensure!(ticket.is_none(), "signed action produced no ciphertext");
        }
        Ok(())
    }

    fn write_once(&mut self, journal: &mut RecordSink) -> Result<()> {
        let batch = self.outgoing.front_mut().context("missing pending write")?;
        let count = match batch.write_into(&mut self.stream) {
            Ok(count) => count,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                self.write_ready = false;
                return Ok(());
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => return Ok(()),
            Err(error) => return Err(error).context("write TCP stream"),
        };
        let at = Instant::now();
        self.outgoing_bytes -= count;
        if let Some(ticket) = batch.ticket.take() {
            let clock = journal.latency();
            let enqueued_elapsed_ns = clock.elapsed_ns(ticket.enqueued_at);
            let first_write_elapsed_ns = clock.elapsed_ns(at);
            clock.observe(
                Measure::IntentToWrite,
                at.checked_duration_since(ticket.enqueued_at).map(nanos),
            );
            journal.append(
                Some(self.connection_id),
                Event::TxFirstWrite {
                    intent_id: ticket.intent_id,
                    enqueued_elapsed_ns,
                    first_write_elapsed_ns,
                },
            )?;
        }
        if batch.offset == batch.bytes.len() {
            self.outgoing.pop_front();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_write_and_would_block_keep_the_same_unwritten_suffix() -> io::Result<()> {
        struct ShortWriter {
            output: Vec<u8>,
            block_next: bool,
        }
        impl Write for ShortWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if std::mem::replace(&mut self.block_next, false) {
                    return Err(io::ErrorKind::WouldBlock.into());
                }
                let count = bytes.len().min(2);
                self.output.extend_from_slice(&bytes[..count]);
                Ok(count)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut writer = ShortWriter {
            output: vec![],
            block_next: false,
        };
        let mut batch = WriteBatch {
            bytes: b"abcde".to_vec(),
            offset: 0,
            ticket: None,
        };
        assert_eq!(batch.write_into(&mut writer)?, 2);
        writer.block_next = true;
        assert_eq!(
            batch.write_into(&mut writer).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(batch.offset, 2);
        while batch.offset != batch.bytes.len() {
            batch.write_into(&mut writer)?;
        }
        assert_eq!(writer.output, b"abcde");
        Ok(())
    }
}

fn receive_timing(stamp: ReadStamp, journal: &RecordSink) -> ReceiveTiming {
    ReceiveTiming {
        decoded_elapsed_ns: journal.latency().elapsed_ns(Instant::now()),
        ..stamp.timing
    }
}

#[derive(Default)]
struct TlsRecord {
    bytes: Vec<u8>,
}

impl TlsRecord {
    // Feed exactly one TLS record to Rustls at a time. Each resulting plaintext span
    // can then inherit the read that completed the encrypted record, including its tag.
    fn feed(&mut self, bytes: &mut &[u8]) -> Result<bool> {
        if self.bytes.len() < TLS_HEADER_BYTES {
            self.take(bytes, TLS_HEADER_BYTES);
            if self.bytes.len() < TLS_HEADER_BYTES {
                return Ok(false);
            }
        }
        let size = u16::from_be_bytes([self.bytes[3], self.bytes[4]]) as usize;
        ensure!(
            size <= MAX_TLS_BODY,
            "TLS record exceeds the maximum encrypted size"
        );
        let expected = TLS_HEADER_BYTES + size;
        self.take(bytes, expected);
        Ok(self.bytes.len() == expected)
    }

    fn take(&mut self, bytes: &mut &[u8], target: usize) {
        let count = (target - self.bytes.len()).min(bytes.len());
        self.bytes.extend_from_slice(&bytes[..count]);
        *bytes = &bytes[count..];
    }
}
