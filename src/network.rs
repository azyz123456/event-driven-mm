use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, anyhow, bail, ensure};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, RootCertStore};

use crate::event::{Event, FORMAT_VERSION, Message};
use crate::hyperliquid::{self, ENDPOINT, HOST, PATH};
use crate::journal::JournalWriter;
use crate::websocket::{self, MessageDecoder};

const CONNECTION: Option<u64> = Some(1);
const SETUP_TIMEOUT: Duration = Duration::from_secs(10);
const WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const STOP_CHECK_INTERVAL: Duration = Duration::from_millis(250);
const READ_BUFFER_BYTES: usize = 8192;
const MAX_UPGRADE_HEADER_BYTES: usize = 16 * 1024;

pub fn capture(
    journal: JournalWriter,
    coin: &str,
    duration: Duration,
    stop: &AtomicBool,
) -> Result<()> {
    let roots = RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    capture_session(journal, coin, duration, stop, connect, client_config(roots))
}

fn client_config(roots: RootCertStore) -> Arc<ClientConfig> {
    Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

fn capture_session(
    mut journal: JournalWriter,
    coin: &str,
    duration: Duration,
    stop: &AtomicBool,
    connect: impl FnOnce() -> Result<TcpStream>,
    config: Arc<ClientConfig>,
) -> Result<()> {
    journal.append(
        None,
        Event::SessionStart {
            version: FORMAT_VERSION,
            endpoint: ENDPOINT.into(),
            coin: coin.into(),
        },
    )?;

    let mut socket = match connect() {
        Ok(socket) => socket,
        Err(error) => {
            journal.append(
                CONNECTION,
                Event::TcpConnectFailed {
                    error: format!("{error:#}"),
                },
            )?;
            return Err(error);
        }
    };
    let opened_at = SystemTime::now();
    journal.append_at(
        opened_at,
        CONNECTION,
        Event::TcpOpen {
            local_addr: socket.local_addr()?,
            peer_addr: socket.peer_addr()?,
        },
    )?;

    let result = run_session(&mut socket, &mut journal, coin, duration, stop, config);
    let _ = socket.shutdown(Shutdown::Both);
    drop(socket);
    let closed_at = SystemTime::now();

    let reason = match &result {
        Ok(reason) => (*reason).into(),
        Err(error) => format!("{error:#}"),
    };
    let close_record = journal.append_at(closed_at, CONNECTION, Event::TcpClose { reason });
    result.and(close_record)
}

fn connect() -> Result<TcpStream> {
    let deadline = Instant::now() + SETUP_TIMEOUT;
    let mut last_error = None;
    for address in (HOST, 443)
        .to_socket_addrs()
        .context("resolve Hyperliquid address")?
    {
        let remaining = deadline.saturating_duration_since(Instant::now());
        ensure!(!remaining.is_zero(), "TCP connection timed out");
        match TcpStream::connect_timeout(&address, remaining) {
            Ok(socket) => return Ok(socket),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.map_or_else(
        || anyhow!("no socket address for {HOST}"),
        anyhow::Error::from,
    ))
    .context("connect to Hyperliquid")
}

fn run_session(
    socket: &mut TcpStream,
    journal: &mut JournalWriter,
    coin: &str,
    duration: Duration,
    stop: &AtomicBool,
    config: Arc<ClientConfig>,
) -> Result<&'static str> {
    socket.set_nodelay(true)?;
    socket.set_write_timeout(Some(WRITE_TIMEOUT))?;
    let mut tls = TlsSocket::new(socket, config)?;
    let deadline = Instant::now() + SETUP_TIMEOUT;
    tls.handshake(deadline)?;
    let unread = upgrade(&mut tls, journal, deadline)?;
    let reason = capture_messages(&mut tls, journal, unread, coin, duration, stop)?;
    tls.connection.send_close_notify();
    let _ = tls.flush_records();
    Ok(reason)
}

fn upgrade(
    tls: &mut TlsSocket<'_>,
    journal: &mut JournalWriter,
    deadline: Instant,
) -> Result<Vec<u8>> {
    let (request, key) = websocket::upgrade_request(HOST, PATH)?;
    journal.append(
        CONNECTION,
        Event::HttpRequest {
            raw: request.as_bytes().to_vec().into(),
        },
    )?;
    tls.write(request.as_bytes())
        .context("send WebSocket upgrade request")?;

    let mut response = Vec::new();
    let mut buffer = [0; READ_BUFFER_BYTES];
    loop {
        let old_len = response.len();
        match tls.read(&mut buffer, deadline)? {
            ReadResult::Data(count) => response.extend_from_slice(&buffer[..count]),
            ReadResult::Eof => bail!("connection closed during WebSocket upgrade"),
            ReadResult::Timeout => bail!("WebSocket upgrade timed out"),
        }
        if let Some(start) = response[old_len.saturating_sub(3)..]
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
        {
            let received_at = SystemTime::now();
            let end = old_len.saturating_sub(3) + start + 4;
            ensure!(
                end <= MAX_UPGRADE_HEADER_BYTES,
                "WebSocket upgrade header is too large"
            );
            journal.append_at(
                received_at,
                CONNECTION,
                Event::HttpResponse {
                    raw: response[..end].to_vec().into(),
                },
            )?;
            websocket::validate_upgrade(&response[..end], &key)?;
            return Ok(response.split_off(end));
        }
        ensure!(
            response.len() < MAX_UPGRADE_HEADER_BYTES,
            "WebSocket upgrade header is too large"
        );
    }
}

fn capture_messages(
    tls: &mut TlsSocket<'_>,
    journal: &mut JournalWriter,
    upgrade_read_ahead: Vec<u8>,
    coin: &str,
    duration: Duration,
    stop: &AtomicBool,
) -> Result<&'static str> {
    let mut messages = IncomingMessages::new(upgrade_read_ahead);
    let mut closing = None;
    send(tls, journal, Message::Text(hyperliquid::subscribe(coin)))?;
    let started_at = Instant::now();
    let mut next_heartbeat = started_at + HEARTBEAT_INTERVAL;
    let end = started_at
        .checked_add(duration)
        .context("capture duration is too large")?;

    loop {
        let now = Instant::now();
        let stopped = stop.load(Ordering::Relaxed);
        if closing.is_none() && (now >= end || stopped) {
            send(
                tls,
                journal,
                Message::Close(1000_u16.to_be_bytes().to_vec()),
            )?;
            let reason = if stopped {
                "reader_stopped"
            } else {
                "capture_complete"
            };
            closing = Some((reason, Instant::now() + CLOSE_TIMEOUT));
        }

        let deadline = if let Some((reason, close_deadline)) = closing {
            if now >= close_deadline {
                return Ok(reason);
            }
            close_deadline
        } else {
            if now >= next_heartbeat {
                send(tls, journal, Message::Text(r#"{"method":"ping"}"#.into()))?;
                next_heartbeat = now + HEARTBEAT_INTERVAL;
            }
            end.min(next_heartbeat)
        };

        let message = match messages.receive(tls, deadline.min(now + STOP_CHECK_INTERVAL))? {
            Receive::Message(message) => message,
            Receive::Timeout => continue,
            Receive::Eof => match closing {
                Some((reason, _)) => return Ok(reason),
                None => bail!("TLS stream ended before WebSocket close"),
            },
        };
        let received_at = SystemTime::now();
        let reply = match &message {
            Message::Ping(payload) => Some(Message::Pong(payload.clone())),
            Message::Close(payload) if closing.is_none() => Some(Message::Close(payload.clone())),
            _ => None,
        };
        let peer_closed = matches!(message, Message::Close(_));
        journal.append_at(received_at, CONNECTION, Event::WsIn { message })?;
        if let Some(reply) = reply {
            send(tls, journal, reply)?;
        }
        if peer_closed {
            return Ok(closing.map_or("peer_closed", |(reason, _)| reason));
        }
    }
}

fn send(tls: &mut TlsSocket<'_>, journal: &mut JournalWriter, message: Message) -> Result<()> {
    let bytes = websocket::encode(&message)?;
    journal.append(CONNECTION, Event::WsOut { message })?;
    tls.write(&bytes).context("send WebSocket message")
}

struct IncomingMessages {
    decoder: MessageDecoder,
    plaintext: [u8; READ_BUFFER_BYTES],
    start: usize,
    end: usize,
}

enum Receive {
    Message(Message),
    Timeout,
    Eof,
}

impl IncomingMessages {
    fn new(read_ahead: Vec<u8>) -> Self {
        let mut plaintext = [0; READ_BUFFER_BYTES];
        plaintext[..read_ahead.len()].copy_from_slice(&read_ahead);
        Self {
            decoder: MessageDecoder::new(),
            plaintext,
            start: 0,
            end: read_ahead.len(),
        }
    }

    fn receive(&mut self, tls: &mut TlsSocket<'_>, deadline: Instant) -> Result<Receive> {
        loop {
            let (used, message) = self
                .decoder
                .next(&mut self.plaintext[self.start..self.end])?;
            self.start += used;
            if let Some(message) = message {
                return Ok(Receive::Message(message));
            }
            debug_assert_eq!(self.start, self.end);
            match tls.read(&mut self.plaintext, deadline)? {
                ReadResult::Data(count) => {
                    self.start = 0;
                    self.end = count;
                }
                ReadResult::Eof => {
                    self.decoder.finish()?;
                    return Ok(Receive::Eof);
                }
                ReadResult::Timeout => return Ok(Receive::Timeout),
            }
        }
    }
}

// Rustls only sees in-memory buffers. This adapter owns all socket I/O.
struct TlsSocket<'a> {
    socket: &'a mut TcpStream,
    connection: ClientConnection,
    ciphertext_in: [u8; READ_BUFFER_BYTES],
    in_start: usize,
    in_end: usize,
    ciphertext_out: Vec<u8>,
}

enum ReadResult {
    Data(usize),
    Timeout,
    Eof,
}

impl<'a> TlsSocket<'a> {
    fn new(socket: &'a mut TcpStream, config: Arc<ClientConfig>) -> Result<Self> {
        let connection = ClientConnection::new(config, ServerName::try_from(HOST)?)?;
        Ok(Self {
            socket,
            connection,
            ciphertext_in: [0; READ_BUFFER_BYTES],
            in_start: 0,
            in_end: 0,
            ciphertext_out: Vec::new(),
        })
    }

    fn handshake(&mut self, deadline: Instant) -> Result<()> {
        self.flush_records()?;
        while self.connection.is_handshaking() {
            ensure!(self.read_records(deadline)?, "TLS handshake timed out");
        }
        Ok(())
    }

    fn read(&mut self, plaintext: &mut [u8], deadline: Instant) -> Result<ReadResult> {
        loop {
            match self.connection.reader().read(plaintext) {
                Ok(0) => return Ok(ReadResult::Eof),
                Ok(count) => return Ok(ReadResult::Data(count)),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error).context("read TLS plaintext"),
            }
            if !self.read_records(deadline)? {
                return Ok(ReadResult::Timeout);
            }
        }
    }

    fn read_records(&mut self, deadline: Instant) -> Result<bool> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        if self.in_start == self.in_end {
            self.socket
                .set_read_timeout(Some(remaining.max(Duration::from_millis(1))))?;
            let count = match self.socket.read(&mut self.ciphertext_in) {
                Ok(0) => bail!("TCP stream ended before TLS close notification"),
                Ok(count) => count,
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                    ) =>
                {
                    return Ok(false);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => return Ok(true),
                Err(error) => return Err(error).context("read TCP stream"),
            };
            self.in_start = 0;
            self.in_end = count;
        }

        // Rustls may consume just part of a TCP read. Drain available plaintext
        // before feeding it more ciphertext.
        let mut input = &self.ciphertext_in[self.in_start..self.in_end];
        let consumed = self
            .connection
            .read_tls(&mut input)
            .context("buffer TLS records")?;
        ensure!(consumed > 0, "TLS stopped consuming ciphertext");
        self.in_start += consumed;
        self.connection
            .process_new_packets()
            .context("process TLS records")?;
        self.flush_records()?;
        Ok(true)
    }

    fn write(&mut self, plaintext: &[u8]) -> Result<()> {
        self.connection
            .writer()
            .write_all(plaintext)
            .context("buffer TLS plaintext")?;
        self.flush_records()
    }

    fn flush_records(&mut self) -> Result<()> {
        self.ciphertext_out.clear();
        while self.connection.wants_write() {
            self.connection
                .write_tls(&mut self.ciphertext_out)
                .context("encode TLS records")?;
        }
        self.socket
            .write_all(&self.ciphertext_out)
            .context("write TCP stream")
    }
}

#[cfg(test)]
mod tests;
