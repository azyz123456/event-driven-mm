use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use mio::net::TcpStream;
use rustls::{ClientConfig, RootCertStore};

use crate::event::{Event, Message, REACTOR_FORMAT_VERSION, TimerKind};
use crate::hyperliquid::{self, ENDPOINT, HOST};
use crate::queue::RecordSink;

use self::connection::Connection;
use self::timer::Periodic;

mod connection;
pub mod reactor;
mod rx_timestamp;
mod timer;
pub mod trading;

use reactor::Reactor;

const CONNECTION: u64 = 1;
const SETUP_TIMEOUT: Duration = Duration::from_secs(10);
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const STRATEGY_INTERVAL: Duration = Duration::from_secs(1);
const MESSAGE_BUDGET: usize = 64;

pub fn capture(
    reactor: Reactor,
    journal: RecordSink,
    coin: &str,
    duration: Duration,
    stop: &AtomicBool,
) -> Result<()> {
    let addresses = resolve(HOST);
    capture_session(
        reactor,
        journal,
        coin,
        duration,
        stop,
        |reactor, deadline| {
            reactor.connect(
                &addresses
                    .as_ref()
                    .map_err(|error| anyhow::anyhow!("{error:#}"))?[..],
                deadline,
            )
        },
        public_tls_config(),
    )
}

fn resolve(host: &str) -> Result<Vec<SocketAddr>> {
    Ok((host, 443)
        .to_socket_addrs()
        .context("resolve Hyperliquid address")?
        .collect())
}

fn public_tls_config() -> Arc<ClientConfig> {
    client_config(RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    })
}

fn client_config(roots: RootCertStore) -> Arc<ClientConfig> {
    Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

fn capture_session(
    mut reactor: Reactor,
    mut journal: RecordSink,
    coin: &str,
    duration: Duration,
    stop: &AtomicBool,
    connect: impl FnOnce(&mut Reactor, Instant) -> Result<TcpStream>,
    config: Arc<ClientConfig>,
) -> Result<()> {
    journal.append(
        None,
        Event::SessionStart {
            version: REACTOR_FORMAT_VERSION,
            endpoint: ENDPOINT.into(),
            coin: coin.into(),
            order_test: None,
        },
    )?;
    let stream = match connect(&mut reactor, Instant::now() + SETUP_TIMEOUT) {
        Ok(stream) => stream,
        Err(error) => {
            journal.append(
                Some(CONNECTION),
                Event::TcpConnectFailed {
                    error: format!("{error:#}"),
                },
            )?;
            return Err(error);
        }
    };
    let mut connection = Connection::open(
        stream,
        &mut reactor,
        CONNECTION,
        HOST,
        config,
        &mut journal,
        Instant::now() + SETUP_TIMEOUT,
    )?;
    let outcome = capture_messages(
        &mut reactor,
        &mut connection,
        &mut journal,
        coin,
        duration,
        stop,
    );
    let reason = match &outcome {
        Ok(reason) => (*reason).into(),
        Err(error) => format!("{error:#}"),
    };
    let shutdown = if outcome.is_ok() {
        flush_close(&mut reactor, &mut connection, &mut journal)
    } else {
        Ok(())
    };
    let close = connection.close(&mut reactor, &mut journal, reason);
    outcome.and(shutdown).and(close)
}

fn capture_messages(
    reactor: &mut Reactor,
    connection: &mut Connection,
    journal: &mut RecordSink,
    coin: &str,
    duration: Duration,
    stop: &AtomicBool,
) -> Result<&'static str> {
    connection.send(journal, Message::Text(hyperliquid::subscribe(coin)))?;
    let start = Instant::now();
    let deadline = start
        .checked_add(duration)
        .context("capture duration is too large")?;
    let mut second = Periodic::new(TimerKind::StrategySecond, STRATEGY_INTERVAL, start);
    let mut heartbeat = Periodic::new(TimerKind::Heartbeat, HEARTBEAT_INTERVAL, start);
    let mut closing = None;

    loop {
        for _ in 0..MESSAGE_BUDGET {
            let Some(incoming) = connection.next_message()? else {
                break;
            };
            let reply = match &incoming.message {
                Message::Ping(payload) => Some(Message::Pong(payload.clone())),
                Message::Close(payload) if closing.is_none() => {
                    Some(Message::Close(payload.clone()))
                }
                _ => None,
            };
            let peer_closed = matches!(incoming.message, Message::Close(_));
            journal.append_at(
                incoming.wall,
                Some(CONNECTION),
                Some(incoming.timing),
                Event::WsIn {
                    message: incoming.message,
                },
            )?;
            if let Some(reply) = reply {
                connection.send(journal, reply)?;
            }
            if peer_closed {
                return Ok(closing.map_or("peer_closed", |(reason, _)| reason));
            }
        }
        if connection.eof() {
            return match closing {
                Some((reason, _)) => Ok(reason),
                None => bail!("TLS stream ended before WebSocket close"),
            };
        }

        let now = Instant::now();
        if let Some(tick) = second.fire(now, journal.latency()) {
            journal.append(Some(CONNECTION), tick)?;
        }
        let stopped = stop.load(Ordering::Relaxed);
        if closing.is_none() && (now >= deadline || stopped) {
            connection.send(journal, Message::Close(1000_u16.to_be_bytes().to_vec()))?;
            let reason = if stopped {
                "reader_stopped"
            } else {
                "capture_complete"
            };
            closing = Some((reason, now + CLOSE_TIMEOUT));
        }
        let until = if let Some((reason, close_deadline)) = closing {
            if now >= close_deadline {
                return Ok(reason);
            }
            close_deadline
        } else {
            if let Some(tick) = heartbeat.fire(now, journal.latency()) {
                journal.append(Some(CONNECTION), tick)?;
                connection.send(journal, Message::Text(r#"{"method":"ping"}"#.into()))?;
            }
            deadline.min(heartbeat.deadline())
        };
        reactor.service(connection, until.min(second.deadline()), journal)?;
    }
}

fn flush_close(
    reactor: &mut Reactor,
    connection: &mut Connection,
    journal: &mut RecordSink,
) -> Result<()> {
    connection.finish_tls()?;
    let deadline = Instant::now() + CLOSE_TIMEOUT;
    while connection.has_writes() && Instant::now() < deadline {
        reactor.service(connection, deadline, journal)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
