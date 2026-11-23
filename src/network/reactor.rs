use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use mio::{Events, Interest, Poll, Token, Waker, net::TcpStream};

use crate::queue::RecordSink;

use super::connection::Connection;

const WAKE: Token = Token(0);
const CONNECT_ATTEMPT: Duration = Duration::from_secs(3);

pub struct Reactor {
    poll: Poll,
    events: Events,
    waker: Arc<Waker>,
    next_token: usize,
}

impl Reactor {
    pub fn new() -> Result<Self> {
        let poll = Poll::new().context("create network poller")?;
        let waker = Arc::new(Waker::new(poll.registry(), WAKE).context("register network waker")?);
        Ok(Self {
            poll,
            events: Events::with_capacity(32),
            waker,
            next_token: 1,
        })
    }

    pub fn waker(&self) -> Arc<Waker> {
        Arc::clone(&self.waker)
    }

    pub fn register(&mut self, stream: &mut TcpStream, interest: Interest) -> io::Result<Token> {
        let token = Token(self.next_token);
        self.next_token += 1;
        self.poll.registry().register(stream, token, interest)?;
        Ok(token)
    }

    pub fn reregister(
        &self,
        stream: &mut TcpStream,
        token: Token,
        interest: Interest,
    ) -> io::Result<()> {
        self.poll.registry().reregister(stream, token, interest)
    }

    pub fn deregister(&self, stream: &mut TcpStream) -> io::Result<()> {
        self.poll.registry().deregister(stream)
    }

    pub fn connect(&mut self, addresses: &[SocketAddr], deadline: Instant) -> Result<TcpStream> {
        let mut last_error = None;
        for &address in addresses {
            if Instant::now() >= deadline {
                break;
            }
            let attempt = deadline.min(Instant::now() + CONNECT_ATTEMPT);
            let result = self.connect_one(address, attempt);
            match result {
                Ok(stream) => return Ok(stream),
                Err(error) => last_error = Some(error),
            }
        }
        if let Some(error) = last_error {
            Err(error).context("connect to Hyperliquid")
        } else {
            bail!("no Hyperliquid address connected before the deadline")
        }
    }

    fn connect_one(&mut self, address: SocketAddr, deadline: Instant) -> Result<TcpStream> {
        let mut stream = TcpStream::connect(address)?;
        let token = self.register(&mut stream, Interest::WRITABLE)?;
        let result = loop {
            self.poll_until(deadline)?;
            if self.events.iter().any(|event| event.token() == token) {
                if let Some(error) = stream.take_error()? {
                    break Err(error);
                }
                match stream.peer_addr() {
                    Ok(_) => break Ok(()),
                    Err(error) if still_connecting(&error) => {}
                    Err(error) => break Err(error),
                }
            }
            if Instant::now() >= deadline {
                break Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("connect to {address} timed out"),
                ));
            }
        };
        self.deregister(&mut stream)?;
        result?;
        Ok(stream)
    }

    pub fn park(&mut self, deadline: Instant) -> Result<()> {
        self.poll_until(deadline)
    }

    pub fn service(
        &mut self,
        connection: &mut Connection,
        deadline: Instant,
        journal: &mut RecordSink,
    ) -> Result<()> {
        self.service_many(&mut [connection], deadline, journal)
    }

    pub(super) fn service_many(
        &mut self,
        connections: &mut [&mut Connection],
        deadline: Instant,
        journal: &mut RecordSink,
    ) -> Result<()> {
        for connection in connections.iter_mut() {
            connection.refresh_interest(self)?;
        }
        let until = if connections
            .iter()
            .any(|connection| connection.has_ready_work())
        {
            Instant::now()
        } else {
            deadline
        };
        self.poll_until(until)?;
        for event in self.events.iter() {
            if let Some(connection) = connections
                .iter_mut()
                .find(|connection| connection.token() == event.token())
            {
                connection.note_ready(event);
            }
        }
        for connection in connections {
            connection.drive(journal)?;
        }
        Ok(())
    }

    fn poll_until(&mut self, deadline: Instant) -> Result<()> {
        loop {
            let timeout = deadline.saturating_duration_since(Instant::now());
            match self.poll.poll(&mut self.events, Some(timeout)) {
                Ok(()) => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error).context("poll network"),
            }
        }
    }
}

fn still_connecting(error: &io::Error) -> bool {
    if matches!(
        error.kind(),
        io::ErrorKind::NotConnected | io::ErrorKind::WouldBlock
    ) {
        return true;
    }
    #[cfg(unix)]
    return error.raw_os_error() == Some(libc::EINPROGRESS);
    #[cfg(not(unix))]
    false
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::sync::mpsc::TryRecvError;
    use std::thread;

    use super::*;
    use crate::queue::command_queue;

    #[test]
    fn connects_to_loopback_after_a_refused_address() -> Result<()> {
        let refused = TcpListener::bind("127.0.0.1:0")?;
        let first = refused.local_addr()?;
        let listening = TcpListener::bind("127.0.0.1:0")?;
        drop(refused);
        let address = listening.local_addr()?;
        let stream =
            Reactor::new()?.connect(&[first, address], Instant::now() + Duration::from_secs(5))?;
        assert_eq!(stream.peer_addr()?, address);
        Ok(())
    }

    #[test]
    fn enqueuing_a_command_wakes_an_idle_poller() -> Result<()> {
        let mut reactor = Reactor::new()?;
        let (sender, receiver) = command_queue(1, reactor.waker());
        thread::scope(|scope| -> Result<()> {
            let worker = scope.spawn(move || {
                thread::sleep(Duration::from_millis(30));
                sender.enqueue(42)
            });
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                reactor.park(deadline)?;
                match receiver.try_recv() {
                    Ok(command) => {
                        assert_eq!(command.value, 42);
                        break;
                    }
                    Err(TryRecvError::Empty) if Instant::now() < deadline => continue,
                    Err(error) => return Err(error).context("missing queued command"),
                }
            }
            assert!(Instant::now() < deadline);
            worker.join().expect("queue test panicked")
        })
    }
}
