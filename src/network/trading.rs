use std::collections::BTreeMap;
use std::sync::mpsc::{Receiver, TryRecvError};

use rust_decimal::Decimal;

use crate::event::ORDER_FORMAT_VERSION;
use crate::hyperliquid::trading::{self as protocol, Request};
use crate::hyperliquid::{TESTNET_ENDPOINT, TESTNET_HOST};
use crate::intent::{Action, Intent};
use crate::order_test::{Command, Config, Input, Stream};
use crate::signing::Signer;

use super::*;

const DISPATCH_POLL: Duration = Duration::from_millis(25);
const CLOCK_INTERVAL: Duration = Duration::from_millis(100);
const RESPONSE_TO_DISCONNECT: Duration = Duration::from_secs(3);

pub fn run(
    journal: JournalWriter,
    config: Config,
    signer: Signer,
    commands: Receiver<Command>,
) -> Result<()> {
    run_with(
        journal,
        config,
        signer,
        commands,
        || connect_to(TESTNET_HOST),
        public_tls_config(),
    )
}

pub(super) fn run_with(
    mut journal: JournalWriter,
    config: Config,
    signer: Signer,
    commands: Receiver<Command>,
    mut connect: impl FnMut() -> Result<TcpStream>,
    tls_config: Arc<ClientConfig>,
) -> Result<()> {
    journal.append(
        None,
        Event::SessionStart {
            version: ORDER_FORMAT_VERSION,
            endpoint: TESTNET_ENDPOINT.into(),
            coin: config.coin.clone(),
            order_test: Some(config.clone()),
        },
    )?;
    let agent = signer.address();
    let mut worker = Worker {
        journal,
        config,
        signer,
        commands,
        pending: BTreeMap::new(),
        next_request_id: 1,
        started: Instant::now(),
        connection: 1,
        latest_bid: None,
        last_status_response: 0,
        agent,
        agent_verified: false,
    };

    loop {
        let result = match connect() {
            Ok(socket) => worker.connected(socket, Arc::clone(&tls_config)),
            Err(error) => {
                worker.record(Event::TcpConnectFailed {
                    error: format!("{error:#}"),
                })?;
                Err(error)
            }
        };
        let Err(error) = result else {
            return Ok(());
        };

        let reason = format!("{error:#}");
        worker.input(Input::Disconnected {
            reason: reason.clone(),
        })?;
        let may_reconnect = worker.connection == 1;
        if !may_reconnect {
            worker.input(Input::RecoveryUnavailable { reason })?;
        }
        if !worker.await_recovery(may_reconnect)? {
            return Ok(());
        }
        worker.connection += 1;
        worker.pending.clear();
        worker.latest_bid = None;
        worker.agent_verified = false;
    }
}

struct Worker {
    journal: JournalWriter,
    config: Config,
    signer: Signer,
    commands: Receiver<Command>,
    pending: BTreeMap<u64, Request>,
    next_request_id: u64,
    started: Instant,
    connection: u64,
    latest_bid: Option<Decimal>,
    last_status_response: u64,
    agent: String,
    agent_verified: bool,
}

impl Worker {
    fn connected(&mut self, mut socket: TcpStream, tls_config: Arc<ClientConfig>) -> Result<()> {
        let open = Event::TcpOpen {
            local_addr: socket.local_addr()?,
            peer_addr: socket.peer_addr()?,
        };
        self.record(open)?;
        let result = self.session(&mut socket, tls_config);
        let _ = socket.shutdown(Shutdown::Both);
        drop(socket);
        let reason = match &result {
            Ok(()) => "order_test_finished".into(),
            Err(error) => format!("{error:#}"),
        };
        self.record(Event::TcpClose { reason })?;
        result
    }

    fn session(&mut self, socket: &mut TcpStream, config: Arc<ClientConfig>) -> Result<()> {
        socket.set_nodelay(true)?;
        socket.set_write_timeout(Some(WRITE_TIMEOUT))?;
        let mut tls = TlsSocket::new(socket, config, TESTNET_HOST)?;
        let deadline = Instant::now() + SETUP_TIMEOUT;
        tls.handshake(deadline)?;
        let mut messages = IncomingMessages::new(upgrade(
            &mut tls,
            &mut self.journal,
            deadline,
            TESTNET_HOST,
            Some(self.connection),
        )?);

        self.bootstrap(&mut tls)?;
        let mut next_tick = Instant::now();
        let mut next_heartbeat = next_tick + HEARTBEAT_INTERVAL;
        loop {
            match self.commands.try_recv() {
                Ok(Command::Finish(outcome)) => {
                    self.record(Event::OrderOutcome { outcome })?;
                    let _ = self.send(&mut tls, Message::Close(1000_u16.to_be_bytes().to_vec()));
                    tls.connection.send_close_notify();
                    let _ = tls.flush_records();
                    return Ok(());
                }
                Ok(command) => self.dispatch(&mut tls, command)?,
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => bail!("order engine closed its command channel"),
            }

            let now = Instant::now();
            if now >= next_tick {
                self.input(Input::Tick)?;
                next_tick = now + CLOCK_INTERVAL;
            }
            if now >= next_heartbeat {
                self.send(&mut tls, Message::Text(r#"{"method":"ping"}"#.into()))?;
                next_heartbeat = now + HEARTBEAT_INTERVAL;
            }
            match messages.receive(&mut tls, (now + DISPATCH_POLL).min(next_tick))? {
                Receive::Message(message) => self.received(&mut tls, message)?,
                Receive::Eof => bail!("TLS stream ended during order test"),
                Receive::Timeout => {}
            }
        }
    }

    fn bootstrap(&mut self, tls: &mut TlsSocket<'_>) -> Result<()> {
        if self.connection == 1 {
            self.send(
                tls,
                Message::Text(hyperliquid::subscribe(&self.config.coin)),
            )?;
        }
        for stream in [Stream::Orders, Stream::Fills] {
            self.send(
                tls,
                Message::Text(protocol::subscribe(stream, &self.config.account)),
            )?;
        }
        self.info(tls, Request::AgentRole)?;
        if self.connection == 1 {
            for request in Request::startup() {
                self.info(tls, request)?;
            }
        }
        Ok(())
    }

    fn await_recovery(&mut self, may_reconnect: bool) -> Result<bool> {
        let deadline = Instant::now() + RESPONSE_TO_DISCONNECT;
        loop {
            let command = self
                .commands
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .context("await order engine after disconnect")?;
            match command {
                Command::Finish(outcome) => {
                    self.record(Event::OrderOutcome { outcome })?;
                    return Ok(false);
                }
                Command::Reconnect if may_reconnect => return Ok(true),
                Command::Trade(intent) => {
                    let place = matches!(intent.action, Action::Place { .. });
                    self.record(Event::IntentQueued { intent })?;
                    if place {
                        self.input(Input::PlaceNotSent {
                            reason: "connection closed before placement dispatch".into(),
                        })?;
                    }
                }
                _ => {}
            }
        }
    }

    fn dispatch(&mut self, tls: &mut TlsSocket<'_>, command: Command) -> Result<()> {
        match command {
            Command::Trade(intent) => self.trade(tls, intent),
            Command::QueryStatus if self.agent_verified => self.info(tls, Request::Status),
            Command::QueryStatus => Ok(()),
            Command::VerifyAccount => {
                for request in Request::verification() {
                    self.info(tls, request)?;
                }
                Ok(())
            }
            Command::Reconnect => Ok(()),
            Command::Finish(_) => unreachable!("finish is handled by the connection loop"),
        }
    }

    fn info(&mut self, tls: &mut TlsSocket<'_>, request: Request) -> Result<()> {
        let id = self.assign_request(request);
        self.send(
            tls,
            Message::Text(request.info(&self.config, &self.agent, id)),
        )
    }

    fn trade(&mut self, tls: &mut TlsSocket<'_>, intent: Intent) -> Result<()> {
        let intent_id = intent.id;
        let action = intent.action.clone();
        self.record(Event::IntentQueued {
            intent: intent.clone(),
        })?;
        ensure!(
            self.agent_verified,
            "testnet agent is unverified on this connection"
        );
        let request = match action {
            Action::Place {
                price,
                size,
                book_at_ms,
                ..
            } => {
                if self.connection != 1
                    || price != self.config.price
                    || size != self.config.size
                    || self.latest_bid.is_none_or(|bid| price >= bid)
                    || self.elapsed_ms().saturating_sub(book_at_ms)
                        > self.config.timings.max_book_age_ms
                {
                    return self.input(Input::PlaceNotSent {
                        reason: "place intent failed dispatch checks or book expired".into(),
                    });
                }
                Request::Place
            }
            Action::Cancel { .. } => Request::Cancel,
        };
        ensure!(
            intent.cloid == self.config.cloid,
            "intent references a different client order ID"
        );
        let signed = match self
            .signer
            .sign(&intent, self.config.timings.action_expiry_ms)
        {
            Ok(signed) => signed,
            Err(_) if request == Request::Place => {
                return self.input(Input::PlaceNotSent {
                    reason: "could not sign place intent".into(),
                });
            }
            Err(_) => return self.input(Input::ActionUncertain),
        };
        let request_id = self.assign_request(request);
        let text = protocol::signed_post(request_id, &signed.wire_payload);
        let bytes = websocket::encode(&Message::Text(text))?;
        self.record(Event::ActionPrepared {
            intent_id,
            request_id,
            nonce: signed.nonce,
            expires_after: signed.expires_after,
            action: signed.action,
        })?;
        self.journal.sync()?;
        tls.write(&bytes).context("send signed testnet action")
    }

    fn received(&mut self, tls: &mut TlsSocket<'_>, message: Message) -> Result<()> {
        let received_at = SystemTime::now();
        let connection = Some(self.connection);
        let message = match message {
            Message::Text(text) => {
                let mut value =
                    serde_json::from_str(&text).context("decode testnet WebSocket JSON")?;
                protocol::redact(&mut value);
                let message = Message::Text(value.to_string());
                self.journal
                    .append_at(received_at, connection, Event::WsIn { message })?;
                let request = protocol::response_id(&value)?.and_then(|id| self.take_request(id));
                for observation in protocol::observations(&value, request, &self.config)? {
                    if matches!(observation, Input::AgentVerified) {
                        self.agent_verified = true;
                        if self.connection != 1 {
                            self.info(tls, Request::Status)?;
                        }
                    }
                    if let Input::PreflightFailed { reason } = &observation
                        && self.connection != 1
                        && request == Some(Request::AgentRole)
                    {
                        self.input(Input::RecoveryUnavailable {
                            reason: reason.clone(),
                        })?;
                    }
                    self.input(observation)?;
                }
                return Ok(());
            }
            message => message,
        };
        let reply = match &message {
            Message::Ping(payload) => Some(Message::Pong(payload.clone())),
            Message::Close(payload) => Some(Message::Close(payload.clone())),
            _ => None,
        };
        let closed = matches!(message, Message::Close(_));
        self.journal
            .append_at(received_at, connection, Event::WsIn { message })?;
        if let Some(reply) = reply {
            self.send(tls, reply)?;
        }
        ensure!(
            !closed,
            "testnet WebSocket closed before the order test finished"
        );
        Ok(())
    }

    fn assign_request(&mut self, request: Request) -> u64 {
        let id = self.next_request_id;
        self.next_request_id += 1;
        if request != Request::Status {
            self.pending.retain(|_, kind| *kind != request);
        }
        self.pending.insert(id, request);
        id
    }

    fn take_request(&mut self, id: u64) -> Option<Request> {
        let request = self.pending.remove(&id)?;
        if request == Request::Status {
            if id <= self.last_status_response {
                return None;
            }
            self.last_status_response = id;
            self.pending
                .retain(|older, kind| *kind != Request::Status || *older > id);
        }
        Some(request)
    }

    fn send(&mut self, tls: &mut TlsSocket<'_>, message: Message) -> Result<()> {
        send_on(tls, &mut self.journal, Some(self.connection), message)
    }

    fn elapsed_ms(&self) -> u64 {
        self.started
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    fn input(&mut self, input: Input) -> Result<()> {
        if let Input::Book { best_bid } = &input {
            self.latest_bid = *best_bid;
        }
        self.record(Event::OrderInput {
            input,
            elapsed_ms: self.elapsed_ms(),
        })
    }

    fn record(&mut self, event: Event) -> Result<()> {
        self.journal.append(Some(self.connection), event)
    }
}
