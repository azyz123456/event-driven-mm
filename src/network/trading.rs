use std::collections::BTreeMap;
use std::sync::mpsc::TryRecvError;

use anyhow::ensure;
use rust_decimal::Decimal;

use crate::hyperliquid::trading::{self as protocol, Request};
use crate::hyperliquid::{TESTNET_ENDPOINT, TESTNET_HOST};
use crate::intent::{Action, Intent};
use crate::order_test::{Command, Config, Input, Stream};
use crate::queue::{CommandsIn, Queued};
use crate::signing::Signer;

use super::connection::{Incoming, TxTicket};
use super::*;
use crate::websocket;

const CLOCK_INTERVAL: Duration = Duration::from_millis(100);
const RESPONSE_TO_DISCONNECT: Duration = Duration::from_secs(3);

pub fn run(
    reactor: Reactor,
    journal: RecordSink,
    config: Config,
    signer: Signer,
    commands: CommandsIn<Command>,
) -> Result<()> {
    let addresses = resolve(TESTNET_HOST);
    run_with(
        reactor,
        journal,
        config,
        signer,
        commands,
        |reactor, deadline| {
            let addresses = addresses
                .as_ref()
                .map_err(|error| anyhow::anyhow!("{error:#}"))?;
            reactor.connect(addresses, deadline)
        },
        public_tls_config(),
    )
}

pub(super) fn run_with(
    mut reactor: Reactor,
    mut journal: RecordSink,
    config: Config,
    signer: Signer,
    commands: CommandsIn<Command>,
    mut connect: impl FnMut(&mut Reactor, Instant) -> Result<TcpStream>,
    tls_config: Arc<ClientConfig>,
) -> Result<()> {
    journal.append(
        None,
        Event::SessionStart {
            version: REACTOR_FORMAT_VERSION,
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
        connection: 1,
        latest_bid: None,
        last_status_response: 0,
        agent,
        agent_verified: false,
    };

    loop {
        let result = match connect(&mut reactor, Instant::now() + SETUP_TIMEOUT) {
            Ok(socket) => worker.connected(&mut reactor, socket, Arc::clone(&tls_config)),
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
        if !worker.await_recovery(&mut reactor, may_reconnect)? {
            return Ok(());
        }
        worker.connection += 1;
        worker.pending.clear();
        worker.latest_bid = None;
        worker.agent_verified = false;
    }
}

struct Worker {
    journal: RecordSink,
    config: Config,
    signer: Signer,
    commands: CommandsIn<Command>,
    pending: BTreeMap<u64, Request>,
    next_request_id: u64,
    connection: u64,
    latest_bid: Option<Decimal>,
    last_status_response: u64,
    agent: String,
    agent_verified: bool,
}

impl Worker {
    fn connected(
        &mut self,
        reactor: &mut Reactor,
        socket: TcpStream,
        config: Arc<ClientConfig>,
    ) -> Result<()> {
        let mut connection = Connection::open(
            socket,
            reactor,
            self.connection,
            TESTNET_HOST,
            config,
            &mut self.journal,
            Instant::now() + SETUP_TIMEOUT,
        )?;
        let result = self.session(reactor, &mut connection);
        let reason = match &result {
            Ok(()) => "order_test_finished".into(),
            Err(error) => format!("{error:#}"),
        };
        let shutdown = if result.is_ok() {
            flush_close(reactor, &mut connection, &mut self.journal)
        } else {
            Ok(())
        };
        let closed = connection.close(reactor, &mut self.journal, reason);
        result.and(shutdown).and(closed)
    }

    fn session(&mut self, reactor: &mut Reactor, connection: &mut Connection) -> Result<()> {
        self.bootstrap(connection)?;
        let start = Instant::now();
        let mut clock = Periodic::new(TimerKind::OrderDeadlineCheck, CLOCK_INTERVAL, start);
        let mut second = Periodic::new(TimerKind::StrategySecond, STRATEGY_INTERVAL, start);
        let mut heartbeat = Periodic::new(TimerKind::Heartbeat, HEARTBEAT_INTERVAL, start);

        loop {
            for _ in 0..MESSAGE_BUDGET {
                let Some(incoming) = connection.next_message()? else {
                    break;
                };
                self.received(connection, incoming)?;
            }
            if connection.eof() {
                bail!("TLS stream ended during order test");
            }

            let now = Instant::now();
            for timer in [&mut clock, &mut second] {
                if let Some(tick) = timer.fire(now, self.journal.latency()) {
                    self.record(tick)?;
                }
            }
            if let Some(tick) = heartbeat.fire(now, self.journal.latency()) {
                self.record(tick)?;
                self.send(connection, Message::Text(r#"{"method":"ping"}"#.into()))?;
            }

            let mut until = clock
                .deadline()
                .min(second.deadline())
                .min(heartbeat.deadline());
            for i in 0..MESSAGE_BUDGET {
                match self.commands.try_recv() {
                    Ok(Queued {
                        value: Command::Finish(outcome),
                        ..
                    }) => {
                        self.record(Event::OrderOutcome { outcome })?;
                        let _ =
                            self.send(connection, Message::Close(1000_u16.to_be_bytes().to_vec()));
                        return Ok(());
                    }
                    Ok(command) => self.dispatch(connection, command)?,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        bail!("order engine closed its command channel")
                    }
                }
                if i + 1 == MESSAGE_BUDGET {
                    until = Instant::now();
                }
            }
            reactor.service(connection, until, &mut self.journal)?;
        }
    }

    fn bootstrap(&mut self, tls: &mut Connection) -> Result<()> {
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

    fn await_recovery(&mut self, reactor: &mut Reactor, may_reconnect: bool) -> Result<bool> {
        let deadline = Instant::now() + RESPONSE_TO_DISCONNECT;
        loop {
            let command = match self.commands.try_recv() {
                Ok(queued) => queued.value,
                Err(TryRecvError::Empty) if Instant::now() < deadline => {
                    reactor.park(deadline)?;
                    continue;
                }
                Err(error) => return Err(error).context("await order engine after disconnect"),
            };
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

    fn dispatch(&mut self, tls: &mut Connection, command: Queued<Command>) -> Result<()> {
        match command.value {
            Command::Trade(intent) => self.trade(tls, intent, command.enqueued_at),
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

    fn info(&mut self, tls: &mut Connection, request: Request) -> Result<()> {
        let id = self.assign_request(request);
        self.send(
            tls,
            Message::Text(request.info(&self.config, &self.agent, id)),
        )
    }

    fn trade(&mut self, tls: &mut Connection, intent: Intent, enqueued_at: Instant) -> Result<()> {
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
        if let Action::Place { book_at_ms, .. } = intent.action
            && self.elapsed_ms().saturating_sub(book_at_ms) > self.config.timings.max_book_age_ms
        {
            self.pending.remove(&request_id);
            return self.input(Input::PlaceNotSent {
                reason: "place intent expired during journal sync".into(),
            });
        }
        tls.queue_bytes(
            &bytes,
            Some(TxTicket {
                intent_id,
                enqueued_at,
            }),
        )
        .context("queue signed testnet action")
    }

    fn received(&mut self, tls: &mut Connection, incoming: Incoming) -> Result<()> {
        let Incoming {
            message,
            wall,
            timing,
        } = incoming;
        let connection = Some(self.connection);
        let message = match message {
            Message::Text(text) => {
                let mut value =
                    serde_json::from_str(&text).context("decode testnet WebSocket JSON")?;
                protocol::redact(&mut value);
                let message = Message::Text(value.to_string());
                self.journal
                    .append_at(wall, connection, Some(timing), Event::WsIn { message })?;
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
            .append_at(wall, connection, Some(timing), Event::WsIn { message })?;
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

    fn send(&mut self, tls: &mut Connection, message: Message) -> Result<()> {
        tls.send(&mut self.journal, message)
    }

    fn elapsed_ms(&self) -> u64 {
        self.journal.latency().elapsed_ns(Instant::now()) / 1_000_000
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
