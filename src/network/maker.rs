mod hyperliquid;
mod readback;

pub use readback::Account;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use mio::net::TcpStream;
use rust_decimal::Decimal;
use serde_json::Value;

use crate::event::{Event, MAKER_FORMAT_VERSION, MakerCapture, Message, ReceiveTiming};
use crate::lighter;
use crate::maker::{Action, Engine, Input, Intent, Mode, Status, Venue};
use crate::queue::{CommandsIn, CommandsOut, RecordSink, command_queue};
use crate::signing::Signer;

use super::connection::{Connection, Incoming, TxTicket};
use super::reactor::Reactor;

const TICK: Duration = Duration::from_millis(100);
const RETRY: Duration = Duration::from_secs(3);
const HELLO: Duration = Duration::from_secs(10);
const PING: Duration = Duration::from_secs(20);
const QUIET: Duration = Duration::from_secs(50);
const RECOVERY: Duration = Duration::from_secs(345);
const MAX_FEED_COPIES: u8 = 5;

pub static STOP: AtomicBool = AtomicBool::new(false);

struct ProtectionRequest {
    id: u64,
    generation: u64,
    sent: Instant,
    expires_ms: Option<u64>,
}

struct Session {
    engine: Engine,
    accounts: Account,
    journal: RecordSink,
    reactor: Reactor,
    feeds: Vec<Feed>,
    seed: u64,
    start_wall: u64,
    lighter: Option<lighter::signer::Signer>,
    hyperliquid: Option<Signer>,
    replies: BTreeMap<u64, (Instant, Intent)>,
    next_connection: u64,
    protect: Option<ProtectionRequest>,
    next_protection_id: u64,
    next_protection: Instant,
    disarmed: bool,
}

pub fn run(
    engine: Engine,
    accounts: Account,
    duration: Duration,
    copies: u8,
    reactor: Reactor,
    mut journal: RecordSink,
) -> Result<()> {
    ensure!(
        copies > 0 && copies <= MAX_FEED_COPIES,
        "feed copies must be between 1 and 5"
    );
    ensure!(
        engine.observe || copies == 1,
        "duplicate feeds require observe mode"
    );
    ensure!(
        engine.observe || cfg!(unix),
        "signed maker sessions require macOS or Linux"
    );
    if let Some(account) = &accounts.hyperliquid {
        ensure!(
            account.len() == 42
                && account.starts_with("0x")
                && account[2..].bytes().all(|byte| byte.is_ascii_hexdigit()),
            "invalid Hyperliquid account address"
        );
    }
    let config = &engine.config;
    let hyperliquid = if !engine.observe && config.mode.uses(Venue::Hyperliquid) {
        let account = accounts
            .hyperliquid
            .as_deref()
            .context("specify --hyperliquid-account")?;
        Some(Signer::from_env(account)?)
    } else {
        None
    };
    let lighter = if !engine.observe && config.mode.uses(Venue::Lighter) {
        Some(lighter::signer::Signer::from_env(
            accounts.lighter.context("specify --lighter-account")?,
            accounts.key_index.context("specify --lighter-key-index")?,
        )?)
    } else {
        None
    };
    let start_wall = unix_ms()?;
    let seed = engine.seed;
    let mut feeds = Vec::new();
    for venue in [Venue::Hyperliquid, Venue::Lighter]
        .into_iter()
        .filter(|&v| config.mode.uses(v))
    {
        for copy in 0..copies {
            let (commands, results) = if copy == 0 {
                let (commands, requests) = command_queue(1, reactor.waker());
                let (responses, results) = command_queue(1, reactor.waker());
                let config = config.clone();
                let accounts = accounts.clone();
                let agent = hyperliquid.as_ref().map(Signer::address);
                thread::Builder::new()
                    .name(format!("{venue:?}-readback"))
                    .spawn(move || {
                        read_worker(requests, responses, config, accounts, venue, seed, agent);
                    })?;
                (Some(commands), Some(results))
            } else {
                (None, None)
            };
            feeds.push(Feed::new(venue, copy == 0, commands, results, start_wall));
        }
    }
    journal.append(
        None,
        Event::MakerStart {
            version: MAKER_FORMAT_VERSION,
            config: config.clone(),
            seed,
            observe: engine.observe,
            capture: Some(MakerCapture {
                hyperliquid: crate::hyperliquid::TESTNET_ENDPOINT.into(),
                lighter: format!("wss://{}{}", lighter::HOST, lighter::PATH),
                feed_copies: copies,
            }),
        },
    )?;
    let mut session = Session {
        engine,
        accounts,
        journal,
        reactor,
        feeds,
        seed,
        start_wall,
        lighter,
        hyperliquid,
        replies: BTreeMap::new(),
        next_connection: 1,
        protect: None,
        next_protection_id: 1 << 40,
        next_protection: Instant::now(),
        disarmed: false,
    };
    session.drive(duration)
}

type ReadResult = (readback::Request, Result<readback::Snapshot>);

fn read_worker(
    requests: CommandsIn<readback::Request>,
    results: CommandsOut<ReadResult>,
    config: crate::maker::Config,
    accounts: Account,
    venue: Venue,
    seed: u64,
    agent: Option<String>,
) {
    let client = reqwest::blocking::Client::builder()
        .user_agent(concat!("event-driven-mm/", env!("CARGO_PKG_VERSION")))
        .timeout(Duration::from_secs(3))
        .redirect(reqwest::redirect::Policy::none())
        .build();
    while let Ok(request) = requests.recv() {
        let request = request.value;
        let result = client
            .as_ref()
            .map_err(|_| anyhow::anyhow!("create venue readback HTTP client"))
            .and_then(|client| {
                readback::fetch(
                    client,
                    &config,
                    &accounts,
                    venue,
                    seed,
                    agent.as_deref(),
                    &request,
                )
            });
        if results.enqueue((request, result)).is_err() {
            break;
        }
    }
}

struct Feed {
    venue: Venue,
    primary: bool,
    socket: Option<Connection>,
    dial: Option<mpsc::Receiver<Result<TcpStream>>>,
    start_by: Instant,
    next_dial: Instant,
    subscribed: bool,
    fills_ready: bool,
    last_rx: Instant,
    next_ping: Instant,
    commands: Option<CommandsOut<readback::Request>>,
    results: Option<CommandsIn<ReadResult>>,
    reading: bool,
    next_read: Instant,
    cutoff_ms: u64,
    ready_once: bool,
    nonce: Option<u64>,
    auth: Option<(Instant, String)>,
    book_resub: Option<Instant>,
}

impl Feed {
    fn new(
        venue: Venue,
        primary: bool,
        commands: Option<CommandsOut<readback::Request>>,
        results: Option<CommandsIn<ReadResult>>,
        cutoff_ms: u64,
    ) -> Self {
        let now = Instant::now();
        Self {
            venue,
            primary,
            commands,
            results,
            cutoff_ms,
            socket: None,
            dial: None,
            start_by: now,
            next_dial: now,
            subscribed: false,
            fills_ready: false,
            last_rx: now,
            next_ping: now,
            reading: false,
            next_read: now,
            ready_once: false,
            nonce: None,
            auth: None,
            book_resub: None,
        }
    }

    fn host(&self) -> &'static str {
        match self.venue {
            Venue::Hyperliquid => crate::hyperliquid::TESTNET_HOST,
            Venue::Lighter => lighter::HOST,
        }
    }

    fn path(&self) -> &'static str {
        match self.venue {
            Venue::Hyperliquid => crate::hyperliquid::PATH,
            Venue::Lighter => lighter::PATH,
        }
    }
}

impl Session {
    fn drive(&mut self, duration: Duration) -> Result<()> {
        let expires = Instant::now()
            .checked_add(duration)
            .context("duration out of range")?;
        let mut next_tick = Instant::now();
        let mut stopped = None;
        let mut verified_stop = None;
        loop {
            let now = Instant::now();
            if stopped.is_none() && (now >= expires || STOP.load(Ordering::Relaxed)) {
                self.input(None, None, Input::Stop)?;
                stopped = Some(now);
                for feed in &mut self.feeds {
                    feed.next_read = now;
                }
            }
            if let Some(stopped) = stopped {
                let summary = self.engine.end();
                let mut finish =
                    summary.clean || self.engine.observe || now.duration_since(stopped) >= RECOVERY;
                if summary.clean
                    && self.hyperliquid.is_some()
                    && self.engine.config.requires_hyperliquid_protection()
                    && !self.disarmed
                {
                    let verified_at = *verified_stop.get_or_insert(now);
                    let elapsed = now.duration_since(verified_at);
                    if elapsed < Duration::from_secs(12) {
                        self.protection(now, true)?;
                        finish = false;
                    } else {
                        self.journal.append(None, Event::MakerNotice {
                            venue: Venue::Hyperliquid,
                            reason: "clean orders confirmed, but scheduled cancel removal was not acknowledged".into(),
                        })?;
                    }
                }
                if finish {
                    let clean = summary.clean;
                    for socket in self.feeds.iter_mut().filter_map(|feed| feed.socket.take()) {
                        socket.close(
                            &mut self.reactor,
                            &mut self.journal,
                            "maker_session_finished".into(),
                        )?;
                    }
                    self.journal.append(None, Event::MakerEnd { summary })?;
                    self.journal.sync()?;
                    if self.engine.observe {
                        ensure!(
                            clean,
                            "observe session ended without books and metadata from each venue"
                        );
                    } else {
                        ensure!(
                            clean,
                            "maker session ended with unresolved orders, exposure, or account state"
                        );
                    }
                    return Ok(());
                }
            }
            if now >= next_tick {
                self.input(None, None, Input::Tick)?;
                next_tick = now + TICK;
            }
            self.replies
                .retain(|_, (at, _)| now.saturating_duration_since(*at) < Duration::from_secs(30));

            for index in 0..self.feeds.len() {
                if let Err(error) = self.feed(index, now) {
                    self.disconnect(index, error)?;
                }
            }
            if stopped.is_none()
                && self.hyperliquid.is_some()
                && self.engine.config.requires_hyperliquid_protection()
            {
                self.protection(now, false)?;
            }
            let mut sockets = self
                .feeds
                .iter_mut()
                .filter_map(|feed| feed.socket.as_mut())
                .collect::<Vec<_>>();
            self.reactor
                .service_many(&mut sockets, next_tick, &mut self.journal)?;
        }
    }

    fn feed(&mut self, index: usize, now: Instant) -> Result<()> {
        self.connect(index, now)?;
        if self.feeds[index].socket.is_none() {
            self.read_results(index, now)?;
            return Ok(());
        }
        if !self.feeds[index].subscribed {
            let f = &mut self.feeds[index];
            let (host, path) = (f.host(), f.path());
            if !f.socket.as_mut().expect("connected").progress_setup(
                host,
                path,
                &mut self.journal,
                f.start_by,
            )? {
                return Ok(());
            }
            self.bootstrap(index, now)?;
        }
        for _ in 0..super::MESSAGE_BUDGET {
            let Some(incoming) = self.feeds[index]
                .socket
                .as_mut()
                .expect("connected")
                .next_message()?
            else {
                break;
            };
            self.feeds[index].last_rx = now;
            self.received(index, incoming)?;
        }
        let f = &mut self.feeds[index];
        let socket = f.socket.as_mut().expect("connected");
        ensure!(
            !socket.eof() && now.saturating_duration_since(f.last_rx) < QUIET,
            "maker feed ended or went silent"
        );
        if now >= f.next_ping {
            let ping = match f.venue {
                Venue::Hyperliquid => r#"{"method":"ping"}"#,
                Venue::Lighter => r#"{"type":"ping"}"#,
            };
            socket.send_private(ping.into(), None)?;
            f.next_ping = now + PING;
        }
        if f.auth
            .as_ref()
            .is_some_and(|(deadline, _)| now >= *deadline)
        {
            anyhow::bail!("refreshing Lighter authenticated feed");
        }
        self.read_results(index, now)?;
        self.schedule_read(index, now)?;
        Ok(())
    }

    fn connect(&mut self, index: usize, now: Instant) -> Result<()> {
        let f = &mut self.feeds[index];
        if f.socket.is_some() {
            return Ok(());
        }
        if let Some(dial) = &f.dial {
            let result = match dial.try_recv() {
                Ok(result) => result,
                Err(mpsc::TryRecvError::Empty) => return Ok(()),
                Err(error) => Err(error.into()),
            };
            f.dial = None;
            let stream = result?;
            let id = self.next_connection;
            self.next_connection += 1;
            f.socket = Some(Connection::begin(
                stream,
                &mut self.reactor,
                id,
                f.host(),
                super::public_tls_config(),
                &mut self.journal,
            )?);
            f.start_by = now + HELLO;
            return Ok(());
        }
        if now < f.next_dial {
            return Ok(());
        }
        let (result, dial) = mpsc::sync_channel(1);
        let host = f.host();
        let waker = self.reactor.waker();
        thread::Builder::new()
            .name(format!("{host}-connect"))
            .spawn(move || {
                let outcome = (|| -> Result<TcpStream> {
                    for address in super::resolve(host)?.into_iter().take(4) {
                        if let Ok(socket) = std::net::TcpStream::connect_timeout(&address, RETRY) {
                            socket.set_nonblocking(true)?;
                            return Ok(TcpStream::from_std(socket));
                        }
                    }
                    anyhow::bail!("no exchange address connected")
                })();
                let _ = result.send(outcome);
                let _ = waker.wake();
            })?;
        f.dial = Some(dial);
        Ok(())
    }

    fn bootstrap(&mut self, index: usize, now: Instant) -> Result<()> {
        let f = &mut self.feeds[index];
        let socket = f.socket.as_mut().expect("connected");
        match f.venue {
            Venue::Hyperliquid => {
                for kind in ["l2Book", "trades"] {
                    socket.send_private(
                        hyperliquid::subscribe(kind, "coin", &self.engine.config.coin),
                        None,
                    )?;
                }
                if let Some(account) = self.accounts.hyperliquid.as_deref().filter(|_| f.primary) {
                    for kind in ["orderUpdates", "userFills"] {
                        socket.send_private(hyperliquid::subscribe(kind, "user", account), None)?;
                    }
                }
            }
            Venue::Lighter => {
                let market = self
                    .engine
                    .config
                    .lighter_market
                    .expect("configured market");
                for kind in ["order_book", "trade"] {
                    socket
                        .send_private(lighter::subscribe(format!("{kind}/{market}"), None), None)?;
                }
                if let Some(account) = self.accounts.lighter.filter(|_| f.primary) {
                    socket.send_private(
                        lighter::subscribe(format!("account_all_trades/{account}"), None),
                        None,
                    )?;
                    if let Some(signer) = &self.lighter {
                        let token = signer.auth()?;
                        socket.send_private(
                            lighter::subscribe(
                                format!("account_orders/{market}/{account}"),
                                Some(&token),
                            ),
                            None,
                        )?;
                        socket.send_private(
                            lighter::subscribe(format!("account_all_positions/{account}"), None),
                            None,
                        )?;
                        f.auth = Some((now + Duration::from_secs(540), token));
                    }
                }
            }
        }
        f.subscribed = true;
        f.next_ping = now + PING;
        f.last_rx = now;
        if f.primary {
            let venue = f.venue;
            let generation = socket.id();
            self.input(
                Some(generation),
                None,
                Input::Connected { venue, generation },
            )?;
        }
        Ok(())
    }

    fn received(&mut self, index: usize, incoming: Incoming) -> Result<()> {
        let f = &mut self.feeds[index];
        let socket = f.socket.as_mut().expect("connected");
        let id = socket.id();
        let venue = f.venue;
        let message: Value = match incoming.message {
            Message::Text(text) => {
                serde_json::from_str(&text).map_err(|_| anyhow::anyhow!("invalid exchange JSON"))?
            }
            Message::Ping(bytes) => return socket.send(&mut self.journal, Message::Pong(bytes)),
            Message::Close(_) => anyhow::bail!("exchange closed WebSocket"),
            _ => return Ok(()),
        };
        ensure!(
            message["channel"] != "error"
                && message["error"].is_null()
                && message["type"] != "error",
            "exchange declined a WebSocket request"
        );
        if f.primary {
            f.fills_ready |= match venue {
                Venue::Hyperliquid => {
                    message["channel"] == "userFills"
                        && self
                            .accounts
                            .hyperliquid
                            .as_deref()
                            .zip(message["data"]["user"].as_str())
                            .is_some_and(|(account, user)| account.eq_ignore_ascii_case(user))
                }
                Venue::Lighter => {
                    message["type"] == "subscribed/account_all_trades"
                        && self.accounts.lighter.is_some_and(|account| {
                            message["channel"] == format!("account_all_trades:{account}")
                        })
                }
            };
        }
        let mut inputs = match venue {
            Venue::Hyperliquid => hyperliquid::observations(
                &message,
                &self.engine.config.coin,
                self.accounts.hyperliquid.as_deref().filter(|_| f.primary),
                self.seed,
                id,
            )?,
            Venue::Lighter => lighter::observations(
                &message,
                self.engine
                    .config
                    .lighter_market
                    .expect("configured market"),
                self.accounts.lighter.filter(|_| f.primary),
                id,
            )?,
        };
        if venue == Venue::Lighter && message["type"] == "ping" {
            socket.send_private(r#"{"type":"pong"}"#.into(), None)?;
        }
        let mut protection = None;
        if f.primary
            && venue == Venue::Hyperliquid
            && message["channel"] == "post"
            && let Some(request_id) = message["data"]["id"].as_u64()
        {
            if self
                .protect
                .as_ref()
                .is_some_and(|pending| pending.id == request_id && pending.generation == id)
            {
                let pending = self.protect.take().expect("matched protection request");
                let response = &message["data"]["response"];
                if response["type"] == "action" && response["payload"]["status"] == "ok" {
                    self.disarmed = pending.expires_ms.is_none();
                    protection = Some(Input::Protection {
                        expires_ms: pending.expires_ms,
                    });
                } else {
                    self.journal.append(
                        Some(id),
                        Event::MakerNotice {
                            venue,
                            reason: "Hyperliquid declined scheduled cancellation".into(),
                        },
                    )?;
                }
            } else if let Some((_, intent)) = self.replies.remove(&request_id)
                && let Some(input) = hyperliquid::order_reply(&message, &intent)?
            {
                inputs.push(input);
            }
        }
        inputs.extend(protection);
        let primary = f.primary;
        let was_book = self.engine.state.venue(venue).book.has_snapshot();
        let mut saw_delta = false;
        let mut saw_snapshot = false;
        let compare_feeds = self.feeds.len()
            > if self.engine.config.mode == Mode::Hedged {
                2
            } else {
                1
            };
        for input in inputs {
            if let Input::Fill { ref fill, .. } = input
                && fill.venue_ms < self.start_wall
            {
                continue;
            }
            if let Input::Book {
                previous, sequence, ..
            } = &input
            {
                if compare_feeds {
                    self.input(
                        Some(id),
                        Some((incoming.wall, incoming.timing)),
                        Input::PublicBook {
                            venue,
                            sequence: *sequence,
                            connection: id,
                        },
                    )?;
                }
                saw_delta |= previous.is_some();
                saw_snapshot |= previous.is_none();
            }
            if !primary && !matches!(input, Input::PublicTrade { .. }) {
                continue;
            }
            self.input(Some(id), Some((incoming.wall, incoming.timing)), input)?;
        }
        if venue == Venue::Lighter && primary {
            if saw_snapshot {
                self.feeds[index].book_resub = None;
            }
            if saw_delta
                && !self.engine.state.venue(venue).book.has_snapshot()
                && (was_book || self.feeds[index].book_resub.is_none())
            {
                let market = self
                    .engine
                    .config
                    .lighter_market
                    .expect("configured market");
                let f = &mut self.feeds[index];
                let socket = f.socket.as_mut().expect("connected");
                let unsubscribe = serde_json::json!({"type":"unsubscribe","channel":format!("order_book/{market}")});
                socket.send_private(unsubscribe.to_string(), None)?;
                socket.send_private(
                    lighter::subscribe(format!("order_book/{market}"), None),
                    None,
                )?;
                f.book_resub = Some(Instant::now());
            }
            ensure!(
                self.feeds[index]
                    .book_resub
                    .is_none_or(|at| at.elapsed() < HELLO),
                "Lighter resnapshot timed out"
            );
        }
        Ok(())
    }

    fn protection(&mut self, now: Instant, remove: bool) -> Result<()> {
        if let Some(pending) = &self.protect {
            if now.saturating_duration_since(pending.sent) < Duration::from_secs(5) {
                return Ok(());
            }
            self.protect = None;
        }
        if !remove
            && (now < self.next_protection
                || (self.engine.state.protection_ms.is_none()
                    && !self
                        .engine
                        .state
                        .account_verified(Venue::Hyperliquid, &self.engine.config)))
        {
            return Ok(());
        }
        let Some(socket) = self
            .feeds
            .iter_mut()
            .find(|f| f.primary && f.venue == Venue::Hyperliquid && f.ready_once)
            .and_then(|f| f.socket.as_mut())
        else {
            return Ok(());
        };
        let signer = self
            .hyperliquid
            .as_mut()
            .expect("Hyperliquid protection signer");
        let request_id = self.next_protection_id;
        self.next_protection_id += 1;
        let expires_unix_ms = (!remove)
            .then(|| unix_ms().map(|now| now + 60_000))
            .transpose()?;
        let signed = signer.schedule_cancel(expires_unix_ms)?;
        self.journal.append(
            Some(socket.id()),
            Event::MakerProtectionPrepared {
                request_id,
                nonce: signed.nonce,
                expires_unix_ms,
            },
        )?;
        self.journal.sync()?;
        socket.send_private(
            crate::hyperliquid::trading::signed_post(request_id, &signed.wire_payload),
            None,
        )?;
        self.protect = Some(ProtectionRequest {
            id: request_id,
            generation: socket.id(),
            sent: now,
            expires_ms: (!remove)
                .then(|| self.journal.latency().elapsed_ns(now) / 1_000_000 + 60_000),
        });
        self.next_protection = now + Duration::from_secs(15);
        Ok(())
    }

    fn disconnect(&mut self, index: usize, error: anyhow::Error) -> Result<()> {
        let f = &mut self.feeds[index];
        let venue = f.venue;
        let reason = format!("{error:#}");
        let id = f.socket.as_ref().map(Connection::id);
        if let Some(socket) = f.socket.take() {
            socket.close(&mut self.reactor, &mut self.journal, reason.clone())?;
        }
        if f.primary && venue == Venue::Hyperliquid {
            self.protect = None;
            self.next_protection = Instant::now();
        }
        f.subscribed = false;
        f.fills_ready = false;
        f.next_read = Instant::now() + RETRY;
        f.ready_once = false;
        f.nonce = None;
        f.auth = None;
        f.book_resub = None;
        f.next_dial = Instant::now() + RETRY;
        if f.primary {
            self.input(id, None, Input::Disconnected { venue, reason })?;
        } else {
            self.journal
                .append(id, Event::MakerNotice { venue, reason })?;
        }
        Ok(())
    }

    fn schedule_read(&mut self, index: usize, now: Instant) -> Result<()> {
        let f = &mut self.feeds[index];
        let Some(commands) = &f.commands else {
            return Ok(());
        };
        if f.reading || now < f.next_read || (f.ready_once && self.engine.observe) {
            return Ok(());
        }
        let generation = f.socket.as_ref().expect("connected").id();
        let requested_ms = self.journal.latency().elapsed_ns(now) / 1_000_000;
        let targets = self
            .engine
            .state
            .active_orders()
            .filter(|(_, order)| order.venue == f.venue)
            .map(|(client, order)| readback::Target {
                client,
                oid: order.oid,
                reported: order.reported,
                check: order.cancel_ms.is_some()
                    || order.generation != generation
                    || order.role == crate::maker::OrderRole::Hedge
                    || requested_ms.saturating_sub(order.sent_ms)
                        >= self.engine.config.action_timeout_ms,

                abandonable: order.oid.is_none()
                    && order.reported.is_zero()
                    && requested_ms.saturating_sub(order.sent_ms)
                        >= if f.venue == Venue::Lighter {
                            lighter::ORDER_LIFETIME_MS + 15_000
                        } else {
                            15_000
                        },
            })
            .collect();
        commands.enqueue(readback::Request {
            generation,
            requested_ms,
            cutoff_ms: f.cutoff_ms,
            start_ms: self.start_wall,
            first: !f.ready_once,
            auth: f.auth.as_ref().map(|(_, token)| token.to_owned()),
            targets,
        })?;
        f.reading = true;
        f.next_read = now
            + match f.venue {
                Venue::Hyperliquid => Duration::from_secs(2),
                Venue::Lighter => Duration::from_secs(15),
            };
        Ok(())
    }

    fn read_results(&mut self, index: usize, now: Instant) -> Result<()> {
        let f = &mut self.feeds[index];
        let Some(results) = &f.results else {
            return Ok(());
        };
        let (request, result) = match results.try_recv() {
            Ok(value) => value.value,
            Err(mpsc::TryRecvError::Empty) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        f.reading = false;
        if f.socket
            .as_ref()
            .is_none_or(|socket| socket.id() != request.generation)
        {
            return Ok(());
        }
        let snapshot = match result {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.journal.append(
                    Some(request.generation),
                    Event::MakerNotice {
                        venue: f.venue,
                        reason: format!("{error:#}"),
                    },
                )?;
                return Ok(());
            }
        };
        f.ready_once = true;
        if let Some(nonce) = snapshot.nonce {
            f.nonce = Some(nonce);
        }
        // Use the request start with overlap, not the response time.
        f.cutoff_ms = self
            .start_wall
            .saturating_add(request.requested_ms)
            .saturating_sub(3_000)
            .max(self.start_wall);
        f.next_read = f.next_read.max(now);
        let account_ready = f.fills_ready || self.engine.observe;
        for input in snapshot.inputs {
            if !account_ready && matches!(input, Input::Account { .. }) {
                continue;
            }
            self.input(Some(request.generation), None, input)?;
        }
        Ok(())
    }

    fn input(
        &mut self,
        connection: Option<u64>,
        rx: Option<(SystemTime, ReceiveTiming)>,
        input: Input,
    ) -> Result<()> {
        let elapsed_ms = self.journal.latency().elapsed_ns(Instant::now()) / 1_000_000;
        let (wall, timing) = rx.map_or_else(
            || (SystemTime::now(), None),
            |(wall, timing)| (wall, Some(timing)),
        );
        let seq = self.journal.append_sequenced(
            wall,
            connection,
            timing,
            Event::MakerInput {
                elapsed_ms,
                input: input.clone(),
            },
        )?;
        let intents = self.engine.apply(seq, elapsed_ms, input)?;
        let mut failed = Vec::new();
        for intent in intents {
            let at = Instant::now();
            self.journal.append(
                None,
                Event::MakerIntent {
                    intent: intent.clone(),
                },
            )?;
            if let Err(error) = self.dispatch(&intent, at) {
                self.journal.append(
                    None,
                    Event::MakerNotice {
                        venue: intent.venue,
                        reason: format!("{error:#}"),
                    },
                )?;
                if matches!(intent.action, Action::Place { .. }) {
                    failed.push(intent);
                }
            }
        }
        for intent in failed {
            self.input(
                None,
                None,
                Input::Order {
                    venue: intent.venue,
                    client: Some(intent.client),
                    oid: None,
                    status: Status::Terminal,
                    filled: Decimal::ZERO,
                },
            )?;
        }
        Ok(())
    }

    fn dispatch(&mut self, intent: &Intent, enqueued_at: Instant) -> Result<()> {
        let f = self
            .feeds
            .iter_mut()
            .find(|f| f.venue == intent.venue && f.primary)
            .context("missing intent venue")?;
        let socket = f.socket.as_mut().context("intent venue is disconnected")?;
        let instrument = self
            .engine
            .state
            .venue(intent.venue)
            .instrument
            .as_ref()
            .context("missing instrument")?;
        let (wire, nonce, tx_hash) = match intent.venue {
            Venue::Hyperliquid => {
                let signer = self
                    .hyperliquid
                    .as_mut()
                    .context("missing Hyperliquid signer")?;
                let (wire, nonce) = hyperliquid::sign(signer, intent, instrument.asset, self.seed)?;
                (wire, nonce, None)
            }
            Venue::Lighter => {
                let signer = self.lighter.as_ref().context("missing Lighter signer")?;
                let nonce = f.nonce.context("Lighter nonce is unverified")?;
                let (wire, hash) = signer.sign(intent, instrument, nonce)?;
                (wire, nonce, Some(hash))
            }
        };
        self.journal.append(
            Some(socket.id()),
            Event::MakerPrepared {
                intent_id: intent.id,
                nonce,
                tx_hash,
            },
        )?;
        self.journal.sync()?;
        socket.send_private(
            wire,
            Some(TxTicket {
                intent_id: intent.id,
                enqueued_at,
            }),
        )?;
        match intent.venue {
            Venue::Hyperliquid => {
                self.replies
                    .insert(intent.id, (enqueued_at, intent.clone()));
            }
            Venue::Lighter => {
                f.nonce = nonce.checked_add(1);
            }
        }
        Ok(())
    }
}

pub(crate) fn number(value: &Value, field: &str) -> Result<u64> {
    value[field]
        .as_u64()
        .or_else(|| value[field].as_str()?.parse().ok())
        .with_context(|| format!("invalid integer field {field}"))
}

pub(crate) fn decimal(value: &Value, field: &str) -> Result<Decimal> {
    value[field]
        .as_str()
        .and_then(|text| text.parse().ok())
        .or_else(|| value[field].as_number()?.to_string().parse().ok())
        .with_context(|| format!("invalid decimal field {field}"))
}

fn unix_ms() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}
