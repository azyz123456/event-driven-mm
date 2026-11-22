use std::collections::VecDeque;
use std::io::Write;
use std::sync::mpsc::SyncSender;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, ensure};

use crate::book::Book;
use crate::event::{Event, FORMAT_VERSION, ORDER_FORMAT_VERSION, Record};
use crate::hyperliquid::TESTNET_ENDPOINT;
use crate::intent::Intent;
use crate::journal::JournalReader;
use crate::order_test::{Command, Lifecycle, Outcome};

const FILE_POLL_INTERVAL: Duration = Duration::from_millis(5);

pub fn run(
    reader: JournalReader,
    writer_is_done: impl Fn() -> bool,
    output: impl Write,
) -> Result<()> {
    follow(reader, writer_is_done, output, None).map(drop)
}

pub fn run_order_test(
    reader: JournalReader,
    writer_is_done: impl Fn() -> bool,
    output: impl Write,
    commands: SyncSender<Command>,
) -> Result<Outcome> {
    follow(reader, writer_is_done, output, Some(commands))?
        .context("expected an order-test outcome")
}

fn follow(
    mut reader: JournalReader,
    writer_is_done: impl Fn() -> bool,
    mut output: impl Write,
    commands: Option<SyncSender<Command>>,
) -> Result<Option<Outcome>> {
    let mut session = None;
    loop {
        // If the writer is done before this read, EOF is final. Checking after
        // a read could miss a record appended between that read and the check.
        let done = writer_is_done();
        match reader.next_record()? {
            Some(Record {
                event:
                    Event::SessionStart {
                        version,
                        endpoint,
                        coin,
                        order_test,
                    },
                seq,
                connection_id,
                ..
            }) => {
                ensure!(seq == 0 && connection_id.is_none(), "invalid session start");
                ensure!(!coin.is_empty(), "empty instrument in session start");
                session = Some(match version {
                    FORMAT_VERSION => {
                        ensure!(
                            order_test.is_none() && commands.is_none(),
                            "unexpected order configuration in a capture journal"
                        );
                        Session::Book(Book::new(coin))
                    }
                    ORDER_FORMAT_VERSION => {
                        let config = order_test.context("missing order-test configuration")?;
                        ensure!(
                            endpoint == TESTNET_ENDPOINT && coin == config.coin,
                            "invalid order-test journal endpoint or instrument"
                        );
                        config.validate()?;
                        Session::Orders(Box::new(OrderReplay::new(Lifecycle::new(config))))
                    }
                    _ => anyhow::bail!("unsupported journal format {version}"),
                });
            }
            Some(record) => {
                let seq = record.seq;
                let session = session
                    .as_mut()
                    .context("journal must start with session_start")?;
                session
                    .apply(record, &mut output, commands.as_ref())
                    .with_context(|| format!("apply journal record {seq}"))?;
            }
            None if done => break,
            None => thread::sleep(FILE_POLL_INTERVAL),
        }
    }
    reader.finish()?;
    let session = session.context("journal is missing session_start")?;
    output.flush().context("flush session output")?;
    match session {
        Session::Book(_) => Ok(None),
        Session::Orders(state) => state.finish().map(Some),
    }
}

enum Session {
    Book(Book),
    Orders(Box<OrderReplay>),
}

impl Session {
    fn apply(
        &mut self,
        record: Record,
        output: &mut impl Write,
        commands: Option<&SyncSender<Command>>,
    ) -> Result<()> {
        match self {
            Self::Book(book) => {
                ensure!(
                    !matches!(
                        record.event,
                        Event::OrderInput { .. }
                            | Event::IntentQueued { .. }
                            | Event::ActionPrepared { .. }
                            | Event::OrderOutcome { .. }
                    ),
                    "order record in a capture journal"
                );
                if let Some(mid) = book.update(record)? {
                    writeln!(output, "{},{}", mid.received_at_unix_ms, mid.price)
                        .context("write midpoint output")?;
                }
                Ok(())
            }
            Self::Orders(state) => state.apply(record, output, commands),
        }
    }
}

struct OrderReplay {
    lifecycle: Lifecycle,
    generated: VecDeque<Intent>,
    expected_outcome: Option<Outcome>,
    verified_outcome: Option<Outcome>,
}

impl OrderReplay {
    fn new(lifecycle: Lifecycle) -> Self {
        Self {
            lifecycle,
            generated: VecDeque::new(),
            expected_outcome: None,
            verified_outcome: None,
        }
    }

    fn apply(
        &mut self,
        record: Record,
        output: &mut impl Write,
        commands: Option<&SyncSender<Command>>,
    ) -> Result<()> {
        match record.event {
            Event::OrderInput { elapsed_ms, input } => {
                if let Some(command) = self.lifecycle.update(record.seq, elapsed_ms, input)? {
                    match &command {
                        Command::Trade(intent) => {
                            self.generated.push_back(intent.clone());
                            writeln!(output, "intent {}", serde_json::to_string(intent)?)?;
                        }
                        Command::Finish(outcome) => self.expected_outcome = Some(outcome.clone()),
                        _ => {}
                    }
                    if let Some(commands) = commands {
                        commands
                            .try_send(command)
                            .context("deliver order-test command")?;
                    }
                }
            }
            Event::IntentQueued { intent } => {
                let generated = self.generated.pop_front().with_context(|| {
                    format!(
                        "unexpected recorded intent {} (trigger {})",
                        intent.id, intent.caused_by_seq
                    )
                })?;
                ensure!(
                    generated == intent,
                    "intent mismatch at trigger {}: generated {:?}, recorded {:?}",
                    generated.caused_by_seq,
                    generated,
                    intent
                );
            }
            Event::OrderOutcome { outcome } => {
                ensure!(
                    self.verified_outcome.is_none(),
                    "duplicate order-test outcome"
                );
                ensure!(
                    self.generated.is_empty(),
                    "order-test outcome precedes a recorded intent"
                );
                ensure!(
                    self.expected_outcome.as_ref() == Some(&outcome),
                    "order-test outcome mismatch: generated {:?}, recorded {:?}",
                    self.expected_outcome,
                    outcome
                );
                writeln!(output, "outcome {}", serde_json::to_string(&outcome)?)?;
                self.verified_outcome = Some(outcome);
            }
            _ => {}
        }
        Ok(())
    }

    fn finish(self) -> Result<Outcome> {
        if let Some(missing) = self.generated.front() {
            anyhow::bail!(
                "missing journaled intent {} (trigger {})",
                missing.id,
                missing.caused_by_seq
            );
        }
        self.verified_outcome
            .context("order-test journal has no verified outcome")
    }
}
