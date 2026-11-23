mod book;
mod engine;
mod event;
mod hyperliquid;
mod intent;
mod journal;
mod latency;
mod lighter;
mod maker;
mod network;
mod order_test;
mod queue;
mod research_export;
mod signing;
mod sim;
mod websocket;

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
use clap::{Parser, Subcommand, builder::NonEmptyStringValueParser};
use rust_decimal::Decimal;

use crate::latency::Latency;
use crate::network::reactor::Reactor;
use crate::queue::{RecordSource, command_queue, file_queue};

#[derive(Parser)]
#[command(about = "Capture, trade on testnet, and replay venue journals")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Record public market data and follow the journal as it grows.
    Capture {
        #[arg(long, value_parser = NonEmptyStringValueParser::new())]
        coin: String,
        #[arg(long)]
        duration_secs: NonZeroU64,
        #[arg(long)]
        log: PathBuf,
        /// Print latency sample counts and percentiles to stderr.
        #[arg(long)]
        latency: bool,
    },
    /// Place and cancel one post-only order on Hyperliquid testnet.
    OrderTest {
        #[arg(long)]
        account: String,
        #[arg(long)]
        coin: String,
        #[arg(long)]
        price: Decimal,
        #[arg(long)]
        size: Decimal,
        #[arg(long)]
        max_notional: Decimal,
        #[arg(long)]
        log: PathBuf,
        /// Print latency sample counts and percentiles to stderr.
        #[arg(long)]
        latency: bool,
    },
    /// Run a market-making session on testnet or capture public feeds in observe mode.
    Maker {
        #[arg(long)]
        config: PathBuf,
        #[arg(long)]
        duration_secs: NonZeroU64,
        #[arg(long)]
        log: PathBuf,
        #[arg(long)]
        observe: bool,
        #[arg(long)]
        hyperliquid_account: Option<String>,
        #[arg(long)]
        lighter_account: Option<u64>,
        #[arg(long, requires = "lighter_account", conflicts_with = "observe")]
        lighter_key_index: Option<u8>,
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u8).range(1..=5))]
        feed_copies: u8,
    },
    /// Read a maker journal as a report or step through it one record at a time.
    InspectMaker {
        #[arg(long)]
        log: PathBuf,
        #[arg(long)]
        step: bool,
    },
    /// Compare execution policies on a finished hedged maker journal.
    Simulate {
        #[arg(long)]
        log: PathBuf,
        #[arg(long)]
        config: PathBuf,
        #[arg(long, value_enum)]
        mode: sim::Flow,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long)]
        trace: Option<PathBuf>,
        #[arg(long)]
        quote_signal: Option<PathBuf>,
    },
    /// Export point-in-time books, trades, and quote lifecycle observations as JSONL.
    ResearchExport {
        #[arg(long)]
        log: PathBuf,
    },
    /// Replay a finished journal offline and verify any recorded order intents.
    Replay {
        #[arg(long)]
        log: PathBuf,
    },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Capture {
            coin,
            duration_secs,
            log,
            latency,
        } => {
            let file = create_journal(&log)?;
            let clock = Arc::new(Latency::new());
            let (journal, reader) = file_queue(file, read_journal(&log)?, Arc::clone(&clock));
            let reactor = Reactor::new()?;
            let waker = reactor.waker();
            let stop = Arc::new(AtomicBool::new(false));
            let worker_stop = Arc::clone(&stop);
            let worker = thread::Builder::new()
                .name("hyperliquid-network".into())
                .spawn(move || {
                    network::capture(
                        reactor,
                        journal,
                        &coin,
                        Duration::from_secs(duration_secs.get()),
                        &worker_stop,
                    )
                })
                .context("start network worker")?;

            let result = engine::run(reader, || worker.is_finished(), io::stdout().lock());
            if result.is_err() {
                stop.store(true, Ordering::Relaxed);
                let _ = waker.wake();
            }
            let worker_result = worker
                .join()
                .map_err(|_| anyhow!("network worker panicked"))?;
            if latency {
                clock.write_summary(io::stderr().lock())?;
            }
            result.and(worker_result)
        }
        Command::OrderTest {
            account,
            coin,
            price,
            size,
            max_notional,
            log,
            latency,
        } => {
            let config = order_test::Config::new(account, coin, price, size, max_notional)?;
            run_order_test(config, &log, latency)
        }
        Command::Maker {
            config,
            duration_secs,
            log,
            observe,
            hyperliquid_account,
            lighter_account,
            lighter_key_index,
            feed_copies,
        } => {
            let config: maker::Config = serde_json::from_reader(
                File::open(&config)
                    .with_context(|| format!("open maker config {}", config.display()))?,
            )?;
            let mut bytes = [0; 8];
            getrandom::fill(&mut bytes).map_err(|error| anyhow!("seed maker session: {error}"))?;
            let seed = (u64::from_le_bytes(bytes) % ((1 << 26) - 1)) + 1;
            let engine = maker::Engine::new(config, seed, observe)?;
            let clock = Arc::new(Latency::new());
            let (journal, _) = file_queue(create_journal(&log)?, read_journal(&log)?, clock);
            install_maker_interrupt()?;
            network::maker::run(
                engine,
                network::maker::Account {
                    hyperliquid: hyperliquid_account,
                    lighter: lighter_account,
                    key_index: lighter_key_index,
                },
                Duration::from_secs(duration_secs.get()),
                feed_copies,
                Reactor::new()?,
                journal,
            )
        }
        Command::InspectMaker { log, step } => maker::diagnostics::inspect(
            RecordSource::replay(read_journal(&log)?),
            step.then(|| io::stdin().lock()),
            io::stdout().lock(),
        ),
        Command::Simulate {
            log,
            config,
            mode,
            seed,
            trace,
            quote_signal,
        } => {
            let trace: Box<dyn Write> = match trace {
                Some(path) => Box::new(io::BufWriter::new(create_journal(&path)?)),
                None => Box::new(io::sink()),
            };
            sim::run(
                &log,
                &config,
                mode,
                seed,
                quote_signal.as_deref(),
                io::stdout().lock(),
                trace,
            )
        }
        Command::ResearchExport { log } => research_export::run(
            RecordSource::replay(read_journal(&log)?),
            io::stdout().lock(),
        ),
        Command::Replay { log } => engine::run(
            RecordSource::replay(read_journal(&log)?),
            || true,
            io::stdout().lock(),
        ),
    }
}

fn run_order_test(config: order_test::Config, log: &std::path::Path, latency: bool) -> Result<()> {
    let signer = signing::Signer::from_env(&config.account)?;
    let file = create_journal(log)?;
    let clock = Arc::new(Latency::new());
    let (journal, reader) = file_queue(file, read_journal(log)?, Arc::clone(&clock));
    let reactor = Reactor::new()?;
    let cloid = config.cloid.clone();
    eprintln!("Hyperliquid testnet client order ID {cloid}");
    let (sender, receiver) = command_queue(8, reactor.waker());
    let worker = thread::Builder::new()
        .name("hyperliquid-network".into())
        .spawn(move || network::trading::run(reactor, journal, config, signer, receiver))
        .context("start testnet network worker")?;

    // A closed stdout must never interrupt order reconciliation.
    let mut output = Vec::new();
    let result = engine::run_order_test(reader, || worker.is_finished(), &mut output, sender);
    let worker_result = worker
        .join()
        .map_err(|_| anyhow!("network worker panicked"))?;
    io::stdout()
        .write_all(&output)
        .context("write order-test output")?;
    if latency {
        clock.write_summary(io::stderr().lock())?;
    }
    worker_result?;
    let outcome = result?;
    ensure!(
        outcome.passed(),
        "order test did not verify a clean cancellation for {cloid}"
    );
    Ok(())
}

fn create_journal(path: &std::path::Path) -> Result<File> {
    OpenOptions::new()
        .append(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create journal {}", path.display()))
}

fn read_journal(path: &std::path::Path) -> Result<File> {
    File::open(path).with_context(|| format!("open journal {}", path.display()))
}

#[cfg(unix)]
fn install_maker_interrupt() -> Result<()> {
    extern "C" fn stop(_: libc::c_int) {
        network::maker::STOP.store(true, Ordering::Relaxed);
    }
    // The signal handler only sets a lock-free atomic flag.
    for signal in [libc::SIGINT, libc::SIGTERM] {
        let previous = unsafe { libc::signal(signal, stop as *const () as libc::sighandler_t) };
        ensure!(
            previous != libc::SIG_ERR,
            "could not install maker shutdown signal handler"
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn install_maker_interrupt() -> Result<()> {
    Ok(())
}
