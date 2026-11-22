mod book;
mod engine;
mod event;
mod hyperliquid;
mod intent;
mod journal;
mod network;
mod order_test;
mod signing;
mod websocket;

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::sync_channel;
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
use clap::{Parser, Subcommand, builder::NonEmptyStringValueParser};
use rust_decimal::Decimal;

use crate::journal::{JournalReader, JournalWriter};

#[derive(Parser)]
#[command(about = "Record a Hyperliquid book and replay its midpoints")]
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
        } => {
            let file = create_journal(&log)?;
            let reader = open_journal(&log)?;
            let stop = Arc::new(AtomicBool::new(false));
            let worker_stop = Arc::clone(&stop);
            let worker = thread::Builder::new()
                .name("hyperliquid-network".into())
                .spawn(move || {
                    network::capture(
                        JournalWriter::new(file),
                        &coin,
                        Duration::from_secs(duration_secs.get()),
                        &worker_stop,
                    )
                })
                .context("start network worker")?;

            let result = engine::run(reader, || worker.is_finished(), io::stdout().lock());
            if result.is_err() {
                stop.store(true, Ordering::Relaxed);
            }
            let worker_result = worker
                .join()
                .map_err(|_| anyhow!("network worker panicked"))?;
            result.and(worker_result)
        }
        Command::OrderTest {
            account,
            coin,
            price,
            size,
            max_notional,
            log,
        } => {
            let config = order_test::Config::new(account, coin, price, size, max_notional)?;
            run_order_test(config, &log)
        }
        Command::Replay { log } => engine::run(open_journal(&log)?, || true, io::stdout().lock()),
    }
}

fn run_order_test(config: order_test::Config, log: &std::path::Path) -> Result<()> {
    let signer = signing::Signer::from_env(&config.account)?;
    let file = create_journal(log)?;
    let reader = open_journal(log)?;
    let cloid = config.cloid.clone();
    eprintln!("Hyperliquid testnet client order ID {cloid}");
    let (sender, receiver) = sync_channel(8);
    let worker = thread::Builder::new()
        .name("hyperliquid-network".into())
        .spawn(move || network::trading::run(JournalWriter::new(file), config, signer, receiver))
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

fn open_journal(path: &std::path::Path) -> Result<JournalReader> {
    let file = File::open(path).with_context(|| format!("open journal {}", path.display()))?;
    Ok(JournalReader::new(file))
}
