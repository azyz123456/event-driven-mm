mod book;
mod engine;
mod event;
mod hyperliquid;
mod journal;
mod network;
mod websocket;

use std::fs::{File, OpenOptions};
use std::io;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::{Parser, Subcommand, builder::NonEmptyStringValueParser};

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
    /// Read a finished journal and regenerate the midpoint output offline.
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
            let file = OpenOptions::new()
                .append(true)
                .create_new(true)
                .open(&log)
                .with_context(|| format!("create journal {}", log.display()))?;
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
        Command::Replay { log } => engine::run(open_journal(&log)?, || true, io::stdout().lock()),
    }
}

fn open_journal(path: &std::path::Path) -> Result<JournalReader> {
    let file = File::open(path).with_context(|| format!("open journal {}", path.display()))?;
    Ok(JournalReader::new(file))
}
