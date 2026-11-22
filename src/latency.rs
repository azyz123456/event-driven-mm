use std::io::{self, Write};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::event::ReceiveTiming;

pub struct Latency {
    origin: Instant,
    histograms: Mutex<[Histogram; 5]>,
}

#[derive(Clone, Copy)]
pub enum Measure {
    KernelToRead,
    ReadToDecode,
    AppendToConsume,
    IntentToWrite,
    JournalSync,
}

const MEASURES: [(Measure, &str); 5] = [
    (Measure::KernelToRead, "kernel_batch_to_read"),
    (Measure::ReadToDecode, "read_to_decode"),
    (Measure::AppendToConsume, "append_to_consume"),
    (Measure::IntentToWrite, "intent_to_first_write"),
    (Measure::JournalSync, "journal_sync"),
];

impl Latency {
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
            histograms: Mutex::new(std::array::from_fn(|_| Histogram::new())),
        }
    }

    pub fn elapsed_ns(&self, at: Instant) -> u64 {
        nanos(at.saturating_duration_since(self.origin))
    }

    pub fn observe(&self, measure: Measure, value: Option<u64>) {
        self.histograms.lock().unwrap_or_else(|e| e.into_inner())[measure as usize].add(value);
    }

    pub fn receive(&self, timing: ReceiveTiming) {
        let kernel_delay = timing
            .read_unix_ns
            .zip(timing.kernel_rx_unix_ns)
            .and_then(|(read, kernel)| read.checked_sub(kernel));
        self.observe(Measure::KernelToRead, kernel_delay);
        self.observe(
            Measure::ReadToDecode,
            timing
                .decoded_elapsed_ns
                .checked_sub(timing.read_elapsed_ns),
        );
    }

    pub fn write_summary(&self, mut output: impl Write) -> io::Result<()> {
        let histograms = self.histograms.lock().unwrap_or_else(|e| e.into_inner());
        for (measure, label) in MEASURES {
            let histogram = &histograms[measure as usize];
            write!(
                output,
                "latency {label} samples={} unavailable={}",
                histogram.total, histogram.unavailable
            )?;
            if histogram.total != 0 {
                write!(
                    output,
                    " p50_le_ns={} p95_le_ns={} p99_le_ns={}",
                    histogram.percentile(50),
                    histogram.percentile(95),
                    histogram.percentile(99)
                )?;
            }
            writeln!(output)?;
        }
        Ok(())
    }
}

pub fn nanos(duration: Duration) -> u64 {
    duration.as_nanos().try_into().unwrap_or(u64::MAX)
}

pub fn unix_ns(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()?
        .as_nanos()
        .try_into()
        .ok()
}

struct Histogram {
    bins: [u64; 65],
    total: u64,
    unavailable: u64,
}

impl Histogram {
    fn new() -> Self {
        Self {
            bins: [0; 65],
            total: 0,
            unavailable: 0,
        }
    }

    fn add(&mut self, value: Option<u64>) {
        if let Some(ns) = value {
            self.bins[(64 - ns.leading_zeros()) as usize] += 1;
            self.total += 1;
        } else {
            self.unavailable += 1;
        }
    }

    fn percentile(&self, percent: u64) -> u128 {
        let rank = (u128::from(self.total) * u128::from(percent)).div_ceil(100);
        let mut count = 0;
        for (i, &bin) in self.bins.iter().enumerate() {
            count += u128::from(bin);
            if count >= rank {
                return (1_u128 << i) - 1;
            }
        }
        u128::from(u64::MAX)
    }
}
