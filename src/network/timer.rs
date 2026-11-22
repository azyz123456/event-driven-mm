use std::time::{Duration, Instant};

use crate::event::{Event, TimerKind};
use crate::latency::{Latency, nanos};

pub struct Periodic {
    kind: TimerKind,
    period: Duration,
    next: Instant,
}

impl Periodic {
    pub fn new(kind: TimerKind, period: Duration, start: Instant) -> Self {
        Self {
            kind,
            period,
            next: start + period,
        }
    }

    pub fn deadline(&self) -> Instant {
        self.next
    }

    pub fn fire(&mut self, now: Instant, clock: &Latency) -> Option<Event> {
        let late = nanos(now.checked_duration_since(self.next)?);
        let period = nanos(self.period);
        let scheduled = now - Duration::from_nanos(late % period);
        self.next = scheduled + self.period;
        Some(Event::Timer {
            kind: self.kind,
            scheduled_elapsed_ns: clock.elapsed_ns(scheduled),
            fired_elapsed_ns: clock.elapsed_ns(now),
            missed_intervals: late / period,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coalesces_late_intervals_without_drifting() {
        let clock = Latency::new();
        let start = Instant::now();
        let mut timer = Periodic::new(TimerKind::StrategySecond, Duration::from_secs(1), start);
        assert!(
            timer
                .fire(start + Duration::from_millis(999), &clock)
                .is_none()
        );
        let event = timer
            .fire(start + Duration::from_millis(3400), &clock)
            .unwrap();
        assert!(
            matches!(event, Event::Timer { kind: TimerKind::StrategySecond, scheduled_elapsed_ns, fired_elapsed_ns, missed_intervals: 2 } if fired_elapsed_ns - scheduled_elapsed_ns == 400_000_000)
        );
        assert_eq!(timer.deadline(), start + Duration::from_secs(4));
    }
}
