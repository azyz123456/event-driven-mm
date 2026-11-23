use std::collections::BTreeMap;

// SplitMix64 keeps the sampler stable across dependency and platform changes.
pub fn jitter(seed: u64, key: u64, stage: u64, span: u64) -> u64 {
    let mut bits = seed ^ key.wrapping_mul(0x9e3779b97f4a7c15) ^ stage;
    bits = (bits ^ (bits >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    bits = (bits ^ (bits >> 27)).wrapping_mul(0x94d049bb133111eb);
    (bits ^ (bits >> 31)) % (span + 1)
}

pub struct Clock<T> {
    pending: BTreeMap<(u64, u64), T>,
    ordinal: u64,
}

impl<T> Default for Clock<T> {
    fn default() -> Self {
        Self {
            pending: BTreeMap::new(),
            ordinal: 0,
        }
    }
}

impl<T> Clock<T> {
    pub fn push(&mut self, at_us: u64, event: T) {
        self.pending.insert((at_us, self.ordinal), event);
        self.ordinal += 1;
    }

    pub fn before(&mut self, exclusive_us: u64) -> Option<(u64, T)> {
        if self.pending.first_key_value()?.0.0 >= exclusive_us {
            return None;
        }
        self.pending.pop_first().map(|((at, _), event)| (at, event))
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }
}
