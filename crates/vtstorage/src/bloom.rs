#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct StringBloomFilter {
    bits_lo: u64,
    bits_hi: u64,
}

impl StringBloomFilter {
    pub(crate) fn insert(&mut self, value: &str) {
        for bit in bloom_positions(value) {
            self.set(bit);
        }
    }

    pub(crate) fn might_contain(&self, value: &str) -> bool {
        bloom_positions(value)
            .into_iter()
            .all(|bit| self.is_set(bit))
    }

    fn set(&mut self, bit: u8) {
        if bit < 64 {
            self.bits_lo |= 1u64 << bit;
        } else {
            self.bits_hi |= 1u64 << (bit - 64);
        }
    }

    fn is_set(&self, bit: u8) -> bool {
        if bit < 64 {
            (self.bits_lo & (1u64 << bit)) != 0
        } else {
            (self.bits_hi & (1u64 << (bit - 64))) != 0
        }
    }
}

fn bloom_positions(value: &str) -> [u8; 3] {
    let first = stable_hash(value, 0xcbf29ce484222325, 0x100000001b3) % 128;
    let second = stable_hash(value, 0x9e3779b97f4a7c15, 0xbf58476d1ce4e5b9) % 128;
    let third = stable_hash(value, 0x94d049bb133111eb, 0x100000001b3) % 128;
    [first as u8, second as u8, third as u8]
}

fn stable_hash(value: &str, offset_basis: u64, prime: u64) -> u64 {
    let mut hash = offset_basis;
    for byte in value.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(prime);
    }
    hash
}
