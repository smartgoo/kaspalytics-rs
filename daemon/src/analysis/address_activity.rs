use std::collections::{BTreeSet, HashMap, VecDeque};

/// Tracks rolling per-minute address activity over a configurable window.
///
/// Accumulates address spending data per minute and, when a minute completes,
/// flushes the top-N most active addresses (by rolling window tx count) to DB.
pub struct AddressActivityWindow {
    /// Completed minutes in the window: (minute_bucket_ms, {address → (tx_count, total_spent)})
    /// Oldest entry at front; evicted when older than window_duration_ms.
    minute_log: VecDeque<(u64, HashMap<String, (u64, u64)>)>,
    /// Running totals across all minutes currently in the window.
    totals: HashMap<String, (u64, u64)>,
    /// (tx_count, address) ordered ascending — smallest count at front.
    ranked: BTreeSet<(u64, String)>,
    /// Minute bucket (unix ms, minute-aligned) currently being accumulated.
    pending_minute: u64,
    /// Entries for the current (incomplete) minute being accumulated.
    pending: HashMap<String, (u64, u64)>,
    top_n: usize,
    window_duration_ms: u64,
}

impl AddressActivityWindow {
    pub fn new(top_n: usize, window_duration_ms: u64) -> Self {
        Self {
            minute_log: VecDeque::new(),
            totals: HashMap::new(),
            ranked: BTreeSet::new(),
            pending_minute: 0,
            pending: HashMap::new(),
            top_n,
            window_duration_ms,
        }
    }

    /// Ingest address activity for a given minute bucket.
    ///
    /// `entries`: address → (tx_count, total_spent) for this minute.
    ///
    /// Returns `Some((minute_bucket_ms, rows))` when a completed minute is ready
    /// to write to DB — the rows are top-N addresses from that minute.
    pub fn ingest(
        &mut self,
        minute_bucket: u64,
        entries: HashMap<String, (u64, u64)>,
    ) -> Option<(u64, Vec<(String, u64, u64)>)> {
        // First call ever: initialise pending and return nothing.
        if self.pending_minute == 0 {
            self.pending_minute = minute_bucket;
            for (addr, (cnt, spent)) in entries {
                let e = self.pending.entry(addr).or_default();
                e.0 += cnt;
                e.1 += spent;
            }
            return None;
        }

        if minute_bucket != self.pending_minute {
            // A new minute arrived — flush the completed pending minute.
            let completed = self.flush_pending();
            self.pending_minute = minute_bucket;
            self.pending.clear();
            for (addr, (cnt, spent)) in entries {
                let e = self.pending.entry(addr).or_default();
                e.0 += cnt;
                e.1 += spent;
            }
            return completed;
        }

        // Same minute — accumulate.
        for (addr, (cnt, spent)) in entries {
            let e = self.pending.entry(addr).or_default();
            e.0 += cnt;
            e.1 += spent;
        }
        None
    }

    /// Force-flush the current pending minute (e.g. on shutdown).
    pub fn flush(&mut self) -> Option<(u64, Vec<(String, u64, u64)>)> {
        self.flush_pending()
    }

    fn flush_pending(&mut self) -> Option<(u64, Vec<(String, u64, u64)>)> {
        if self.pending.is_empty() {
            return None;
        }

        // 1. Update running totals and ranked set with pending entries.
        for (addr, (cnt, spent)) in &self.pending {
            let entry = self.totals.entry(addr.clone()).or_default();
            let old_count = entry.0;
            entry.0 += cnt;
            entry.1 += spent;
            // Remove old rank position (no-op if addr was not yet ranked).
            self.ranked.remove(&(old_count, addr.clone()));
            self.ranked.insert((entry.0, addr.clone()));
        }

        // 2. Evict minutes that have fallen outside the rolling window.
        let cutoff = self.pending_minute.saturating_sub(self.window_duration_ms);
        while self
            .minute_log
            .front()
            .map(|(m, _)| *m < cutoff)
            .unwrap_or(false)
        {
            if let Some((_, old_minute)) = self.minute_log.pop_front() {
                for (addr, (cnt, spent)) in old_minute {
                    let remove = if let Some(total) = self.totals.get_mut(&addr) {
                        let old_count = total.0;
                        total.0 = total.0.saturating_sub(cnt);
                        total.1 = total.1.saturating_sub(spent);
                        self.ranked.remove(&(old_count, addr.clone()));
                        if total.0 > 0 {
                            self.ranked.insert((total.0, addr.clone()));
                        }
                        total.0 == 0
                    } else {
                        false
                    };
                    if remove {
                        self.totals.remove(&addr);
                    }
                }
            }
        }

        // 3. Push the completed minute into the log.
        self.minute_log
            .push_back((self.pending_minute, self.pending.clone()));

        // 4. Determine top-N addresses by rolling window tx count.
        let top_addresses: std::collections::HashSet<&str> = self
            .ranked
            .iter()
            .rev()
            .take(self.top_n)
            .map(|(_, addr)| addr.as_str())
            .collect();

        // 5. Collect rows for this minute (only top-N addresses).
        let rows: Vec<(String, u64, u64)> = self
            .pending
            .iter()
            .filter(|(addr, _)| top_addresses.contains(addr.as_str()))
            .map(|(addr, (cnt, spent))| (addr.clone(), *cnt, *spent))
            .collect();

        let bucket = self.pending_minute;
        self.pending.clear();

        if rows.is_empty() {
            None
        } else {
            Some((bucket, rows))
        }
    }
}
