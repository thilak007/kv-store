//! Approximate real-time causal consistency checker.

use std::collections::{HashMap, VecDeque};
use std::fmt;

use runner::KvResp;

/// Expected values for a consistency check.
#[derive(Debug, Clone)]
pub(crate) enum ExpectedResult {
    Put { possible_found: Vec<bool> },
    Swap { possible_old_values: Vec<Option<String>> },
    Get { possible_values: Vec<Option<String>> },
    Delete { possible_found: Vec<bool> },
    Scan { expected_entries: Vec<(String, String)> },
}

impl fmt::Display for ExpectedResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpectedResult::Put { possible_found } => {
                write!(f, "Put - possible 'found' values: {:?}", possible_found)
            }
            ExpectedResult::Swap { possible_old_values } => {
                write!(f, "Swap - possible old values: {:?}", possible_old_values)
            }
            ExpectedResult::Get { possible_values } => {
                write!(f, "Get - possible values: {:?}", possible_values)
            }
            ExpectedResult::Delete { possible_found } => {
                write!(f, "Delete - possible 'found' values: {:?}", possible_found)
            }
            ExpectedResult::Scan { expected_entries } => {
                write!(f, "Scan - expected entries: {:?}", expected_entries)
            }
        }
    }
}

/// Per-key, non-read-only operation record with timestamp span.
#[derive(Debug, Clone)]
struct UpdateSpan {
    ts_call: u64,
    ts_resp: u64,
    value: Option<String>,
}

/// Check queue entry with timestamp span.
#[derive(Debug, Clone)]
struct QueuedSpan {
    ts_call: u64,
    ts_resp: u64,
    resp: KvResp,
}

/// Trimmed history of per-client acknowledged operations.
#[derive(Debug)]
pub(crate) struct History {
    /// Queue of pending responses to check (naturally ordered by response
    /// timestamp).
    queue: VecDeque<QueuedSpan>,

    /// Per-key per-client trimmed history of acknowledged update operations.
    spans: HashMap<String, Vec<VecDeque<UpdateSpan>>>,

    /// Per-client max update resp timestamp seen.
    maxtr: Vec<u64>,
}

impl History {
    /// Create a new empty history for given number of clients and keys pool.
    pub(crate) fn new(num_clis: usize, keys: &[Vec<String>]) -> Self {
        let mut spans = HashMap::new();
        for cli_keys in keys {
            for key in cli_keys {
                if !spans.contains_key(key) {
                    spans.insert(
                        key.clone(),
                        vec![
                            VecDeque::<UpdateSpan>::from([UpdateSpan {
                                ts_call: 0,
                                ts_resp: 0,
                                value: None, // dummy Delete to simplify logic
                            }]);
                            num_clis
                        ],
                    );
                }
            }
        }

        History {
            queue: VecDeque::new(),
            spans,
            maxtr: vec![0; num_clis],
        }
    }

    /// Add a newly acknowledged response result to the check queue.
    pub(crate) fn add_to_queue(&mut self, ts_call: u64, ts_resp: u64, resp: KvResp) {
        debug_assert!(ts_call < ts_resp);
        debug_assert!(self.queue.is_empty() || self.queue.back().unwrap().ts_resp < ts_resp);

        self.queue.push_back(QueuedSpan {
            ts_call,
            ts_resp,
            resp,
        });
    }

    /// Get the number of remaining checks in the check queue.
    pub(crate) fn queue_len(&self) -> usize {
        self.queue.len()
    }

    /// Add a newly acknowledged update to the history, possibly trimming the
    /// heads of the history and possibly triggering some pending results to
    /// get checked. Returns:
    ///   - `Some(Some((resp, expected)))` if the check of a `resp` failed
    ///   - `Some(None)` if update key is unexpected
    ///   - `None` if everything is still alright
    pub(crate) fn apply_update(
        &mut self,
        cidx: usize,
        ts_call: u64,
        ts_resp: u64,
        key: String,
        value: Option<String>,
    ) -> Option<Option<(KvResp, ExpectedResult)>> {
        if let Some(key_spans) = self.spans.get_mut(&key) {
            debug_assert!(cidx < self.maxtr.len());
            debug_assert!(cidx < key_spans.len());
            debug_assert!(
                key_spans[cidx].is_empty() || key_spans[cidx].back().unwrap().ts_resp < ts_call
            );

            key_spans[cidx].push_back(UpdateSpan {
                ts_call,
                ts_resp,
                value,
            });
            self.maxtr[cidx] = ts_resp;
            let min_coming_ts = *self.maxtr.iter().min().unwrap();
            let min_queued_ts = self
                .queue
                .iter()
                .map(|e| e.ts_call)
                .min()
                .unwrap_or(u64::MAX);

            // trim off updates at head of client histories
            for cli_spans in key_spans.iter_mut() {
                let mut keep_ts = 0;
                for span in cli_spans.iter().rev() {
                    // can discard only if there's one span that's fully ahead
                    // of the next possible incoming request and any pending
                    // request in the check queue
                    if span.ts_resp < min_coming_ts && span.ts_resp < min_queued_ts {
                        keep_ts = span.ts_call;
                        break;
                    }
                }
                while cli_spans.len() > 1 && cli_spans.front().unwrap().ts_resp < keep_ts {
                    cli_spans.pop_front();
                }
            }

            // pop off now-checkable results from the check queue
            while self.queue.front().is_some()
                && self.queue.front().unwrap().ts_resp < min_coming_ts
            {
                let entry = self.queue.pop_front().unwrap();
                if let Some(expected) = self.check_call(&entry) {
                    return Some(Some((entry.resp, expected)));
                }
            }

            None
        } else {
            Some(None)
        }
    }

    /// Check a call popped off from the check queue, which is now decidable.
    /// Returns None if check passed, Some(ExpectedResult) if check failed.
    fn check_call(&self, entry: &QueuedSpan) -> Option<ExpectedResult> {
        match &entry.resp {
            KvResp::Put { key, found } => {
                if let Some(key_spans) = self.spans.get(key) {
                    Self::check_put(key_spans, entry.ts_call, entry.ts_resp, found)
                } else {
                    None
                }
            }
            KvResp::Swap { key, old_value } => {
                if let Some(key_spans) = self.spans.get(key) {
                    Self::check_swap(key_spans, entry.ts_call, entry.ts_resp, old_value.as_ref())
                } else {
                    None
                }
            }
            KvResp::Get { key, value } => {
                if let Some(key_spans) = self.spans.get(key) {
                    Self::check_get(key_spans, entry.ts_call, entry.ts_resp, value.as_ref())
                } else {
                    None
                }
            }
            KvResp::Scan {
                key_start,
                key_end,
                entries,
            } => Self::check_scan(
                &self.spans,
                entry.ts_call,
                entry.ts_resp,
                key_start,
                key_end,
                entries,
            ),
            KvResp::Delete { key, found } => {
                if let Some(key_spans) = self.spans.get(key) {
                    Self::check_delete(key_spans, entry.ts_call, entry.ts_resp, found)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Check a Put operation result assuming given history.
    /// Returns None if valid, Some(expected) if invalid.
    fn check_put(
        key_spans: &[VecDeque<UpdateSpan>],
        ts_call: u64,
        ts_resp: u64,
        found: &bool,
    ) -> Option<ExpectedResult> {
        // eprintln!(
        //     "--- PUT <{} - {}> {} {:?}",
        //     ts_call, ts_resp, found, key_spans
        // );
        let mut possible_found = std::collections::HashSet::new();
        for cli_spans in key_spans {
            for span in cli_spans.iter().rev() {
                if span.ts_call < ts_resp && span.value.is_some() == *found {
                    return None;  // Valid - found expected state
                }
                if span.ts_resp >= ts_call {
                    possible_found.insert(span.value.is_some());
                }
                if span.ts_resp < ts_call {
                    break;
                }
            }
        }
        // Invalid - collect possible expected values
        Some(ExpectedResult::Put {
            possible_found: possible_found.into_iter().collect(),
        })
    }

    /// Check a Swap operation result assuming given history.
    /// Returns None if valid, Some(expected) if invalid.
    fn check_swap(
        key_spans: &[VecDeque<UpdateSpan>],
        ts_call: u64,
        ts_resp: u64,
        old_value: Option<&String>,
    ) -> Option<ExpectedResult> {
        // eprintln!(
        //     "--- SWAP <{} - {}> {:?} {:?}",
        //     ts_call, ts_resp, old_value, key_spans
        // );
        let mut possible_old_values = std::collections::HashSet::new();
        for cli_spans in key_spans {
            for span in cli_spans.iter().rev() {
                if span.ts_call < ts_resp && span.value.as_ref() == old_value {
                    return None;  // Valid - found expected state
                }
                if span.ts_resp >= ts_call {
                    possible_old_values.insert(span.value.clone());
                }
                if span.ts_resp < ts_call {
                    break;
                }
            }
        }
        // Invalid - collect possible expected values
        Some(ExpectedResult::Swap {
            possible_old_values: possible_old_values.into_iter().collect(),
        })
    }

    /// Check a Get operation result assuming given history.
    /// Returns None if valid, Some(expected) if invalid.
    fn check_get(
        key_spans: &[VecDeque<UpdateSpan>],
        ts_call: u64,
        ts_resp: u64,
        value: Option<&String>,
    ) -> Option<ExpectedResult> {
        // eprintln!(
        //     "--- GET <{} - {}> {:?} {:?}",
        //     ts_call, ts_resp, value, key_spans
        // );
        let mut possible_values = std::collections::HashSet::new();
        for cli_spans in key_spans {
            for span in cli_spans.iter().rev() {
                if span.ts_call < ts_resp && span.value.as_ref() == value {
                    return None;  // Valid - found expected state
                }
                if span.ts_resp >= ts_call {
                    possible_values.insert(span.value.clone());
                }
                if span.ts_resp < ts_call {
                    break;
                }
            }
        }
        // Invalid - collect possible expected values
        Some(ExpectedResult::Get {
            possible_values: possible_values.into_iter().collect(),
        })
    }

    /// Check a Scan operation result assuming given history. All possible
    /// keys in range are searched here.
    /// Returns None if valid, Some(expected) if invalid.
    fn check_scan(
        spans: &HashMap<String, Vec<VecDeque<UpdateSpan>>>,
        ts_call: u64,
        ts_resp: u64,
        key_start: &String,
        key_end: &String,
        entries: &[(String, String)],
    ) -> Option<ExpectedResult> {
        let mut entries_map = HashMap::new();
        for (key, value) in entries {
            if key < key_start || key > key_end {
                // out-of-range in scan result
                return Some(ExpectedResult::Scan {
                    expected_entries: vec![],
                });
            }
            if entries_map.contains_key(key) {
                // duplicate key in scan result
                return Some(ExpectedResult::Scan {
                    expected_entries: vec![],
                });
            }
            entries_map.insert(key, value);
        }

        // Build expected entries for valid response
        let mut expected_entries = Vec::new();
        // eprintln!("--- SCAN <{} - {}> loop", ts_call, ts_resp);
        for (key, key_spans) in spans {
            // if key >= key_start && key <= key_end {
            //     println!("... {} {:?} {:?}", key, entries_map.get(key), key_spans);
            // }
            if key >= key_start && key <= key_end {
                if Self::check_get(key_spans, ts_call, ts_resp, entries_map.get(key).copied()).is_some() {
                    // This key failed the check
                    return Some(ExpectedResult::Scan {
                        expected_entries: expected_entries,
                    });
                }
                // Collect what the value should be
                if let Some(value) = entries_map.get(key) {
                    expected_entries.push((key.clone(), (*value).clone()));
                }
            }
        }
        None // all possible keys in range passed check
    }

    /// Check a Delete operation result assuming given history.
    /// Returns None if valid, Some(expected) if invalid.
    fn check_delete(
        key_spans: &[VecDeque<UpdateSpan>],
        ts_call: u64,
        ts_resp: u64,
        found: &bool,
    ) -> Option<ExpectedResult> {
        // eprintln!(
        //     "--- DELETE <{} - {}> {} {:?}",
        //     ts_call, ts_resp, found, key_spans
        // );
        let mut possible_found = std::collections::HashSet::new();
        for cli_spans in key_spans {
            for span in cli_spans.iter().rev() {
                if span.ts_call < ts_resp && span.value.is_some() == *found {
                    return None;  // Valid - found expected state
                }
                if span.ts_resp >= ts_call {
                    possible_found.insert(span.value.is_some());
                }
                if span.ts_resp < ts_call {
                    break;
                }
            }
        }
        // Invalid - collect possible expected values
        Some(ExpectedResult::Delete {
            possible_found: possible_found.into_iter().collect(),
        })
    }
}
