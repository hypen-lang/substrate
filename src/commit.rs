//! Caller-elected, bounded group commit. No worker owns the database lifetime.
use super::*;
use std::{
    collections::VecDeque,
    sync::Condvar,
    time::{Duration, Instant},
};

/// Queue admission bounds cover encoded redo bytes, not total process memory.
#[derive(Clone, Copy, Debug)]
pub struct GroupCommitOptions {
    pub max_queued_transactions: usize,
    pub max_queued_bytes: usize,
    pub max_group_transactions: usize,
    pub max_group_bytes: usize,
    /// Optional collection time; zero drains only work already queued.
    pub collection_window: Duration,
}
impl Default for GroupCommitOptions {
    fn default() -> Self {
        Self {
            max_queued_transactions: 1024,
            max_queued_bytes: 256 * 1024 * 1024,
            max_group_transactions: 64,
            max_group_bytes: 128 * 1024 * 1024,
            collection_window: Duration::ZERO,
        }
    }
}
impl GroupCommitOptions {
    pub(super) fn validate(&self) -> Result<()> {
        if self.max_queued_transactions == 0
            || self.max_group_transactions == 0
            || self.max_queued_bytes == 0
            || self.max_group_bytes == 0
            || self.collection_window > Duration::from_secs(1)
        {
            return Err(Error::Constraint(
                "commit limits must be positive; collection window must be at most one second"
                    .into(),
            ));
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct GroupCommitStats {
    /// Successful Immediate redb transactions for writes, including automatic checkpoints.
    /// This counts commit groups, not filesystem sync calls internal to redb.
    pub durable_groups: u64,
    pub committed_writes: u64,
    pub largest_group: usize,
    pub queued_transactions: usize,
    pub queued_bytes: usize,
}
pub(super) struct Request {
    base: Arc<State>,
    reads: BTreeSet<Key>,
    ops: Vec<Op>,
    live: LiveGuard,
    bytes: usize,
    result: Mutex<Option<Result<u64>>>,
}
impl Request {
    pub(super) fn new(
        base: Arc<State>,
        reads: BTreeSet<Key>,
        ops: Vec<Op>,
        live: LiveGuard,
        bytes: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            base,
            reads,
            ops,
            live,
            bytes,
            result: Mutex::new(None),
        })
    }
}
#[derive(Default)]
struct Queue {
    pending: VecDeque<Arc<Request>>,
    bytes: usize,
    active: bool,
    stats: GroupCommitStats,
}
pub(super) struct Coordinator {
    pub(super) writer: Mutex<Option<Durable>>,
    pub(super) maintenance_active: std::sync::atomic::AtomicBool,
    queue: Mutex<Queue>,
    changed: Condvar,
    options: GroupCommitOptions,
}
impl Coordinator {
    pub(super) fn new(durable: Option<Durable>, options: GroupCommitOptions) -> Self {
        Self {
            writer: Mutex::new(durable),
            maintenance_active: std::sync::atomic::AtomicBool::new(false),
            queue: Mutex::new(Queue::default()),
            changed: Condvar::new(),
            options,
        }
    }
    pub(super) fn stats(&self) -> GroupCommitStats {
        let queue = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        GroupCommitStats {
            queued_transactions: queue.pending.len(),
            queued_bytes: queue.bytes,
            ..queue.stats
        }
    }
    pub(super) fn submit(&self, head: &Mutex<Head>, request: Arc<Request>) -> Result<u64> {
        if request.bytes > self.options.max_group_bytes
            || request.bytes > self.options.max_queued_bytes
        {
            return Err(Error::Constraint(
                "transaction exceeds commit byte limit".into(),
            ));
        }
        let mut queue = self.queue.lock().map_err(|_| storage_poison())?;
        // Reject overload instead of allowing an unbounded population of blocked submissions.
        if queue.pending.len() >= self.options.max_queued_transactions
            || request.bytes > self.options.max_queued_bytes - queue.bytes
        {
            return Err(Error::Constraint(
                "commit queue is full; retry with backoff".into(),
            ));
        }
        queue.bytes += request.bytes;
        queue.pending.push_back(request.clone());
        self.changed.notify_all();
        loop {
            if let Some(result) = request.result.lock().map_err(|_| storage_poison())?.take() {
                return result;
            }
            if queue.active {
                queue = self.changed.wait(queue).map_err(|_| storage_poison())?;
                continue;
            }
            queue.active = true;
            if !self.options.collection_window.is_zero() {
                let until = Instant::now() + self.options.collection_window;
                while queue.pending.len() < self.options.max_group_transactions
                    && queue.bytes < self.options.max_group_bytes
                {
                    let Some(remaining) = until.checked_duration_since(Instant::now()) else {
                        break;
                    };
                    queue = self
                        .changed
                        .wait_timeout(queue, remaining)
                        .map_err(|_| storage_poison())?
                        .0;
                }
            }
            let mut group = Vec::new();
            let mut bytes = 0;
            while let Some(next) = queue.pending.front() {
                if group.len() == self.options.max_group_transactions
                    || next.bytes > self.options.max_group_bytes - bytes
                {
                    break;
                }
                let next = queue.pending.pop_front().unwrap();
                bytes += next.bytes;
                queue.bytes -= next.bytes;
                group.push(next);
            }
            drop(queue);
            let (results, writes, durable) = self.process(head, &group);
            queue = self.queue.lock().map_err(|_| storage_poison())?;
            for (request, result) in group.iter().zip(results) {
                *request.result.lock().map_err(|_| storage_poison())? = Some(result);
            }
            queue.stats.committed_writes += writes as u64;
            queue.stats.durable_groups += u64::from(durable && writes > 0);
            queue.stats.largest_group = queue.stats.largest_group.max(writes);
            queue.active = false;
            self.changed.notify_all();
            // Return our acknowledgement before considering another group. Other
            // waiters may elect themselves immediately; no leader starvation.
        }
    }
    fn process(
        &self,
        head: &Mutex<Head>,
        group: &[Arc<Request>],
    ) -> (Vec<Result<u64>>, usize, bool) {
        let fail_all = |message: String| {
            (
                group
                    .iter()
                    .map(|_| Err(Error::Storage(message.clone())))
                    .collect(),
                0,
                false,
            )
        };
        let mut writer = match self.writer.lock() {
            Ok(writer) => writer,
            Err(_) => return fail_all("writer lock poisoned".into()),
        };
        let initial = match head.lock() {
            Ok(head) if !head.failed => head.state.clone(),
            _ => return fail_all("reopen database after uncertain storage failure".into()),
        };
        let mut next = (*initial).clone();
        let mut results = Vec::with_capacity(group.len());
        let mut records = Vec::new();
        for request in group {
            let result = (|| {
                if request
                    .reads
                    .iter()
                    .any(|key| request.base.version(key) != next.version(key))
                {
                    return Err(Error::Conflict);
                }
                request.live.check_state(&next, false)?;
                if !request.ops.is_empty() {
                    let revision = next
                        .revision
                        .checked_add(1)
                        .ok_or_else(|| Error::Storage("revision exhausted".into()))?;
                    for op in &request.ops {
                        next.apply(op, revision);
                    }
                    next.revision = revision;
                    records.push((revision, request.ops.as_slice()));
                }
                Ok(next.revision)
            })();
            results.push(result);
        }
        let durable = writer.is_some();
        if !records.is_empty() {
            if let Some(writer) = writer.as_mut()
                && let Err(error) = writer.append_group(&records, &next)
            {
                if let Ok(mut head) = head.lock() {
                    head.failed = true;
                }
                for result in &mut results {
                    if result.is_ok() {
                        *result = Err(Error::Storage(format!(
                            "uncertain group persistence: {error}; reopen database"
                        )));
                    }
                }
                return (results, 0, durable);
            }
            crate::durable::failpoint("group_before_publication");
            match head.lock() {
                Ok(mut head) => head.state = Arc::new(next),
                Err(_) => return fail_all("published head lock poisoned; reopen database".into()),
            }
            crate::durable::failpoint("group_after_publication");
        }
        (results, records.len(), durable)
    }
}
fn storage_poison() -> Error {
    Error::Storage("commit coordinator lock poisoned; reopen database".into())
}

#[cfg(test)]
#[path = "commit_tests.rs"]
mod tests;
