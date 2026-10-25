//! Bounded, opportunistic checkpoint maintenance. Hard redo bounds remain in the
//! commit path; this worker never acknowledges application writes.
use super::*;
use std::{
    sync::{Condvar, TryLockError, atomic::Ordering},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug)]
pub struct CheckpointerOptions {
    /// Poll and busy-writer retry interval. Explicit requests wake an idle worker.
    pub interval: Duration,
    /// Early thresholds, independent of the synchronous hard checkpoint policy.
    pub early_policy: CheckpointPolicy,
}
impl Default for CheckpointerOptions {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(250),
            early_policy: CheckpointPolicy {
                max_log_commits: 64,
                max_log_bytes: 4 * 1024 * 1024,
            },
        }
    }
}
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct CheckpointerStats {
    pub requests: u64,
    pub coalesced_requests: u64,
    pub completed_ticket: u64,
    pub checkpoints: u64,
    pub busy_retries: u64,
    /// Head covered by the last successful maintenance attempt, including no-op.
    pub checkpoint_revision: u64,
    pub stopped: bool,
    pub failure: Option<String>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointCompletion {
    pub requested_ticket: u64,
    /// A single attempt may satisfy multiple coalesced tickets.
    pub completed_ticket: u64,
    /// The durable checkpoint covers at least this revision when the request completes.
    pub revision: u64,
}
#[derive(Default)]
struct Work {
    stats: CheckpointerStats,
    stop: bool,
}
#[derive(Default)]
struct Shared {
    work: Mutex<Work>,
    changed: Condvar,
}
/// Host-owned maintenance lifetime. Drop requests stop and joins any in-flight
/// checkpoint. Idle workers retain only weak database ownership.
pub struct Checkpointer {
    shared: Arc<Shared>,
    worker: Option<JoinHandle<()>>,
}
impl Checkpointer {
    /// Queue one maintenance request. Many calls coalesce into one bounded flag;
    /// no callbacks, row copies, per-request channels or unbounded queue are kept.
    pub fn request(&self) -> Result<u64> {
        let mut work = self.shared.work.lock().unwrap_or_else(|e| e.into_inner());
        available(&work)?;
        let ticket = work
            .stats
            .requests
            .checked_add(1)
            .ok_or_else(|| Error::Storage("checkpoint ticket exhausted".into()))?;
        if work.stats.requests > work.stats.completed_ticket {
            work.stats.coalesced_requests += 1;
        }
        work.stats.requests = ticket;
        self.shared.changed.notify_all();
        Ok(ticket)
    }
    /// Wait for a checkpoint attempt started after the ticket was captured. A
    /// timeout does not cancel maintenance. Later completions also satisfy older
    /// tickets; this is a coverage receipt, not an individual execution receipt.
    pub fn wait(&self, ticket: u64, timeout: Duration) -> Result<CheckpointCompletion> {
        let end = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Error::Constraint("checkpoint timeout overflow".into()))?;
        let mut work = self.shared.work.lock().unwrap_or_else(|e| e.into_inner());
        if ticket == 0 || ticket > work.stats.requests {
            return Err(Error::Constraint("unknown checkpoint ticket".into()));
        }
        loop {
            if work.stats.completed_ticket >= ticket {
                return Ok(CheckpointCompletion {
                    requested_ticket: ticket,
                    completed_ticket: work.stats.completed_ticket,
                    revision: work.stats.checkpoint_revision,
                });
            }
            available(&work)?;
            let remaining = end.checked_duration_since(Instant::now()).ok_or_else(|| {
                Error::Storage("checkpoint wait timed out; request may still complete".into())
            })?;
            work = self
                .shared
                .changed
                .wait_timeout(work, remaining)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }
    pub fn stats(&self) -> CheckpointerStats {
        self.shared
            .work
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .stats
            .clone()
    }
    /// Stop scheduling and wait for any current disk operation. Unstarted
    /// requests are cancelled; already completed tickets remain inspectable.
    pub fn stop(mut self) -> CheckpointerStats {
        self.join();
        self.stats()
    }
    fn join(&mut self) {
        {
            let mut work = self.shared.work.lock().unwrap_or_else(|e| e.into_inner());
            work.stop = true;
            self.shared.changed.notify_all();
        }
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            let mut work = self.shared.work.lock().unwrap_or_else(|e| e.into_inner());
            work.stats.failure = Some("checkpoint worker panicked".into());
            work.stats.stopped = true;
        }
    }
}
impl Drop for Checkpointer {
    fn drop(&mut self) {
        self.join();
    }
}
fn available(work: &Work) -> Result<()> {
    if let Some(error) = &work.stats.failure {
        return Err(Error::Storage(error.clone()));
    }
    if work.stop || work.stats.stopped {
        return Err(Error::Storage("checkpoint worker stopped".into()));
    }
    Ok(())
}
impl Database {
    pub fn start_checkpointer(&self, options: CheckpointerOptions) -> Result<Checkpointer> {
        if options.interval < Duration::from_millis(1)
            || options.interval > Duration::from_secs(3600)
            || options.early_policy.max_log_commits == 0
            || options.early_policy.max_log_bytes == 0
        {
            return Err(Error::Constraint(
                "checkpoint interval must be 1ms..1h and thresholds positive".into(),
            ));
        }
        if self
            .coordinator
            .writer
            .lock()
            .map_err(|_| Error::Storage("writer lock poisoned".into()))?
            .is_none()
        {
            return Err(Error::Storage(
                "memory database has no checkpoint storage".into(),
            ));
        }
        self.coordinator
            .maintenance_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| Error::Constraint("database already has a checkpointer".into()))?;
        let head = Arc::downgrade(&self.head);
        let coordinator = Arc::downgrade(&self.coordinator);
        let shared = Arc::new(Shared::default());
        let work = shared.clone();
        let worker = match thread::Builder::new()
            .name("liquid-checkpoint".into())
            .spawn(move || run(head, coordinator, options, work))
        {
            Ok(worker) => worker,
            Err(error) => {
                self.coordinator
                    .maintenance_active
                    .store(false, Ordering::Release);
                return Err(Error::Storage(error.to_string()));
            }
        };
        Ok(Checkpointer {
            shared,
            worker: Some(worker),
        })
    }
}
enum Attempt {
    Busy,
    Idle,
    Completed { revision: u64, wrote: bool },
    Closed,
}
fn attempt(
    head: &Weak<Mutex<Head>>,
    coordinator: &Weak<commit::Coordinator>,
    options: CheckpointerOptions,
    requested: bool,
) -> Result<Attempt> {
    let (Some(head), Some(coordinator)) = (head.upgrade(), coordinator.upgrade()) else {
        return Ok(Attempt::Closed);
    };
    let mut writer = match coordinator.writer.try_lock() {
        Ok(writer) => writer,
        Err(TryLockError::WouldBlock) => return Ok(Attempt::Busy),
        Err(TryLockError::Poisoned(_)) => {
            return Err(Error::Storage("writer lock poisoned".into()));
        }
    };
    let state = {
        let head = head
            .lock()
            .map_err(|_| Error::Storage("database lock poisoned".into()))?;
        if head.failed {
            return Err(Error::Storage(
                "reopen after uncertain storage failure".into(),
            ));
        }
        head.state.clone()
    };
    let durable = writer
        .as_mut()
        .ok_or_else(|| Error::Storage("no checkpoint storage".into()))?;
    let due = durable.info.log_commits >= options.early_policy.max_log_commits
        || durable.info.log_bytes >= options.early_policy.max_log_bytes;
    if !requested && !due {
        return Ok(Attempt::Idle);
    }
    let wrote = durable.info.log_commits != 0;
    if wrote && let Err(error) = durable.checkpoint(&state) {
        head.lock()
            .map_err(|_| Error::Storage("database lock poisoned".into()))?
            .failed = true;
        return Err(error);
    }
    Ok(Attempt::Completed {
        revision: state.revision,
        wrote,
    })
}
fn run(
    head: Weak<Mutex<Head>>,
    coordinator: Weak<commit::Coordinator>,
    options: CheckpointerOptions,
    shared: Arc<Shared>,
) {
    loop {
        let (ticket, completed) = {
            let work = shared.work.lock().unwrap_or_else(|e| e.into_inner());
            if work.stop {
                break;
            }
            (work.stats.requests, work.stats.completed_ticket)
        };
        let result = attempt(&head, &coordinator, options, ticket > completed);
        let mut work = shared.work.lock().unwrap_or_else(|e| e.into_inner());
        match result {
            Ok(Attempt::Busy) => work.stats.busy_retries += 1,
            Ok(Attempt::Idle) => {}
            Ok(Attempt::Closed) => break,
            Ok(Attempt::Completed { revision, wrote }) => {
                work.stats.checkpoints += u64::from(wrote);
                work.stats.checkpoint_revision = revision;
                work.stats.completed_ticket = ticket;
                shared.changed.notify_all();
            }
            Err(error) => {
                work.stats.failure = Some(error.to_string());
                break;
            }
        }
        if work.stop {
            break;
        }
        // Requests arriving during IO must trigger a fresh attempt; busy retries
        // are interval paced so a long write never creates a spinning worker.
        if work.stats.requests > ticket {
            continue;
        }
        drop(
            shared
                .changed
                .wait_timeout(work, options.interval)
                .unwrap_or_else(|e| e.into_inner()),
        );
    }
    if let Some(coordinator) = coordinator.upgrade() {
        coordinator
            .maintenance_active
            .store(false, Ordering::Release);
    }
    let mut work = shared.work.lock().unwrap_or_else(|e| e.into_inner());
    work.stats.stopped = true;
    shared.changed.notify_all();
}

#[cfg(test)]
#[path = "maintenance_tests.rs"]
mod tests;
