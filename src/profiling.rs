//! Developer query benchmarks. Wall-clock timings include result materialization
//! and disposal, but exclude JSON reporting and transport. Native stack sampling
//! is deliberately external so production queries pay no instrumentation cost.
use crate::{Result, Session, SqlQuery, SqlTable};
use rusqlite::types::Value;
use serde::Serialize;
use std::{
    hint::black_box,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryMode {
    /// Reuse one authorized snapshot, SQL connection and prepared statement.
    #[default]
    Prepared,
    /// Create the snapshot, SQL connection and statement on every iteration.
    Request,
}

#[derive(Clone, Copy, Debug)]
pub struct BenchmarkOptions {
    pub mode: QueryMode,
    /// Additional warmup executions after the separately timed first execution.
    pub warmup: usize,
    pub iterations: usize,
}
impl Default for BenchmarkOptions {
    fn default() -> Self {
        Self {
            mode: QueryMode::Prepared,
            warmup: 10,
            iterations: 200,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct QueryPlanStep {
    pub id: i64,
    pub parent: i64,
    pub detail: String,
}

#[derive(Debug, Serialize)]
pub struct SetupTimings {
    pub snapshot_us: f64,
    pub sql_context_us: f64,
    pub prepare_us: f64,
    /// First execution, including materializing and dropping its result. This is
    /// not a cold-disk measurement: opening the database already loaded its state.
    pub first_execution_us: f64,
}

#[derive(Debug, Serialize)]
pub struct Latencies {
    pub min_us: f64,
    pub mean_us: f64,
    pub p50_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub max_us: f64,
    pub total_us: f64,
}
#[derive(Debug, Serialize)]
pub struct QueryBenchmark {
    pub format: u32,
    pub mode: QueryMode,
    pub initial_revision: u64,
    pub warmup: usize,
    pub iterations: usize,
    pub setup: SetupTimings,
    pub latencies: Latencies,
    pub rows_per_iteration_min: usize,
    pub rows_per_iteration_max: usize,
    pub samples_us: Vec<f64>,
    pub plan: Vec<QueryPlanStep>,
}

fn micros(time: Duration) -> f64 {
    time.as_secs_f64() * 1_000_000.0
}

fn options_valid(options: BenchmarkOptions) -> Result<()> {
    if options.iterations == 0 || options.iterations > 1_000_000 || options.warmup > 1_000_000 {
        return Err(crate::Error::Query(
            "iterations must be 1..=1000000 and warmup 0..=1000000".into(),
        ));
    }
    Ok(())
}

// Named, non-inlined roots make the measured work easy to isolate in a profiler.
#[inline(never)]
fn execute_prepared(query: &mut SqlQuery<'_>, params: &[Value]) -> Result<usize> {
    let result = black_box(query.query(params)?);
    let count = result.rows.len();
    drop(result);
    Ok(count)
}

#[inline(never)]
fn execute_request(session: &Session, sql: &str, params: &[Value]) -> Result<usize> {
    let snapshot = session.snapshot();
    let context = snapshot.sql()?;
    let mut query = context.prepare(sql)?;
    execute_prepared(&mut query, params)
}

fn summarize(samples: &[f64]) -> Latencies {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    // Nearest-rank percentiles of individual executions, not batch means.
    let percentile = |p: usize| sorted[(sorted.len() * p).div_ceil(100) - 1];
    let total_us = samples.iter().sum();
    Latencies {
        min_us: sorted[0],
        mean_us: total_us / sorted.len() as f64,
        p50_us: percentile(50),
        p95_us: percentile(95),
        p99_us: percentile(99),
        max_us: sorted[sorted.len() - 1],
        total_us,
    }
}

fn collect_samples(
    mut execute: impl FnMut() -> Result<usize>,
    options: BenchmarkOptions,
) -> Result<(Vec<f64>, usize, usize)> {
    for _ in 0..options.warmup {
        execute()?;
    }
    let mut samples = Vec::with_capacity(options.iterations);
    let (mut min, mut max) = (usize::MAX, 0);
    for _ in 0..options.iterations {
        let start = Instant::now();
        let count = execute()?;
        samples.push(micros(start.elapsed()));
        min = min.min(count);
        max = max.max(count);
    }
    Ok((samples, min, max))
}

fn decode_plan(table: SqlTable) -> Result<Vec<QueryPlanStep>> {
    table
        .rows
        .into_iter()
        .enumerate()
        .map(|(index, row)| match row.as_slice() {
            [Value::Text(kind), Value::Text(plan)] => Ok(QueryPlanStep {
                // Each EXPLAIN stage is one root; these are not physical operator IDs.
                id: index as i64,
                parent: -1,
                detail: format!("{kind}: {plan}"),
            }),
            _ => Err(crate::Error::Query(
                "unexpected DataFusion EXPLAIN result".into(),
            )),
        })
        .collect()
}

/// Benchmark read-only SQL under the supplied session's actual authorization.
/// `Prepared` pins a snapshot; `Request` can observe newer revisions if the caller
/// writes concurrently. All result rows are materialized; use SQL LIMIT for large
/// results. Plans can contain SQL literals and bound values; reports belong to the requesting host.
pub fn benchmark_query(
    session: &Session,
    sql: &str,
    params: &[Value],
    options: BenchmarkOptions,
) -> Result<QueryBenchmark> {
    options_valid(options)?;
    let start = Instant::now();
    let snapshot = session.snapshot();
    let snapshot_us = micros(start.elapsed());
    let initial_revision = snapshot.revision();
    let start = Instant::now();
    let context = snapshot.sql()?;
    let sql_context_us = micros(start.elapsed());
    let start = Instant::now();
    let mut query = context.prepare(sql)?;
    let prepare_us = micros(start.elapsed());
    let start = Instant::now();
    execute_prepared(&mut query, params)?;
    let first_execution_us = micros(start.elapsed());
    let (samples_us, min, max) = match options.mode {
        QueryMode::Prepared => collect_samples(|| execute_prepared(&mut query, params), options)?,
        QueryMode::Request => {
            drop(query);
            collect_samples(|| execute_request(session, sql, params), options)?
        }
    };
    // Explain only after measurements so it cannot prime the first execution.
    let plan = decode_plan(context.query(&format!("EXPLAIN {sql}"), params)?)?;
    Ok(QueryBenchmark {
        format: 1,
        mode: options.mode,
        initial_revision,
        warmup: options.warmup,
        iterations: options.iterations,
        setup: SetupTimings {
            snapshot_us,
            sql_context_us,
            prepare_us,
            first_execution_us,
        },
        latencies: summarize(&samples_us),
        rows_per_iteration_min: min,
        rows_per_iteration_max: max,
        samples_us,
        plan,
    })
}

/// Repeat a query for a native sampler, without keeping per-iteration timings.
/// Startup and warmup may also appear in the recording; focus the profiler on
/// `execute_prepared` or `execute_request`. Duration is checked between queries,
/// so it is a minimum recording workload, not a query timeout.
pub fn sample_query(
    session: &Session,
    sql: &str,
    params: &[Value],
    options: BenchmarkOptions,
    duration: Duration,
) -> Result<u64> {
    options_valid(options)?;
    if duration.is_zero() || duration > Duration::from_secs(3600) {
        return Err(crate::Error::Query(
            "sample duration must be >0 and <=3600 seconds".into(),
        ));
    }
    let snapshot = session.snapshot();
    let context = snapshot.sql()?;
    let mut query = context.prepare(sql)?;
    let mut execute = || match options.mode {
        QueryMode::Prepared => execute_prepared(&mut query, params),
        QueryMode::Request => execute_request(session, sql, params),
    };
    for _ in 0..options.warmup {
        execute()?;
    }
    let start = Instant::now();
    let mut count = 0;
    while start.elapsed() < duration {
        execute()?;
        count += 1;
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn percentiles_are_individual_nearest_rank_samples() {
        let stats = summarize(&(1..=100).rev().map(f64::from).collect::<Vec<_>>());
        assert_eq!(
            (stats.p50_us, stats.p95_us, stats.p99_us),
            (50.0, 95.0, 99.0)
        );
        assert_eq!(stats.mean_us, 50.5);
        assert_eq!(summarize(&[7.0]).p99_us, 7.0);
    }
}
