#!/usr/bin/env node
// Run from any directory. All subprocess arguments are passed without a shell.
import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
export const help = `Usage: node tools/profile-query.mjs DATABASE PRINCIPAL QUERY.sql [OPTIONS]

  --profiler samply|flamegraph|none   Default samply; none benchmarks only
  --out DIRECTORY                  New output directory (default under target/)
  --mode prepared|request          Default prepared
  --params FILE                    JSON array of SqlWireValue parameters
  --iterations N                   Benchmark executions (default 200)
  --warmup N                       Additional warmups (default 10)
  --seconds N                      Sampling workload duration (default 10)
  --binary FILE                    Use an already-built executable

Without --binary, builds offline with Cargo's profiling profile and frame pointers.
Install one profiler: cargo install --locked samply (interactive flamegraph), or
cargo install --locked flamegraph (SVG). The wrapper never installs tools, enables
privileges, uploads a profile or starts a browser. See docs/QUERY-PROFILING.md.`;

export function parseArgs(args, cwd = process.cwd()) {
  if (args.length < 3) throw new Error(help);
  const config = { database: resolve(cwd, args[0]), principal: args[1], query: resolve(cwd, args[2]),
    profiler: "samply", mode: "prepared", iterations: 200, warmup: 10, seconds: 10 };
  const seen = new Set();
  for (let i = 3; i < args.length; i += 2) {
    const flag = args[i], value = args[i + 1];
    if (seen.has(flag)) throw new Error(`duplicate option ${flag}`);
    seen.add(flag);
    if (value === undefined) throw new Error(`missing value for ${flag}`);
    switch (flag) {
      case "--profiler": if (!["samply", "flamegraph", "none"].includes(value)) throw new Error("invalid profiler"); config.profiler = value; break;
      case "--mode": if (!["prepared", "request"].includes(value)) throw new Error("invalid mode"); config.mode = value; break;
      case "--out": config.out = resolve(cwd, value); break;
      case "--params": config.params = resolve(cwd, value); break;
      case "--binary": config.binary = resolve(cwd, value); break;
      case "--iterations": case "--warmup": case "--seconds": {
        const n = Number(value), field = flag.slice(2);
        const max = field === "seconds" ? 3600 : 1_000_000;
        if (!Number.isFinite(n) || n < (field === "warmup" ? 0 : Number.MIN_VALUE) || n > max || (field !== "seconds" && !Number.isInteger(n))) throw new Error(`invalid ${flag}`);
        config[field] = n; break;
      }
      default: throw new Error(`unknown option ${flag}`);
    }
  }
  config.out ??= resolve(root, "target/query-profiles", `${new Date().toISOString().replace(/[:.]/g, "-")}-${process.pid}`);
  return config;
}

export function workloadArgs(config, sampled = false) {
  const args = ["bench-query", config.database, config.principal, config.query,
    "--mode", config.mode, "--warmup", String(config.warmup)];
  if (config.params) args.push("--params", config.params);
  if (sampled) args.push("--sample-seconds", String(config.seconds));
  else args.push("--iterations", String(config.iterations), "--output", resolve(config.out, "benchmark.json"));
  return args;
}

export function profilerCommand(config, binary) {
  const workload = [binary, ...workloadArgs(config, true)];
  return config.profiler === "samply"
    ? ["samply", ["record", "--save-only", "-o", resolve(config.out, "profile.json"), "--", ...workload]]
    : ["flamegraph", ["-o", resolve(config.out, "flamegraph.svg"), "--", ...workload]];
}

function run(command, args, options = {}) {
  const child = spawnSync(command, args, { stdio: "inherit", ...options });
  if (child.error) throw child.error;
  if (child.status !== 0) {
    // Cargo emits compiler diagnostics on its JSON stdout, not stderr.
    if (typeof child.stdout === "string") for (const line of child.stdout.split("\n")) {
      try { const message = JSON.parse(line); if (message.message?.rendered) process.stderr.write(message.message.rendered); }
      catch { /* Non-JSON tool output does not contain Cargo diagnostics. */ }
    }
    throw new Error(`${command} failed (${child.signal ?? child.status})`);
  }
  return child;
}

function build() {
  const env = { ...process.env };
  if (env.CARGO_ENCODED_RUSTFLAGS !== undefined) env.CARGO_ENCODED_RUSTFLAGS += "\x1f-Cforce-frame-pointers=yes";
  else env.RUSTFLAGS = `${env.RUSTFLAGS ?? ""} -Cforce-frame-pointers=yes`.trim();
  const child = run("cargo", ["build", "--offline", "--profile", "profiling", "--bin", "substrate-liquid", "--message-format=json"],
    { cwd: root, env, stdio: ["inherit", "pipe", "inherit"], encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
  const artifact = child.stdout.split("\n").filter(Boolean).map(line => JSON.parse(line))
    .find(message => message.reason === "compiler-artifact" && message.target.name === "substrate-liquid" && message.executable);
  if (!artifact) throw new Error("Cargo did not report a substrate-liquid executable");
  return artifact.executable;
}

export function main(args) {
  if (args.length === 1 && ["--help", "-h"].includes(args[0])) { console.log(help); return; }
  const config = parseArgs(args);
  if (config.profiler !== "none") {
    const found = spawnSync(config.profiler, ["--version"], { encoding: "utf8" });
    if (found.error || found.status !== 0) throw new Error(`${config.profiler} is unavailable. Install with cargo install --locked ${config.profiler}, or use --profiler none for benchmarks only.`);
  }
  const binary = config.binary ?? build();
  mkdirSync(dirname(config.out), { recursive: true });
  mkdirSync(config.out); // Deliberately refuse to overwrite an earlier run.
  console.log("Benchmarking without a sampler...");
  run(binary, workloadArgs(config));
  const report = JSON.parse(readFileSync(resolve(config.out, "benchmark.json"), "utf8"));
  const benchmark = report.benchmark, latency = benchmark.latencies;
  console.log(`${benchmark.mode}: p50 ${latency.p50_us.toFixed(2)} µs, p95 ${latency.p95_us.toFixed(2)} µs, p99 ${latency.p99_us.toFixed(2)} µs (${benchmark.iterations} executions)`);
  console.log(`Report: ${resolve(config.out, "benchmark.json")}`);
  if (config.profiler !== "none") {
    console.log(`Recording a separate ${config.seconds}s sampler workload...`);
    const [command, argv] = profilerCommand(config, binary);
    try { run(command, argv, { cwd: config.out }); }
    catch (error) { throw new Error(`${error.message}. The benchmark report is preserved. Check profiler permissions/tool prerequisites; see docs/QUERY-PROFILING.md.`); }
    if (config.profiler === "samply") console.log(`Profile: ${resolve(config.out, "profile.json")}\nOpen the interactive flamegraph with: samply load <profile path>`);
    else console.log(`Flamegraph: ${resolve(config.out, "flamegraph.svg")}`);
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { main(process.argv.slice(2)); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
