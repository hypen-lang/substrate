import { test } from "node:test";
import assert from "node:assert/strict";
import { parseArgs, workloadArgs, profilerCommand } from "./profile-query.mjs";

test("paths and SQL parameters remain separate arguments, including spaces and shell metacharacters", () => {
  const c = parseArgs(["db with spaces.redb", "reader", "q;echo.sql", "--params", "p $x.json", "--out", "result", "--seconds", "0.5"], "/tmp");
  assert.equal(c.database, "/tmp/db with spaces.redb");
  assert.equal(c.query, "/tmp/q;echo.sql");
  assert.ok(workloadArgs(c).includes("/tmp/p $x.json"));
  const [command, args] = profilerCommand(c, "/tmp/my binary");
  assert.equal(command, "samply");
  assert.equal(args[args.indexOf("--") + 1], "/tmp/my binary");
  assert.ok(args.includes("--sample-seconds"));
  assert.ok(!args.includes("--output"));
  assert.ok(!args.includes("--iterations"));
});
test("rejects invalid, duplicate and unknown options", () => {
  for (const flags of [["--iterations", "0"], ["--warmup", "-1"], ["--seconds", "NaN"], ["--seconds", "3601"], ["--profiler", "anything"], ["--mode", "typo"], ["--out"], ["--wat", "1"], ["--mode", "request", "--mode", "prepared"]]) {
    assert.throws(() => parseArgs(["db", "admin", "sql", ...flags]));
  }
});
test("SVG profiler uses the same workload and prepared/request mode", () => {
  const c = parseArgs(["db", "admin", "sql", "--profiler", "flamegraph", "--mode", "request"]);
  const [command, args] = profilerCommand(c, "/tmp/bin");
  assert.equal(command, "flamegraph");
  assert.ok(args[1].endsWith("flamegraph.svg"));
  assert.ok(args.includes("request"));
});
