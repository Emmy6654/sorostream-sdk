/**
 * Issue #99 – Benchmark suite for batch operations.
 *
 * Measures `bulkCreateStreams` and `batchWithdraw` throughput at 10, 50, and
 * 100-item batch sizes using `vitest bench`. Run via:
 *
 *   npx vitest bench
 *
 * Baseline numbers are recorded in bench/baseline.json for CI regression checks.
 */

import { bench, describe } from "vitest";
import { MockSoroStreamClient } from "../src/mock.js";
import type { BulkStreamRow } from "../src/types.js";

const RECIPIENT = "GDDZFLD7ZQTSSDLWEMSD6UML2MTU4KKNCH765GZOVHAYKZNRJMWV4GMF";
const TOKEN = "CAVTXNC2WCHINDNP4VBLSOQA2667VE3RPQZNGD5TFI4U2QSHTVAC667T";

function makeRows(count: number): BulkStreamRow[] {
  return Array.from({ length: count }, () => ({
    recipient: RECIPIENT,
    amount: 1_000_000_000n,
    durationSeconds: 3600,
  }));
}

async function bulkCreate(count: number): Promise<void> {
  const mock = new MockSoroStreamClient();
  const rows = makeRows(count);
  for (const row of rows) {
    await mock.createStream({ ...row, token: TOKEN, autoRenew: false });
  }
}

async function batchWithdraw(count: number): Promise<void> {
  const mock = new MockSoroStreamClient();
  const ids: string[] = [];
  for (let i = 0; i < count; i++) {
    const { streamId } = await mock.createStream({
      recipient: RECIPIENT,
      token: TOKEN,
      amount: 1_000_000_000n,
      durationSeconds: 3600,
      autoRenew: false,
    });
    ids.push(streamId);
  }
  for (const id of ids) {
    await mock.withdraw({ streamId: id });
  }
}

// ── bulkCreateStreams ─────────────────────────────────────────────────────────

describe("bulkCreateStreams throughput", () => {
  bench("10 streams", () => bulkCreate(10), { iterations: 50 });
  bench("50 streams", () => bulkCreate(50), { iterations: 20 });
  bench("100 streams", () => bulkCreate(100), { iterations: 10 });
});

// ── batchWithdraw throughput ──────────────────────────────────────────────────

describe("batchWithdraw throughput", () => {
  bench("10 withdrawals", () => batchWithdraw(10), { iterations: 50 });
  bench("50 withdrawals", () => batchWithdraw(50), { iterations: 20 });
  bench("100 withdrawals", () => batchWithdraw(100), { iterations: 10 });
});
