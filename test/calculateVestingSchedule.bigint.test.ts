/**
 * Regression tests for calculateVestingSchedule when totalAmount exceeds
 * Number.MAX_SAFE_INTEGER (≈9.007e15).
 *
 * Originally: `Math.floor(totalSeconds * pct)` and `BigInt(elapsed)` would
 * round intermediate Number values to the nearest representable double,
 * drifting the returned vested amounts by a few stroops for very large
 * streams. The fix keeps every intermediate in BigInt.
 *
 * See: fix/bigint-vesting-arithmetic branch.
 */
import { describe, it, expect } from "vitest";
import { calculateVestingSchedule } from "../src/utils.js";
import type { Stream } from "../src/types.js";

const MAX_SAFE_INTEGER = BigInt(Number.MAX_SAFE_INTEGER);

function makeStream(overrides: Partial<Stream> & {
  flowRate: bigint;
  startTime: number;
  endTime: number;
  deposit: bigint;
}): Stream {
  return {
    id: "0",
    sender: "GSENDER",
    recipient: "GRECIPIENT",
    token: "GTOKEN",
    lastWithdrawTime: overrides.startTime,
    status: "Active",
    autoRenew: false,
    ...overrides,
  };
}

describe("calculateVestingSchedule — BigInt safety (totalAmount > MAX_SAFE_INTEGER)", () => {
  it("produces exact totalAmount when flowRate × duration exceeds Number.MAX_SAFE_INTEGER", () => {
    const startTime = 1_700_000_000;
    // 10 USDC/s × ~3.17 years ≈ 1B USDC total = 10_000_000_000_000_000 stroops
    const flowRate = 100_000_000n;
    const duration = 100_000_000; // ~3.17 years, well within MAX_SAFE_INTEGER
    const totalAmount = 10_000_000_000_000_000n;
    const cliff = 25_000_000;

    const stream = makeStream({
      flowRate,
      startTime,
      endTime: startTime + duration,
      deposit: totalAmount,
    });

    // Mid-cliff: effectiveClaimable should be 0n, totalAmount should be exact.
    const result = calculateVestingSchedule(
      stream,
      cliff,
      startTime + cliff / 2
    );

    expect(result.totalAmount).toBe(totalAmount);
    expect(result.totalAmount).toBeGreaterThan(MAX_SAFE_INTEGER);
    expect(result.effectiveClaimable).toBe(0n);
    expect(result.inCliff).toBe(true);
  });

  it("returns exact effectiveClaimable just past cliff when totalAmount > MAX_SAFE_INTEGER", () => {
    const startTime = 1_700_000_000;
    const flowRate = 100_000_000n;        // 10 USDC/s
    const duration = 100_000_000;         // ~3.17 years
    const totalAmount = 10_000_000_000_000_000n;
    const cliff = 25_000_000;
    const elapsedAfterCliff = 1_000_000;  // seconds since cliff end

    const stream = makeStream({
      flowRate,
      startTime,
      endTime: startTime + duration,
      deposit: totalAmount,
    });

    const result = calculateVestingSchedule(
      stream,
      cliff,
      startTime + cliff + elapsedAfterCliff
    );

    // Expected: flowRate × elapsedAfterCliff = 100_000_000n × 1_000_000n = 10^14
    const expected = 100_000_000_000_000n;
    expect(result.effectiveClaimable).toBe(expected);
    expect(result.inCliff).toBe(false);
  });

  it("preserves BigInt precision for milestone vested amounts when totalSeconds > MAX_SAFE_INTEGER", () => {
    // Constructed: totalSeconds above 2^53 so that intermediate
    // `Math.floor(totalSeconds * 0.25)` in the old code rounded to the
    // next representable Number and drifted the vested amount by ~1 stroop.
    //
    // We use an *even* value so its Number representation is exact (IEEE
    // 754 spacing in the [2^53, 2^54] range is 2, so even values round
    // cleanly). This lets us assert exact BigInt equality.
    const flowRate = 1n;
    const totalSecondsBig = 10_000_000_000_000_000n; // > MAX_SAFE_INTEGER, exact as Number
    const startTime = 0;
    const cliffSeconds = 0;

    const stream = makeStream({
      flowRate,
      startTime,
      endTime: startTime + Number(totalSecondsBig), // even → exact
      deposit: totalSecondsBig,
    });

    const result = calculateVestingSchedule(stream, cliffSeconds, startTime);

    const vestedActual = result.milestones.map((m) => m.vested);

    // Cliff milestone: cliffSeconds = 0 → vested = 0n.
    expect(vestedActual).toContain(0n);

    // Percent milestones at 25% / 50% / 75% / 100% of totalSeconds,
    // all exact BigInt divisions:
    expect(vestedActual).toContain(2_500_000_000_000_000n); // 25% of 10^16
    expect(vestedActual).toContain(5_000_000_000_000_000n); // 50%
    expect(vestedActual).toContain(7_500_000_000_000_000n); // 75%
    expect(vestedActual).toContain(10_000_000_000_000_000n); // 100%

    // Must not contain 0n without a real cliff edge case.
    // (cliffSeconds=0 just so the cliff milestone exists as 0n.)
  });

  it("preserves exact totalAmount and cliff milestone when endTime-startTime > MAX_SAFE_INTEGER", () => {
    // Validates the cliff milestone and totalAmount survive the
    // Number subtraction `endTime - startTime` boundary.
    const flowRate = 7n;
    const totalSecondsBig = 10_000_000_000_000_000n;
    const cliffSeconds = 2_500_000_000_000_000; // > MAX_SAFE_INTEGER, even → exact
    const startTime = 0;

    const stream = makeStream({
      flowRate,
      startTime,
      endTime: startTime + Number(totalSecondsBig),
      deposit: flowRate * totalSecondsBig,
    });

    const result = calculateVestingSchedule(
      stream,
      cliffSeconds,
      startTime + 1_000
    );

    expect(result.totalAmount).toBe(flowRate * totalSecondsBig);
    expect(result.totalAmount).toBeGreaterThan(MAX_SAFE_INTEGER);

    const cliffMs = result.milestones.find(
      (m) => m.time === startTime + cliffSeconds
    );
    expect(cliffMs).toBeDefined();
    expect(cliffMs!.vested).toBe(flowRate * BigInt(cliffSeconds));
  });

  it("handles the user's example: totalAmount = 10_000_000_000_000_000n at mid-cliff", () => {
    // Exact repro of the bug report.
    const totalAmount = 10_000_000_000_000_000n;
    const startTime = 0;
    const endTime = 100_000_000;
    const flowRate = 100_000_000n; // totalAmount / (endTime - startTime) exactly
    const cliff = 25_000_000;
    const now = startTime + cliff / 2; // mid-cliff

    const stream = makeStream({
      flowRate,
      startTime,
      endTime,
      deposit: totalAmount,
    });

    const result = calculateVestingSchedule(stream, cliff, now);

    // Expected behaviour per the bug report:
    //   - totalAmount is exact (no rounding)
    //   - effectiveClaimable is 0 at mid-cliff
    expect(result.totalAmount).toBe(totalAmount);
    expect(result.effectiveClaimable).toBe(0n);
    expect(result.inCliff).toBe(true);

    // The cliff milestone's vested must equal flowRate × cliffSeconds exactly.
    const cliffMs = result.milestones[0];
    expect(cliffMs).toBeDefined();
    expect(cliffMs!.vested).toBe(flowRate * BigInt(cliff));
  });
});
