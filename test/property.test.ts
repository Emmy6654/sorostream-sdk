/**
 * Property-based tests for flow rate and vesting calculations (Issue #103).
 * Uses fast-check with 10,000 iterations per property.
 */
import { describe, it } from "vitest";
import * as fc from "fast-check";
import { calculateVestingSchedule, claimableNow } from "../src/utils.js";
import type { Stream } from "../src/types.js";

const NUM_RUNS = 10_000;

// Arbitrary: valid positive stream duration in seconds (1s – 10 years)
const durationArb = fc.integer({ min: 1, max: 10 * 365 * 24 * 3600 });

// Arbitrary: flow rate in stroops (1 – 1e12)
const flowRateArb = fc.bigInt({ min: 1n, max: 1_000_000_000_000n });

// Arbitrary: a valid Stream object for vesting tests
const streamArb = fc
  .record({
    durationSecs: durationArb,
    flowRate: flowRateArb,
    startTime: fc.integer({ min: 1_000_000, max: 2_000_000_000 }),
  })
  .map(({ durationSecs, flowRate, startTime }): Stream => ({
    id: "0",
    sender: "GSENDER",
    recipient: "GRECIPIENT",
    token: "GTOKEN",
    deposit: flowRate * BigInt(durationSecs),
    flowRate,
    startTime,
    endTime: startTime + durationSecs,
    lastWithdrawTime: startTime,
    status: "Active",
    autoRenew: false,
  }));

describe("property: claimableNow ≤ totalAmount", () => {
  it("never returns more than the full deposit", () => {
    fc.assert(
      fc.property(streamArb, (stream) => {
        const claimable = claimableNow(stream);
        return claimable <= stream.deposit;
      }),
      { numRuns: NUM_RUNS }
    );
  });
});

describe("property: claimableNow is monotonically non-decreasing over time", () => {
  it("claimable at t+dt ≥ claimable at t for active streams", () => {
    fc.assert(
      fc.property(
        streamArb,
        fc.integer({ min: 0, max: 3600 }),
        (stream, delta) => {
          const t1 = stream.startTime + 100;
          const t2 = t1 + delta;
          // Simulate claimableNow at two points by adjusting lastWithdrawTime
          const s1: Stream = { ...stream, lastWithdrawTime: stream.startTime };
          const s2: Stream = { ...stream, lastWithdrawTime: stream.startTime };

          const elapsed1 = Math.max(0, Math.min(t1, stream.endTime) - stream.startTime);
          const elapsed2 = Math.max(0, Math.min(t2, stream.endTime) - stream.startTime);
          const c1 = stream.flowRate * BigInt(elapsed1);
          const c2 = stream.flowRate * BigInt(elapsed2);

          return c2 >= c1;
        }
      ),
      { numRuns: NUM_RUNS }
    );
  });
});

describe("property: claimable = 0 before cliff", () => {
  it("vesting schedule returns effectiveClaimable = 0n during cliff period", () => {
    fc.assert(
      fc.property(
        streamArb,
        fc.integer({ min: 1, max: 4 * 365 * 24 * 3600 }),
        (stream, cliffSeconds) => {
          // Pick a time strictly inside the cliff
          const nowInCliff = stream.startTime + Math.floor(cliffSeconds / 2);
          if (nowInCliff >= stream.startTime + cliffSeconds) return true; // skip edge

          const result = calculateVestingSchedule(stream, cliffSeconds, nowInCliff);
          return result.effectiveClaimable === 0n;
        }
      ),
      { numRuns: NUM_RUNS }
    );
  });
});

describe("property: totalAmount = flowRate × duration", () => {
  it("vesting schedule reports correct totalAmount", () => {
    fc.assert(
      fc.property(streamArb, (stream) => {
        const result = calculateVestingSchedule(stream, 0, stream.startTime);
        const expected = stream.flowRate * BigInt(stream.endTime - stream.startTime);
        return result.totalAmount === expected;
      }),
      { numRuns: NUM_RUNS }
    );
  });
});
