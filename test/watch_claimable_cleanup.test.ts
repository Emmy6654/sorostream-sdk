import { describe, it, expect, vi, afterEach } from "vitest";
import { watchClaimable } from "../src/utils.js";
import type { Stream } from "../src/types.js";

const MOCK_STREAM: Stream = {
  id: "1",
  sender: "GSENDER",
  recipient: "GRECIPIENT",
  token: "GTOKEN",
  deposit: 1_000_000_000n,
  flowRate: 100n,        // 100 stroops/sec
  startTime: Math.floor(Date.now() / 1000) - 10,
  endTime: Math.floor(Date.now() / 1000) + 10000,
  lastWithdrawTime: Math.floor(Date.now() / 1000) - 10,
  status: "Active",
  autoRenew: false,
};

describe("#151 watchClaimable cleanup on unsubscribe", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("stops calling onTick after unsubscribe and leaves no timer leaks", () => {
    vi.useFakeTimers();

    const reconcile = vi.fn().mockResolvedValue(1000n);
    const onTick = vi.fn();

    const tickMs = 200;
    const unsubscribe = watchClaimable(MOCK_STREAM, reconcile, onTick, { tickMs, reconcileMs: 5000 });

    // One initial call happens synchronously before any timers fire
    const callsAfterStart = onTick.mock.calls.length;

    // Advance to fire a couple of ticks
    vi.advanceTimersByTime(tickMs * 3);
    const callsBeforeUnsub = onTick.mock.calls.length;
    expect(callsBeforeUnsub).toBeGreaterThan(callsAfterStart);

    // Unsubscribe
    unsubscribe();
    const callsAtUnsub = onTick.mock.calls.length;

    // Advance well past the polling interval
    vi.advanceTimersByTime(tickMs * 10);

    // No additional calls after unsubscribe
    expect(onTick.mock.calls.length).toBe(callsAtUnsub);

    // No timer leaks
    expect(vi.getTimerCount()).toBe(0);
  });
});
