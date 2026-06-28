import { SoroStreamError } from "./errors.js";
import type {
  PriceFeedAdapter,
  Stream,
  BulkStreamRow,
  TokenAggregate,
  VestingScheduleResult,
  WatchClaimableOptions,
  FormatUSDCOptions,
  StreamDrift,
  ReconcileStreamOptions,
  StreamTotals,
  StatusBreakdown,
  DurationStats,
  StreamHealthReport,
  RecipientAggregate,
} from "./types.js";

/** A single point in a stream's payout forecast. */
export interface PayoutSchedulePoint {
  /** Unix timestamp (seconds) for this sample. */
  timestamp: number;
  /** Cumulative tokens claimable from stream start up to `timestamp`, in stroops. */
  cumulativeClaimable: bigint;
}

const STROOP_FACTOR = 10_000_000n;

/**
 * Converts a token amount (as a decimal string like "100.50") to stroops/smallest unit.
 * @param amount - Amount as a decimal string.
 * @param decimals - Number of decimal places the token uses (default 7 for SAC).
 */
export function toStroops(amount: string, decimals: number = 7): bigint {
  const [whole = "0", decimal = ""] = amount.split(".");
  const factor = 10n ** BigInt(decimals);
  const paddedDecimal = decimal.padEnd(decimals, "0").slice(0, decimals);
  return BigInt(whole) * factor + BigInt(paddedDecimal);
}

/**
 * Formats a stroop amount to a human-readable token string (e.g. "100.5000000").
 *
 * When `options` is provided, the result is locale-aware (grouping separators,
 * configurable decimal places). Without options, the existing precise
 * fixed-decimal string is returned unchanged — safe for calculations.
 *
 * @param stroops - Amount in the smallest token unit.
 * @param decimals - Number of decimal places the token uses (default 7 for SAC).
 * @param options - Optional locale formatting options.
 */
export function formatUSDC(
  stroops: bigint,
  decimals: number = 7,
  options?: FormatUSDCOptions
): string {
  const factor = 10n ** BigInt(decimals);
  const whole = stroops / factor;
  const remainder = stroops % factor;

  if (!options) {
    return `${whole}.${remainder.toString().padStart(decimals, "0")}`;
  }

  // Build a numeric value from the bigint parts to avoid precision loss.
  // `whole` and `remainder` are each individually within Number.MAX_SAFE_INTEGER
  // for any realistic token amount.
  const numericValue = Number(whole) + Number(remainder) / Number(factor);

  return new Intl.NumberFormat(options.locale, {
    minimumFractionDigits: options.minimumFractionDigits,
    maximumFractionDigits: options.maximumFractionDigits ?? decimals,
    useGrouping: options.useGrouping ?? true,
  }).format(numericValue);
}

/**
 * Generic alias for {@link formatUSDC}. Formats a stroop amount for any token.
 */
export function formatToken(stroops: bigint, decimals: number = 7): string {
  return formatUSDC(stroops, decimals);
}

/**
 * Converts a token amount to a fiat display value using a price feed adapter.
 *
 * @param stroops - Amount in the smallest token unit.
 * @param decimals - Number of decimal places the token uses.
 * @param priceFeed - Adapter that provides token-to-fiat pricing.
 * @param tokenAddress - The token contract address.
 * @param displayCurrency - Target currency code (default "usd").
 * @returns An object with both the token amount string and the fiat equivalent.
 */
export async function toFiatDisplay(
  stroops: bigint,
  decimals: number,
  priceFeed: PriceFeedAdapter,
  tokenAddress: string,
  displayCurrency = "usd"
): Promise<{ tokenAmount: string; fiatAmount: string }> {
  const tokenAmount = formatToken(stroops, decimals);
  const pricePerUnit = await priceFeed.getPrice(tokenAddress, displayCurrency);

  const factor = 10n ** BigInt(decimals);
  const whole = stroops / factor;
  const remainder = stroops % factor;
  const fractional = Number(remainder) / Number(factor);
  const numericAmount = Number(whole) + fractional;
  const fiatValue = numericAmount * pricePerUnit;

  const fiatAmount = fiatValue.toFixed(2);
  return { tokenAmount, fiatAmount };
}

/**
 * Checks whether a string looks like a valid Stellar address (account or contract).
 */
export function isValidStellarAddress(address: string): boolean {
  return (
    typeof address === "string" && /^[GC][A-Z2-7]{55}$/.test(address)
  );
}

/**
 * Calculates the per-second flow rate in stroops.
 * @param amount - Total amount in stroops.
 * @param durationSeconds - Duration in seconds.
 */
export function calculateFlowRate(
  amount: bigint,
  durationSeconds: number
): bigint {
  if (durationSeconds <= 0) throw new SoroStreamError("Duration must be > 0");
  return amount / BigInt(durationSeconds);
}

/**
 * Returns the number of seconds remaining until the stream ends.
 * Returns 0 if the stream has already ended.
 * @param stream - The stream object.
 */
export function timeUntilStreamEnd(stream: Stream): number {
  const now = Math.floor(Date.now() / 1000);
  return Math.max(0, stream.endTime - now);
}

/**
 * Calculates the currently claimable amount in stroops based on local time.
 * This is an estimate — the contract is the source of truth.
 * @param stream - The stream object.
 */
export function claimableNow(stream: Stream): bigint {
  if (stream.status !== "Active") return 0n;
  const now = Math.floor(Date.now() / 1000);
  const effectiveNow = Math.min(now, stream.endTime);
  const elapsed = Math.max(0, effectiveNow - stream.lastWithdrawTime);
  return stream.flowRate * BigInt(elapsed);
}

/**
 * Computes a display-only vesting schedule that approximates a cliff.
 *
 * The contract streams linearly from `startTime` with no cliff concept.
 * A "4-year vesting with 1-year cliff" can only be approximated by adjusting
 * the displayed schedule — **this is NOT enforced on-chain**.
 *
 * @param stream - The stream object.
 * @param cliffSeconds - Duration of the cliff period in seconds.
 * @param now - Optional override for "current" time (Unix seconds). Defaults to Date.now().
 */
export function calculateVestingSchedule(
  stream: Stream,
  cliffSeconds: number,
  now?: number
): VestingScheduleResult {
  const currentTime = now ?? Math.floor(Date.now() / 1000);
  const cliffEndTime = stream.startTime + cliffSeconds;
  const inCliff = currentTime < cliffEndTime;
  const totalSeconds = stream.endTime - stream.startTime;
  const totalAmount = stream.flowRate * BigInt(totalSeconds);

  let effectiveClaimable: bigint;
  if (inCliff) {
    effectiveClaimable = 0n;
  } else if (currentTime >= stream.endTime) {
    effectiveClaimable = stream.flowRate * BigInt(
      stream.endTime - Math.max(cliffEndTime, stream.startTime)
    );
  } else {
    const elapsed =
      Math.min(currentTime, stream.endTime) -
      Math.max(cliffEndTime, stream.startTime);
    effectiveClaimable = stream.flowRate * BigInt(Math.max(0, elapsed));
  }

  const milestones: Array<{ time: number; vested: bigint }> = [];

  if (cliffSeconds < totalSeconds) {
    milestones.push({
      time: cliffEndTime,
      vested: stream.flowRate * BigInt(cliffSeconds),
    });
  }

  for (const pct of [0.25, 0.5, 0.75, 1]) {
    const t = stream.startTime + Math.floor(totalSeconds * pct);
    if (t > cliffEndTime) {
      milestones.push({
        time: t,
        vested: stream.flowRate * BigInt(Math.floor(totalSeconds * pct)),
      });
    }
  }

  milestones.sort((a, b) => a.time - b.time);

  return {
    effectiveClaimable,
    totalAmount,
    cliffEndTime,
    inCliff,
    milestones,
  };
}

/**
 * Subscribes to real-time claimable balance updates via WebSocket.
 *
 * Returns an unsubscribe function. Falls back to a no-op if WebSocket is
 * unavailable or the connection fails, relying on the caller to provide
 * a fallback mechanism.
 *
 * @param wsUrl - WebSocket endpoint URL.
 * @param streamId - The stream ID to subscribe to.
 * @param onClaimable - Callback invoked with the latest on-chain claimable value.
 * @returns A function that closes the WS connection when called.
 *
 * @example
 * ```ts
 * const stop = watchClaimableWs("wss://rpc.example.com/ws", "42", (v) => {
 *   console.log("On-chain claimable:", v);
 * });
 * // later: stop();
 * ```
 */
export function watchClaimableWs(
  wsUrl: string,
  streamId: string,
  onClaimable: (claimable: bigint) => void
): () => void {
  let ws: WebSocket | null = null;
  let stopped = false;

  try {
    ws = new WebSocket(wsUrl);

    ws.onopen = () => {
      if (stopped) return;
      ws?.send(JSON.stringify({ type: "subscribe", streamId }));
    };

    ws.onmessage = (event: MessageEvent) => {
      if (stopped) return;
      try {
        const data = JSON.parse(event.data as string);
        if (data.type === "claimable" && data.streamId === streamId) {
          onClaimable(BigInt(data.value));
        }
      } catch {
        // swallow parse errors
      }
    };

    ws.onerror = () => {
      // Connection failed — silently no-op; caller should provide fallback
    };
  } catch {
    // WebSocket not supported in this environment
  }

  return () => {
    stopped = true;
    if (ws) {
      ws.onopen = null;
      ws.onmessage = null;
      ws.onerror = null;
      ws.close();
      ws = null;
    }
  };
}

/**
 * Creates a live "counting up" ticker for the claimable balance of a stream.
 *
 * Emits smoothly interpolated claimable values on an interval, reconciled
 * periodically against the on-chain `getClaimable` value. Returns an unsubscribe
 * function to stop the ticker.
 *
 * When `options.wsUrl` is provided, the function also subscribes to real-time
 * WebSocket updates from the RPC endpoint. The WS handler updates the base
 * value used for interpolation, providing more accurate estimates between
 * polling reconciliations. Falls back to polling-only if WS is unavailable.
 *
 * @param stream - The stream object.
 * @param reconcile - Async function that fetches the current on-chain claimable (typically `client.getClaimable(id)`).
 * @param onTick - Callback invoked with the current interpolated claimable value in stroops.
 * @param options - Optional configuration.
 * @returns A function that stops the ticker when called.
 *
 * @example
 * ```ts
 * const unsubscribe = watchClaimable(
 *   stream,
 *   () => client.getClaimable(stream.id),
 *   (claimable) => { displayElement.textContent = formatUSDC(claimable); }
 * );
 * // later: unsubscribe();
 * ```
 *
 * @example
 * ```ts
 * // With WebSocket subscription for real-time updates
 * const unsubscribe = watchClaimable(
 *   stream,
 *   () => client.getClaimable(stream.id),
 *   (claimable) => { displayElement.textContent = formatUSDC(claimable); },
 *   { wsUrl: "wss://rpc.example.com/ws", wsStreamId: stream.id }
 * );
 * ```
 */
export function watchClaimable(
  stream: Stream,
  reconcile: () => Promise<bigint>,
  onTick: (claimable: bigint) => void,
  options?: WatchClaimableOptions
): () => void {
  const tickMs = options?.tickMs ?? 200;
  const reconcileMs = options?.reconcileMs ?? 5_000;
  let baseValue = claimableNow(stream);
  let baseTime = Date.now();
  let stopped = false;

  onTick(baseValue);

  function emit() {
    if (stopped) return;
    const elapsedMs = Date.now() - baseTime;
    const perMs = Number(stream.flowRate) / 1000;
    const interpolated = baseValue + BigInt(Math.floor(perMs * elapsedMs));
    onTick(interpolated);
  }

  const tickTimer = setInterval(emit, tickMs);

  const reconcileTimer = setInterval(async () => {
    if (stopped) return;
    try {
      const actual = await reconcile();
      baseValue = actual;
      baseTime = Date.now();
      emit();
    } catch {
      // swallow — keep interpolating from last known value
    }
  }, reconcileMs);

  // Optional WebSocket subscription for real-time on-chain updates
  let stopWs: (() => void) | null = null;
  if (options?.wsUrl && options?.wsStreamId) {
    stopWs = watchClaimableWs(options.wsUrl, options.wsStreamId, (actual) => {
      baseValue = actual;
      baseTime = Date.now();
      emit();
    });
  }

  return () => {
    stopped = true;
    clearInterval(tickTimer);
    clearInterval(reconcileTimer);
    stopWs?.();
  };
}

// ── Issue #47: Cache reconciliation / drift detection ────────────────────────

const DRIFT_FIELDS: ReadonlyArray<keyof Stream> = [
  "status",
  "deposit",
  "flowRate",
  "endTime",
  "lastWithdrawTime",
  "autoRenew",
];

/**
 * Compares a cached stream against a fresh on-chain stream and returns any
 * fields that differ. Returns an empty array when there is no drift.
 *
 * Only mutable fields are compared (status, deposit, flowRate, endTime,
 * lastWithdrawTime, autoRenew). Immutable fields (id, sender, recipient,
 * token, startTime) are excluded.
 *
 * @param cached - The locally cached stream state.
 * @param onChain - The freshly fetched on-chain stream state.
 */
export function detectStreamDrift(cached: Stream, onChain: Stream): StreamDrift[] {
  const diffs: StreamDrift[] = [];
  for (const field of DRIFT_FIELDS) {
    if (String(cached[field]) !== String(onChain[field])) {
      diffs.push({ field, cached: cached[field], onChain: onChain[field] });
    }
  }
  return diffs;
}

/**
 * Periodically compares a cached stream against the on-chain state and invokes
 * `onDrift` whenever a difference is detected. Useful for catching missed
 * cache invalidations in long-running applications.
 *
 * Performs an immediate first check, then continues at the configured interval.
 * The internal reference is updated on every successful fetch so that callers
 * receive diffs relative to the most recent known state.
 *
 * @param stream - The initial cached stream.
 * @param fetchOnChain - Async function that returns the current on-chain stream.
 * @param onDrift - Called when drift is detected, with the diff list and fresh stream.
 * @param options - Optional configuration (intervalMs, default 30 000).
 * @returns Unsubscribe function that stops the watcher.
 *
 * @example
 * ```ts
 * const stop = watchStreamDrift(
 *   cachedStream,
 *   () => client.getStream(cachedStream.id),
 *   (diffs, fresh) => console.log("Drift detected:", diffs),
 * );
 * // later: stop();
 * ```
 */
export function watchStreamDrift(
  stream: Stream,
  fetchOnChain: () => Promise<Stream>,
  onDrift: (diffs: StreamDrift[], fresh: Stream) => void,
  options?: ReconcileStreamOptions
): () => void {
  const intervalMs = options?.intervalMs ?? 30_000;
  let current = stream;
  let stopped = false;

  async function check() {
    if (stopped) return;
    try {
      const fresh = await fetchOnChain();
      if (stopped) return; // re-check after async gap in case stop() was called
      const diffs = detectStreamDrift(current, fresh);
      current = fresh; // always update reference to the latest known state
      if (diffs.length > 0) {
        onDrift(diffs, fresh);
      }
    } catch {
      // swallow errors — keep watching from last known value
    }
  }

  // Immediate first check
  void check();

  const timer = setInterval(check, intervalMs);

  return () => {
    stopped = true;
    clearInterval(timer);
  };
}

/**
 * Checks whether an active stream is approaching its end time.
 * @param stream - The stream object.
 * @param thresholdSeconds - Seconds threshold (default 86400 = 24h).
 */
export function isStreamExpiring(stream: Stream, thresholdSeconds: number = 86400): boolean {
  if (stream.status !== "Active") return false;
  const remaining = timeUntilStreamEnd(stream);
  return remaining > 0 && remaining < thresholdSeconds;
}

/**
 * Checks whether an active stream has stalled (no recent withdrawals).
 * @param stream - The stream object.
 * @param staleThresholdSeconds - Seconds since last withdraw to consider stalled (default 604800 = 7d).
 */
export function isStreamStalled(stream: Stream, staleThresholdSeconds: number = 604800): boolean {
  if (stream.status !== "Active") return false;
  const now = Math.floor(Date.now() / 1000);
  return now - stream.lastWithdrawTime > staleThresholdSeconds;
}

/**
 * Checks whether a stream is under-funded — the remaining deposit is
 * insufficient to sustain the flow rate until the current end time.
 * This can happen when deposit rounding leaves a shortfall or when
 * top-up amounts were too small to meaningfully extend the stream.
 * @param stream - The stream object.
 */
export function isStreamUnderfunded(stream: Stream): boolean {
  if (stream.status !== "Active" || stream.flowRate === 0n) return false;
  const streamedSoFar = stream.flowRate * BigInt(stream.lastWithdrawTime - stream.startTime);
  const remainingDeposit = stream.deposit - streamedSoFar;
  const expectedRemaining = stream.flowRate * BigInt(stream.endTime - stream.lastWithdrawTime);
  return remainingDeposit < expectedRemaining;
}

// ── Token aggregation ─────────────────────────────────────────────────────────


/**
 * Groups streams by token address and returns per-token aggregates.
 * Uses the client-side `claimableNow` for claimable estimates.
 *
 * @param streams - Stream list (e.g. from getStreamsByRecipient).
 * @returns Per-token aggregates sorted by deposited amount descending.
 *
 * @example
 * ```ts
 * const streams = await client.getStreamsByRecipient(recipient);
 * const agg = aggregateStreamsByToken(streams);
 * for (const t of agg) console.log(t.token, t.claimable);
 * ```
 */
export function aggregateStreamsByToken(streams: Stream[]): TokenAggregate[] {
  const map = new Map<string, TokenAggregate>();

  for (const s of streams) {
    const existing = map.get(s.token) ?? {
      token: s.token,
      streamCount: 0,
      deposited: 0n,
      claimable: 0n,
      claimedSoFar: 0n,
    };
    existing.streamCount += 1;
    existing.deposited += s.deposit;
    existing.claimable += claimableNow(s);
    existing.claimedSoFar +=
      s.deposit - s.flowRate * BigInt(s.endTime - s.lastWithdrawTime);
    map.set(s.token, existing);
  }

  return [...map.values()].sort((a, b) => {
    if (b.deposited > a.deposited) return 1;
    if (b.deposited < a.deposited) return -1;
    return 0;
  });
}

// ── Dashboard / reporting aggregators ─────────────────────────────────────────

/**
 * Aggregates total deposited, claimable, claimed, and remaining amounts
 * across a set of streams.
 *
 * @param streams - Stream list.
 * @returns Aggregate totals.
 *
 * @example
 * ```ts
 * const totals = totalValueStreamed(streams);
 * console.log(totals.totalDeposited, totals.totalClaimable);
 * ```
 */
export function totalValueStreamed(streams: Stream[]): StreamTotals {
  let totalDeposited = 0n;
  let totalClaimable = 0n;
  let totalClaimed = 0n;
  let totalRemaining = 0n;

  for (const s of streams) {
    totalDeposited += s.deposit;
    totalClaimable += claimableNow(s);
    const claimed = s.deposit - s.flowRate * BigInt(s.endTime - s.lastWithdrawTime);
    totalClaimed += claimed > 0n ? claimed : 0n;
    totalRemaining += s.deposit - (claimed > 0n ? claimed : 0n);
  }

  return {
    totalStreams: streams.length,
    totalDeposited,
    totalClaimable,
    totalClaimed,
    totalRemaining,
  };
}

/**
 * Breaks down a set of streams by their status (Active, Cancelled, Completed).
 *
 * @param streams - Stream list.
 * @returns Per-status counts.
 */
export function aggregateStreamsByStatus(streams: Stream[]): StatusBreakdown {
  let active = 0;
  let cancelled = 0;
  let completed = 0;

  for (const s of streams) {
    if (s.status === "Active") active++;
    else if (s.status === "Cancelled") cancelled++;
    else if (s.status === "Completed") completed++;
  }

  return { active, cancelled, completed };
}

/**
 * Computes duration statistics (average, min, max, median) for a set of streams.
 * Durations are calculated as `endTime - startTime` for each stream.
 *
 * @param streams - Stream list.
 * @returns Duration statistics in seconds.
 */
export function averageStreamDuration(streams: Stream[]): DurationStats {
  if (streams.length === 0) {
    return { average: 0, min: 0, max: 0, median: 0 };
  }

  const durations = streams
    .map((s) => Math.max(0, s.endTime - s.startTime))
    .sort((a, b) => a - b);

  const sum = durations.reduce((a, b) => a + b, 0);
  const mid = Math.floor(durations.length / 2);

  return {
    average: Math.round(sum / durations.length),
    min: durations[0]!,
    max: durations[durations.length - 1]!,
    median: durations.length % 2 === 0
      ? Math.round((durations[mid - 1]! + durations[mid]!) / 2)
      : durations[mid]!,
  };
}

/**
 * Generates a health report for a set of streams, counting how many are
 * expiring, stalled, or underfunded.
 *
 * @param streams - Stream list.
 * @param expiringThresholdSeconds - Seconds threshold for expiry (default 86400 = 24h).
 * @param staleThresholdSeconds - Seconds since last withdraw for stall (default 604800 = 7d).
 * @returns Health report.
 */
export function streamHealthSummary(
  streams: Stream[],
  expiringThresholdSeconds = 86400,
  staleThresholdSeconds = 604800
): StreamHealthReport {
  let expiring = 0;
  let stalled = 0;
  let underfunded = 0;
  let totalActive = 0;

  for (const s of streams) {
    if (s.status !== "Active") continue;
    totalActive++;
    if (isStreamExpiring(s, expiringThresholdSeconds)) expiring++;
    if (isStreamStalled(s, staleThresholdSeconds)) stalled++;
    if (isStreamUnderfunded(s)) underfunded++;
  }

  return { expiring, stalled, underfunded, totalActive };
}

/**
 * Groups streams by recipient address and returns per-recipient aggregates.
 *
 * @param streams - Stream list.
 * @returns Per-recipient aggregates sorted by deposited amount descending.
 */
export function aggregateStreamsByRecipient(streams: Stream[]): RecipientAggregate[] {
  const map = new Map<string, RecipientAggregate>();

  for (const s of streams) {
    const existing = map.get(s.recipient) ?? {
      recipient: s.recipient,
      streamCount: 0,
      deposited: 0n,
      claimable: 0n,
      claimedSoFar: 0n,
    };
    existing.streamCount += 1;
    existing.deposited += s.deposit;
    existing.claimable += claimableNow(s);
    const claimed = s.deposit - s.flowRate * BigInt(s.endTime - s.lastWithdrawTime);
    existing.claimedSoFar += claimed > 0n ? claimed : 0n;
    map.set(s.recipient, existing);
  }

  return [...map.values()].sort((a, b) => {
    if (b.deposited > a.deposited) return 1;
    if (b.deposited < a.deposited) return -1;
    return 0;
  });
}

/**
 * Parses a CSV string into BulkStreamRow objects.
 *
 * Expected CSV format (header required):
 * ```
 * recipient,amount,durationSeconds
 * GABCD...1,10000000,2592000
 * GABCD...2,5000000,604800
 * ```
 *
 * `amount` is in stroops (bigint-compatible string).
 *
 * @param csv - The CSV content with header row.
 * @returns Parsed rows.
 */
export function parseCsvStreamRows(csv: string): BulkStreamRow[] {
  const lines = csv.trim().split(/\r?\n/);
  if (lines.length < 2)
    throw new Error("CSV must have a header row and at least one data row");

  const header = lines[0]!.toLowerCase().trim();
  const cols = header.split(",").map((c) => c.trim());

  const recipientIdx = cols.indexOf("recipient");
  const amountIdx = cols.indexOf("amount");
  const durationIdx = cols.indexOf("durationseconds");
  const tokenIdx = cols.indexOf("token");

  if (recipientIdx === -1) throw new Error("CSV missing 'recipient' column");
  if (amountIdx === -1) throw new Error("CSV missing 'amount' column");
  if (durationIdx === -1)
    throw new Error("CSV missing 'durationSeconds' column");

  const rows: BulkStreamRow[] = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i]!.trim();
    if (!line) continue;
    const fields = line.split(",").map((f) => f.trim());

    const recipient = fields[recipientIdx];
    if (!recipient) throw new Error(`Row ${i + 1}: missing recipient`);

    const amount = BigInt(fields[amountIdx] ?? "");
    const durationSeconds = Number(fields[durationIdx] ?? "0");

    if (!Number.isFinite(durationSeconds) || durationSeconds <= 0) {
      throw new Error(`Row ${i + 1}: invalid durationSeconds`);
    }

    const row: BulkStreamRow = { recipient, amount, durationSeconds };
    if (tokenIdx !== -1 && fields[tokenIdx]) {
      row.token = fields[tokenIdx];
    }

    rows.push(row);
  }

  return rows;
}

