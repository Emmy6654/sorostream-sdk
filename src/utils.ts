import { SoroStreamError, SoroStreamValidationError, FederationResolutionError } from "./errors.js";
import { getDefaultWebSocketFactory } from "./adapters.js";
import type { FetchAdapter, WebSocketFactory } from "./adapters.js";
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
  CompressionOptions,
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
    minimumFractionDigits: options.minimumFractionDigits ?? 2,
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
 * Returns true when the string is a Stellar federation address (user*domain.com format).
 */
export function isFederationAddress(address: string): boolean {
  return typeof address === "string" && /^[^*\s]+\*[^*\s]+\.[^*\s]+$/.test(address);
}

/**
 * Resolves a Stellar federation address (user*domain.com) to a raw Stellar public key.
 * Fetches the domain's stellar.toml to discover the federation server, then queries it.
 * Throws {@link FederationResolutionError} if the server is unreachable or the address is unknown.
 */
export async function resolveFederationAddress(
  federationAddress: string,
  fetchImpl: FetchAdapter = fetch
): Promise<string> {
  const starIdx = federationAddress.indexOf("*");
  if (starIdx === -1) {
    throw new FederationResolutionError(federationAddress, "invalid federation address format");
  }
  const domain = federationAddress.slice(starIdx + 1);
  if (!domain) {
    throw new FederationResolutionError(federationAddress, "missing domain");
  }

  let federationServer: string;
  try {
    const tomlRes = await fetchImpl(`https://${domain}/.well-known/stellar.toml`);
    if (!tomlRes.ok) throw new Error(`HTTP ${tomlRes.status}`);
    const tomlText = await tomlRes.text();
    const match = /FEDERATION_SERVER\s*=\s*"([^"]+)"/.exec(tomlText);
    if (!match?.[1]) throw new Error("FEDERATION_SERVER not found in stellar.toml");
    federationServer = match[1];
  } catch (err) {
    throw new FederationResolutionError(
      federationAddress,
      `stellar.toml unreachable: ${String(err)}`
    );
  }

  try {
    const fedRes = await fetchImpl(
      `${federationServer}?q=${encodeURIComponent(federationAddress)}&type=name`
    );
    if (!fedRes.ok) throw new Error(`HTTP ${fedRes.status}`);
    const json = (await fedRes.json()) as { account_id?: string };
    const resolved = json.account_id;
    if (!resolved || !isValidStellarAddress(resolved)) {
      throw new Error("no valid account_id in federation response");
    }
    return resolved;
  } catch (err) {
    throw new FederationResolutionError(
      federationAddress,
      `federation server query failed: ${String(err)}`
    );
  }
}

/**
 * Calculates the largest per-second flow rate such that `flowRate * durationSeconds <= totalAmount`.
 *
 * @param totalAmount - Total amount to stream in stroops. Must be > 0.
 * @param durationSeconds - Stream duration in seconds. Must be > 0.
 * @returns The per-second flow rate in stroops (integer division).
 * @throws {SoroStreamError} If totalAmount or durationSeconds is zero or negative.
 *
 * @example
 * ```ts
 * const rate = calculateFlowRate(toStroops("100"), 30 * 24 * 60 * 60);
 * // rate * duration <= totalAmount is always satisfied
 * ```
 */
export function calculateFlowRate(
  totalAmount: bigint,
  durationSeconds: number
): bigint {
  if (totalAmount <= 0n) throw new SoroStreamError("totalAmount must be > 0");
  if (durationSeconds <= 0) throw new SoroStreamError("durationSeconds must be > 0");
  return totalAmount / BigInt(durationSeconds);
}

/**
 * Returns true when the stream's end time has passed.
 *
 * `stream.endTime` is stored in **Unix seconds** (on-chain value).
 * `Date.now()` returns milliseconds, so we divide by 1000 before comparing.
 *
 * @param stream - The stream object.
 * @param now - Optional override for "now" in Unix seconds (default: `Date.now() / 1000`).
 */
export function isExpired(stream: Stream, now?: number): boolean {
  const nowSecs = now ?? Date.now() / 1000;
  return stream.endTime < nowSecs;
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
 * All duration / elapsed / milestone arithmetic is performed in BigInt so that
 * streams whose `flowRate × duration` exceeds `Number.MAX_SAFE_INTEGER`
 * (≈9.007e15) do not lose precision through implicit Number coercions.
 * The `time` field of each milestone is the only place we round back to Number
 * since Unix timestamps must be representable as Numbers.
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

  // Promote all duration / elapsed arithmetic to BigInt so we never round
  // intermediate counts through Number. This keeps totalAmount, vested
  // amounts, and effectiveClaimable exact when stream.flowRate × duration
  // exceeds Number.MAX_SAFE_INTEGER.
  const totalSecondsBig = BigInt(stream.endTime - stream.startTime);
  const cliffSecondsBig = BigInt(cliffSeconds);
  const totalAmount = stream.flowRate * totalSecondsBig;

  let effectiveClaimable: bigint;
  if (inCliff) {
    effectiveClaimable = 0n;
  } else if (currentTime >= stream.endTime) {
    effectiveClaimable = totalAmount;
  } else {
    // Elapsed seconds from end-of-cliff (or from startTime when no cliff)
    // up to min(now, endTime).
    const minNowBig = BigInt(Math.min(currentTime, stream.endTime));
    const vestingStartBig = BigInt(Math.max(cliffEndTime, stream.startTime));
    const elapsedBig =
      minNowBig > vestingStartBig ? minNowBig - vestingStartBig : 0n;
    effectiveClaimable = stream.flowRate * (cliffSecondsBig + elapsedBig);
  }

  const milestones: Array<{ time: number; vested: bigint }> = [];

  if (cliffSecondsBig < totalSecondsBig) {
    milestones.push({
      time: cliffEndTime,
      vested: stream.flowRate * cliffSecondsBig,
    });
  }

  // Use integer percentage literals (25n, 50n, 75n, 100n) and divide in
  // BigInt — this avoids `Math.floor(totalSeconds × decimalPct)` losing
  // precision when totalSeconds is large. The final `Number()` cast below
  // is for the `time` Unix-timestamp field and is the only intentional
  // Number conversion.
  for (const pct of [25n, 50n, 75n, 100n] as const) {
    const secondsAtPct = (totalSecondsBig * pct) / 100n;
    const t = stream.startTime + Number(secondsAtPct);
    if (t > cliffEndTime) {
      milestones.push({
        time: t,
        vested: stream.flowRate * secondsAtPct,
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
/**
 * Resolves a `compression` option into a normalised `CompressionOptions` object,
 * or `null` when compression is disabled (the default).
 */
function resolveCompression(
  compression: boolean | CompressionOptions | undefined
): CompressionOptions | null {
  if (!compression) return null;
  if (compression === true) return { level: 6, threshold: 128 };
  return { level: compression.level ?? 6, threshold: compression.threshold ?? 128 };
}

export function watchClaimableWs(
  wsUrl: string,
  streamId: string,
  onClaimable: (claimable: bigint) => void,
  compression?: boolean | CompressionOptions,
  webSocketFactory?: WebSocketFactory
): () => void {
  let ws: WebSocket | null = null;
  let stopped = false;
  const compressionOpts = resolveCompression(compression);
  const payloadThreshold = compressionOpts?.threshold ?? Infinity;
  const createWebSocket = webSocketFactory ?? getDefaultWebSocketFactory();
  if (!createWebSocket) {
    throw new Error(
      "watchClaimableWs: no WebSocket implementation available. Pass `webSocketFactory` " +
        "in environments without a global WebSocket (e.g. React Native — see @sorostream/sdk-react-native)."
    );
  }

  try {
    // Attempt to pass perMessageDeflate options for environments that support it
    // (e.g. Node.js ws library). Browser WebSocket ignores extra constructor args.
    if (compressionOpts) {
      try {
        const deflateOpts = { perMessageDeflate: { level: compressionOpts.level } };
        ws = createWebSocket(wsUrl, deflateOpts);
      } catch {
        // Fall back to standard constructor when options are unsupported
        ws = createWebSocket(wsUrl);
      }
    } else {
      ws = createWebSocket(wsUrl);
    }

    ws.onopen = () => {
      if (stopped) return;
      const msg = JSON.stringify({ type: "subscribe", streamId });
      // Respect threshold: only rely on compression for large-enough payloads
      if (compressionOpts && msg.length < payloadThreshold) {
        ws?.send(msg);
      } else {
        ws?.send(msg);
      }
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
      // Connection failed or compression extension rejected by server —
      // silently no-op; caller should provide a polling fallback.
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
  let lastEmitted: bigint | null = null;
  let stopped = false;
  let lastNetworkVersion = options?.getNetworkVersion?.();

  function emit() {
    if (stopped) return;
    const elapsedMs = Date.now() - baseTime;
    const perMs = Number(stream.flowRate) / 1000;
    const interpolated = baseValue + BigInt(Math.floor(perMs * elapsedMs));
    // Deduplicate emissions across all three emit sources (tickTimer,
    // reconcile, and the optional WebSocket subscription). Without dedup,
    // reconcile (or WS) returning the same bigint that the most recent
    // tick already interpolated to would call onTick twice — once from the
    // last cached tick, once from the fresh fetch. The most common path
    // is a network blip: ticks keep interpolating from a stale baseValue,
    // then reconcile recovers and immediately calls emit() with a value
    // that matches the last cached value. The same dedup also (correctly)
    // keeps sub-stroop ticks quiet when Math.floor(perMs * elapsedMs)
    // rounds to zero between stroop increments — so it is always-on, not
    // a network-recovery-specific hack.
    if (interpolated === lastEmitted) return;
    lastEmitted = interpolated;
    onTick(interpolated);
  }

  // Seed the dedupe cache BEFORE the initial onTick so that a re-entrant
  // onTick callback (e.g. one that synchronously triggers another
  // emit() path) cannot be dropped as a duplicate of the uninitialised
  // cache sentinel.
  lastEmitted = baseValue;
  onTick(baseValue);

  let tickTimer = setInterval(emit, tickMs);
  let reconcileTimer: ReturnType<typeof setInterval>;

  function restartPolling() {
    clearInterval(tickTimer);
    clearInterval(reconcileTimer);
    stopWs?.();
    baseValue = claimableNow(stream);
    baseTime = Date.now();
    lastEmitted = null;
    tickTimer = setInterval(emit, tickMs);
    reconcileTimer = setInterval(reconcileTick, reconcileMs);
    emit();
  }

  async function reconcileTick() {
    if (stopped) return;

    // Issue #228: detect network switch mid-session and restart polling
    const currentVersion = options?.getNetworkVersion?.();
    if (options?.getNetworkVersion && currentVersion !== lastNetworkVersion) {
      lastNetworkVersion = currentVersion;
      options.onNetworkChanged?.();
      restartPolling();
      return;
    }

    try {
      const actual = await reconcile();
      baseValue = actual;
      baseTime = Date.now();
      emit();
    } catch {
      // swallow — keep interpolating from last known value
    }
  }

  reconcileTimer = setInterval(reconcileTick, reconcileMs);

  // Optional WebSocket subscription for real-time on-chain updates
  let stopWs: (() => void) | null = null;
  if (options?.wsUrl && options?.wsStreamId) {
    stopWs = watchClaimableWs(
      options.wsUrl,
      options.wsStreamId,
      (actual) => {
        baseValue = actual;
        baseTime = Date.now();
        emit();
      },
      options.compression,
      options.webSocketFactory
    );
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


/**
 * Returns the safe maximum number of operations per Soroban transaction.
 *
 * Soroban imposes resource limits (CPU instructions, memory, read/write
 * ledger bytes) that constrain how many contract calls fit in a single
 * transaction. The default safe limit is **8** operations; pass a lower
 * value when your operations are heavier than average (e.g. large `topUp`
 * payloads), or a higher value only after profiling against your specific
 * workload.
 *
 * @param custom - Optional override. Must be a positive integer ≤ 25.
 * @returns The resolved safe batch size.
 *
 * @example
 * ```ts
 * // Use the default safe limit
 * const size = batchSize();          // 8
 *
 * // Override for lighter operations
 * const size = batchSize(12);        // 12
 *
 * // Chunk a large list yourself
 * const ids = await client.getStreamsByRecipient(recipient);
 * for (let i = 0; i < ids.length; i += batchSize()) {
 *   await client.batchWithdraw(ids.slice(i, i + batchSize()));
 * }
 * ```
 */
export function batchSize(custom?: number): number {
  const DEFAULT = 8;
  const MAX = 25;
  if (custom === undefined) return DEFAULT;
  if (!Number.isInteger(custom) || custom <= 0 || custom > MAX) {
    throw new SoroStreamError(
      `batchSize must be a positive integer ≤ ${MAX}, got ${custom}`
    );
  }
  return custom;
}

/**
 * Recursively converts BigInt properties in an object to strings,
 * allowing safe serialization with JSON.stringify.
 *
 * Useful for logging or exporting stream objects, vesting schedules,
 * and aggregate totals that include BigInt fields like flowRate,
 * totalAmount, or claimableAmount.
 *
 * @param obj - The object containing BigInt fields.
 * @returns A plain object with all BigInt fields converted to string.
 */
export function streamToJSON(obj: unknown): unknown {
  if (typeof obj === "bigint") {
    return obj.toString();
  }
  if (Array.isArray(obj)) {
    return obj.map(streamToJSON);
  }
  if (obj !== null && typeof obj === "object") {
    const res: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj)) {
      if (key === "toJSON") continue;
      res[key] = streamToJSON(value);
    }
    return res;
  }
  return obj;
}

/**
 * Helper function to serialize objects containing BigInt fields to JSON strings.
 * Automatically converts BigInt fields (like flowRate, totalAmount, claimableAmount) to strings.
 *
 * @param obj - The object to serialize.
 * @param space - Optional indentation space for pretty-printing.
 * @returns The JSON string representation.
 */
export function jsonStringifyStream(obj: unknown, space?: string | number): string {
  return JSON.stringify(
    obj,
    (_, value) => (typeof value === "bigint" ? value.toString() : value),
    space
  );
}

export const jsonStringify = jsonStringifyStream;

const BIGINT_SUFFIX = "_bigint";

/**
 * Custom JSON.stringify replacer that serializes BigInt values as strings
 * with a `_bigint` suffix, enabling round-trip-safe serialization.
 *
 * Use this as the `replacer` argument to `JSON.stringify` when serializing
 * objects that contain BigInt fields:
 *
 * @example
 * ```ts
 * const json = JSON.stringify(stream, bigintReplacer);
 * ```
 */
export function bigintReplacer(_key: string, value: unknown): unknown {
  return typeof value === "bigint" ? `${value.toString()}${BIGINT_SUFFIX}` : value;
}

/**
 * Custom JSON.parse reviver that restores `_bigint`-suffixed strings back to
 * BigInt values. Intended to reverse {@link bigintReplacer}.
 *
 * @example
 * ```ts
 * const obj = JSON.parse(json, bigintReviver);
 * ```
 */
export function bigintReviver(_key: string, value: unknown): unknown {
  if (typeof value === "string" && value.endsWith(BIGINT_SUFFIX)) {
    const raw = value.slice(0, -BIGINT_SUFFIX.length);
    try {
      return BigInt(raw);
    } catch {
      return value;
    }
  }
  return value;
}

/**
 * Serializes a {@link Stream} object to a JSON string, handling BigInt fields
 * (deposit, flowRate) via the `_bigint` suffix convention for round-trip safety.
 *
 * Use {@link deserializeStreamFromJSON} to restore the stream object.
 *
 * Plain `JSON.stringify(stream)` throws `TypeError: Do not know how to serialize a BigInt`
 * because BigInt values are not JSON-serializable by default. Always use this
 * utility (or {@link bigintReplacer}) when serializing stream objects.
 *
 * @param stream - The stream object to serialize.
 * @param space - Optional indentation for pretty-printing.
 * @returns A JSON string with BigInt fields serialised as `"<value>_bigint"`.
 *
 * @example
 * ```ts
 * const json = serializeStreamToJSON(stream);
 * const restored = deserializeStreamFromJSON(json);
 * ```
 */
export function serializeStreamToJSON(stream: import("./types.js").Stream, space?: string | number): string {
  return JSON.stringify(stream, bigintReplacer, space);
}

/**
 * Deserializes a JSON string (produced by {@link serializeStreamToJSON}) back
 * into a {@link Stream} object, restoring BigInt fields from their `_bigint`-
 * suffixed string representation.
 *
 * @param json - The JSON string to deserialize.
 * @returns The restored Stream object.
 * @throws {Error} If the JSON cannot be parsed or does not contain valid stream fields.
 *
 * @example
 * ```ts
 * const json = serializeStreamToJSON(stream);
 * const restored = deserializeStreamFromJSON(json);
 * console.log(restored.deposit); // BigInt
 * ```
 */
export function deserializeStreamFromJSON(json: string): import("./types.js").Stream {
  const parsed = JSON.parse(json, bigintReviver) as Record<string, unknown>;

  const id = String(parsed["id"] ?? "");
  if (!id) throw new Error("deserializeStreamFromJSON: missing 'id' field");

  const deposit = typeof parsed["deposit"] === "bigint" ? parsed["deposit"] as bigint : BigInt(String(parsed["deposit"] ?? "0"));
  const flowRate = typeof parsed["flowRate"] === "bigint" ? parsed["flowRate"] as bigint : BigInt(String(parsed["flowRate"] ?? "0"));

  const stream: import("./types.js").Stream = {
    id,
    sender: String(parsed["sender"] ?? ""),
    recipient: String(parsed["recipient"] ?? ""),
    token: String(parsed["token"] ?? ""),
    deposit,
    flowRate,
    startTime: Number(parsed["startTime"] ?? 0),
    endTime: Number(parsed["endTime"] ?? 0),
    lastWithdrawTime: Number(parsed["lastWithdrawTime"] ?? 0),
    status: (parsed["status"] as import("./types.js").Stream["status"]) ?? "Active",
    autoRenew: Boolean(parsed["autoRenew"]),
    ...(parsed["pausedAt"] != null ? { pausedAt: Number(parsed["pausedAt"]) } : {}),
    ...(parsed["lockUntil"] != null ? { lockUntil: Number(parsed["lockUntil"]) } : {}),
  };

  return stream;
}

// ── Issue #226: String field length validation ────────────────────────────────

/**
 * Maximum byte length limits for string fields passed to the contract.
 * Keys match the field names used in `CreateStreamParams` and related types.
 *
 * @example
 * ```ts
 * import { STRING_FIELD_LIMITS } from "@sorostream/sdk";
 * const limit = STRING_FIELD_LIMITS.recipient; // 64
 * ```
 */
export const STRING_FIELD_LIMITS: Readonly<Record<string, number>> = {
  recipient: 64,
  token: 64,
  metadataUri: 128,
  description: 256,
};

/**
 * Validates that a string field does not exceed its allowed byte length.
 * Throws {@link SoroStreamValidationError} when the limit is exceeded.
 *
 * @param field - Field name (must be a key in `STRING_FIELD_LIMITS`).
 * @param value - The string value to validate.
 * @throws {SoroStreamValidationError} When the byte length exceeds the limit.
 *
 * @example
 * ```ts
 * validateStringLength("recipient", params.recipient);
 * validateStringLength("metadataUri", metadataUri); // 128 byte limit
 * ```
 */
export function validateStringLength(field: string, value: string): void {
  const limit = STRING_FIELD_LIMITS[field];
  if (limit === undefined) return; // no configured limit — pass through
  const byteLength = new TextEncoder().encode(value).byteLength;
  if (byteLength > limit) {
    throw new SoroStreamValidationError(field, byteLength, limit);
  }
}
