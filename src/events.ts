import { rpc, scValToNative, xdr } from "@stellar/stellar-sdk";
import type { StreamEvent, StreamEventType, StreamSubscription } from "./types.js";

/**
 * Retry policy for automatic event poller reconnection on unexpected failures.
 * Issue #186.
 */
export interface StreamRetryPolicy {
  /** Max reconnect attempts. 0 disables retry (default: 5). */
  maxAttempts?: number;
  /** Base delay in ms for exponential backoff (default: 1000). */
  baseDelayMs?: number;
  /** Maximum backoff delay cap in ms (default: 30000). */
  maxDelayMs?: number;
}

export interface EventPollerOptions {
  retryPolicy?: StreamRetryPolicy;
  onReconnecting?: (attempt: number, delayMs: number) => void;
  onReconnected?: () => void;
  onDisconnected?: (error: unknown) => void;
}

const STREAM_EVENT_NAMES = new Set([
  "StreamCreated",
  "StreamWithdrawn",
  "StreamCancelled",
  "StreamCompleted",
  "StreamToppedUp",
  "StreamPaused",
  "StreamResumed",
  "StreamTransferred",
]);

function isStreamEventType(value: string): value is StreamEventType {
  return STREAM_EVENT_NAMES.has(value);
}

function parseStreamEvent(raw: rpc.Api.EventResponse): StreamEvent | null {
  if (!raw.inSuccessfulContractCall) return null;

  const rawType = raw.topic.length > 0 ? scValToNative(raw.topic[0]!) : null;
  if (typeof rawType !== "string" || !isStreamEventType(rawType)) return null;

  const rawStreamId = raw.topic.length > 1 ? scValToNative(raw.topic[1]!) : null;
  const streamId = rawStreamId != null ? String(rawStreamId) : "0";

  const data = raw.value ? (scValToNative(raw.value) as Record<string, unknown>) : {};

  return {
    type: rawType,
    streamId,
    txHash: raw.txHash,
    ledger: raw.ledger,
    timestamp: new Date(raw.ledgerClosedAt).getTime(),
    data,
  };
}

export interface EventFilterFn {
  (event: StreamEvent): boolean;
}

export interface PollerEntry {
  filter: EventFilterFn;
  callback: (event: StreamEvent) => void;
}

/**
 * Polls the Soroban RPC for contract events and dispatches them
 * to matching subscribers.
 */
export class EventPoller {
  private server: rpc.Server;
  private contractId: string;
  private cursor: string | undefined;
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private entries: Map<string, PollerEntry> = new Map();
  private startLedger: number | null = null;

  // Issue #186: retry / reconnect support
  private readonly retryMaxAttempts: number;
  private readonly retryBaseDelayMs: number;
  private readonly retryMaxDelayMs: number;
  private readonly onReconnecting?: (attempt: number, delayMs: number) => void;
  private readonly onReconnected?: () => void;
  private readonly onDisconnected?: (error: unknown) => void;
  private consecutiveFailures = 0;
  private reconnecting = false;

  constructor(server: rpc.Server, contractId: string, options?: EventPollerOptions) {
    this.server = server;
    this.contractId = contractId;
    const rp = options?.retryPolicy ?? {};
    this.retryMaxAttempts = rp.maxAttempts ?? 5;
    this.retryBaseDelayMs = rp.baseDelayMs ?? 1_000;
    this.retryMaxDelayMs = rp.maxDelayMs ?? 30_000;
    this.onReconnecting = options?.onReconnecting;
    this.onReconnected = options?.onReconnected;
    this.onDisconnected = options?.onDisconnected;
  }

  subscribe(
    key: string,
    entry: PollerEntry
  ): StreamSubscription {
    this.entries.set(key, entry);
    if (!this.intervalId) {
      this.startPolling();
    }
    return {
      unsubscribe: () => {
        this.entries.delete(key);
        if (this.entries.size === 0) {
          this.stopPolling();
        }
      },
    };
  }

  private async poll(): Promise<void> {
    try {
      const response = await this.server.getEvents({
        startLedger: this.startLedger ?? undefined,
        filters: [
          {
            type: "contract",
            contractIds: [this.contractId],
          },
        ],
        cursor: this.cursor,
        limit: 100,
      });

      // Successful poll — reset failure tracking and emit reconnected if recovering
      if (this.consecutiveFailures > 0) {
        this.consecutiveFailures = 0;
        if (this.reconnecting) {
          this.reconnecting = false;
          this.onReconnected?.();
        }
      }

      if (response.events.length > 0) {
        if (this.startLedger === null) {
          this.startLedger = response.latestLedger;
        }
        this.cursor = response.cursor;

        for (const raw of response.events) {
          const event = parseStreamEvent(raw);
          if (!event) continue;
          for (const [, entry] of this.entries) {
            if (entry.filter(event)) {
              entry.callback(event);
            }
          }
        }
      } else if (this.startLedger === null) {
        this.startLedger = response.latestLedger;
      }
    } catch (err) {
      // Issue #186: exponential backoff with jitter on poll failure
      if (this.retryMaxAttempts === 0) return;

      this.consecutiveFailures++;

      if (this.consecutiveFailures > this.retryMaxAttempts) {
        this.onDisconnected?.(err);
        this.stopPolling();
        return;
      }

      // Full jitter: delay = random(0, min(maxDelay, base * 2^attempt))
      const cap = Math.min(
        this.retryMaxDelayMs,
        this.retryBaseDelayMs * 2 ** (this.consecutiveFailures - 1)
      );
      const delayMs = Math.floor(Math.random() * cap);

      this.reconnecting = true;
      this.onReconnecting?.(this.consecutiveFailures, delayMs);

      // Brief pause before the regular interval resumes the next attempt
      await new Promise<void>((r) => setTimeout(r, delayMs));
    }
  }

  private startPolling(): void {
    void this.poll();
    this.intervalId = setInterval(() => void this.poll(), 5000);
  }

  private stopPolling(): void {
    if (this.intervalId) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
  }

  destroy(): void {
    this.stopPolling();
    this.entries.clear();
  }
}
