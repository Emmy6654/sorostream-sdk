import { rpc, scValToNative, xdr } from "@stellar/stellar-sdk";
import type { StreamEvent, StreamEventType, StreamSubscription } from "./types.js";

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

  constructor(server: rpc.Server, contractId: string) {
    this.server = server;
    this.contractId = contractId;
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
    } catch {
      // Swallow — will retry on next interval
    }
  }

  private startPolling(): void {
    this.poll();
    this.intervalId = setInterval(() => this.poll(), 5000);
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
