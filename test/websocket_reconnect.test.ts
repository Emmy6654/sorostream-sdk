import { describe, it, expect, vi, afterEach } from "vitest";
import { watchClaimableWs } from "../src/utils.js";

describe("Issue #345 — WebSocket transport adapter reconnects automatically after disconnect", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("reconnects within backoff window after unexpected close and continues emitting events", () => {
    vi.useFakeTimers();

    const sockets: Array<{
      onopen: (() => void) | null;
      onmessage: ((event: { data: string }) => void) | null;
      onerror: (() => void) | null;
      onclose: (() => void) | null;
      send: ReturnType<typeof vi.fn>;
      close: ReturnType<typeof vi.fn>;
    }> = [];

    const webSocketFactory = vi.fn(() => {
      const socket = {
        onopen: null as (() => void) | null,
        onmessage: null as ((event: { data: string }) => void) | null,
        onerror: null as (() => void) | null,
        onclose: null as (() => void) | null,
        send: vi.fn(),
        close: vi.fn(),
      };
      sockets.push(socket);
      return socket as unknown as WebSocket;
    });

    const onClaimable = vi.fn();
    const stop = watchClaimableWs(
      "wss://example.com/ws",
      "100",
      onClaimable,
      undefined,
      webSocketFactory,
      { maxAttempts: 3, baseDelayMs: 200, maxDelayMs: 1000 }
    );

    // Initial socket created
    expect(webSocketFactory).toHaveBeenCalledTimes(1);
    const ws1 = sockets[0]!;
    ws1.onopen?.();

    // First event emitted
    ws1.onmessage?.({
      data: JSON.stringify({ type: "claimable", streamId: "100", value: "500" }),
    });
    expect(onClaimable).toHaveBeenLastCalledWith(500n);

    // Unexpected connection drop
    ws1.onclose?.();

    // Advance past backoff window (200ms for attempt 1)
    vi.advanceTimersByTime(200);

    // A new WebSocket instance should be created upon reconnect
    expect(webSocketFactory).toHaveBeenCalledTimes(2);
    const ws2 = sockets[1]!;
    ws2.onopen?.();

    // Resends subscription request
    expect(ws2.send).toHaveBeenCalledWith(
      JSON.stringify({ type: "subscribe", streamId: "100" })
    );

    // Continue emitting events on reconnected socket
    ws2.onmessage?.({
      data: JSON.stringify({ type: "claimable", streamId: "100", value: "1000" }),
    });
    expect(onClaimable).toHaveBeenLastCalledWith(1000n);

    stop();
    expect(ws2.close).toHaveBeenCalled();
  });

  it("confirms no duplicate events emitted when same value is received during reconnect", () => {
    vi.useFakeTimers();

    const sockets: Array<{
      onopen: (() => void) | null;
      onmessage: ((event: { data: string }) => void) | null;
      onerror: (() => void) | null;
      onclose: (() => void) | null;
      send: ReturnType<typeof vi.fn>;
      close: ReturnType<typeof vi.fn>;
    }> = [];

    const webSocketFactory = vi.fn(() => {
      const socket = {
        onopen: null as (() => void) | null,
        onmessage: null as ((event: { data: string }) => void) | null,
        onerror: null as (() => void) | null,
        onclose: null as (() => void) | null,
        send: vi.fn(),
        close: vi.fn(),
      };
      sockets.push(socket);
      return socket as unknown as WebSocket;
    });

    const onClaimable = vi.fn();
    const stop = watchClaimableWs(
      "wss://example.com/ws",
      "100",
      onClaimable,
      undefined,
      webSocketFactory,
      { maxAttempts: 3, baseDelayMs: 100 }
    );

    const ws1 = sockets[0]!;
    ws1.onopen?.();
    ws1.onmessage?.({
      data: JSON.stringify({ type: "claimable", streamId: "100", value: "500" }),
    });
    expect(onClaimable).toHaveBeenCalledTimes(1);

    // Unexpected close
    ws1.onclose?.();
    vi.advanceTimersByTime(100);

    const ws2 = sockets[1]!;
    ws2.onopen?.();

    // Duplicate value emitted on reconnected socket should be deduplicated
    ws2.onmessage?.({
      data: JSON.stringify({ type: "claimable", streamId: "100", value: "500" }),
    });
    expect(onClaimable).toHaveBeenCalledTimes(1);

    // New value emitted
    ws2.onmessage?.({
      data: JSON.stringify({ type: "claimable", streamId: "100", value: "600" }),
    });
    expect(onClaimable).toHaveBeenCalledTimes(2);
    expect(onClaimable).toHaveBeenLastCalledWith(600n);

    stop();
  });

  it("honors exponential backoff delay during consecutive reconnects", () => {
    vi.useFakeTimers();

    const sockets: Array<{
      onopen: (() => void) | null;
      onclose: (() => void) | null;
      send: ReturnType<typeof vi.fn>;
      close: ReturnType<typeof vi.fn>;
    }> = [];

    const webSocketFactory = vi.fn(() => {
      const socket = {
        onopen: null as (() => void) | null,
        onclose: null as (() => void) | null,
        send: vi.fn(),
        close: vi.fn(),
      };
      sockets.push(socket);
      return socket as unknown as WebSocket;
    });

    const stop = watchClaimableWs(
      "wss://example.com/ws",
      "100",
      vi.fn(),
      undefined,
      webSocketFactory,
      { maxAttempts: 5, baseDelayMs: 200, maxDelayMs: 1000 }
    );

    const ws1 = sockets[0]!;
    ws1.onopen?.();

    // First unexpected close -> reconnect scheduled with base delay 200ms
    ws1.onclose?.();
    expect(webSocketFactory).toHaveBeenCalledTimes(1);

    vi.advanceTimersByTime(150);
    expect(webSocketFactory).toHaveBeenCalledTimes(1); // Not yet

    vi.advanceTimersByTime(50); // total 200ms
    expect(webSocketFactory).toHaveBeenCalledTimes(2);

    // Second unexpected close -> backoff 200 * 2 = 400ms
    const ws2 = sockets[1]!;
    ws2.onclose?.();

    vi.advanceTimersByTime(350);
    expect(webSocketFactory).toHaveBeenCalledTimes(2);

    vi.advanceTimersByTime(50); // total 400ms
    expect(webSocketFactory).toHaveBeenCalledTimes(3);

    stop();
  });
});
