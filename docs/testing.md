# Testing with MockSoroStreamClient

The `@sorostream/sdk/testing` sub-export provides `MockSoroStreamClient` — an in-memory drop-in replacement for `SoroStreamClient` that requires no network access.

## Installation

```bash
npm install @sorostream/sdk
```

## Basic Usage

```ts
import { MockSoroStreamClient } from "@sorostream/sdk/testing";

const mock = new MockSoroStreamClient();

const { streamId } = await mock.createStream({
  recipient: "GRECIPIENT_ADDRESS",
  token: "GUSDC_TOKEN_ADDRESS",
  amount: 1_000_000_000n,
  durationSeconds: 3600,
  autoRenew: false,
});

const claimable = await mock.getClaimable(streamId);
```

## Pre-configuring Stream Fixtures

Use `seedStream` to inject streams directly — useful for testing edge cases like already-cancelled or completed streams.

```ts
import { MockSoroStreamClient } from "@sorostream/sdk/testing";
import type { Stream } from "@sorostream/sdk";

const mock = new MockSoroStreamClient();

const now = Math.floor(Date.now() / 1000);
const fixture: Stream = {
  id: "42",
  sender: "GSENDER_ADDRESS",
  recipient: "GRECIPIENT_ADDRESS",
  token: "GUSDC_TOKEN_ADDRESS",
  deposit: 5_000_000_000n,
  flowRate: 1_388n,
  startTime: now - 3600,
  endTime: now + 3600,
  lastWithdrawTime: now - 3600,
  status: "Active",
  autoRenew: false,
};

mock.seedStream(fixture);

const stream = await mock.getStream("42");
console.log(stream.status); // "Active"
```

## Simulating Time Progression

Use `advanceTime` to simulate time passing without performing a withdrawal.

```ts
mock.advanceTime("42", 1800); // advance lastWithdrawTime by 30 minutes
const claimable = await mock.getClaimable("42");
```

## Configuring Failure Modes

To simulate a missing stream (e.g. RPC error or stream not found), simply don't seed the stream and call `getStream` or `withdraw` — the mock throws `Error("Stream not found: <id>")`.

```ts
await mock.getStream("999"); // throws: Stream not found: 999
```

To simulate an invalid state transition (e.g. withdraw from a cancelled stream), seed a stream with the desired status:

```ts
mock.seedStream({ ...fixture, id: "43", status: "Cancelled" });
await mock.withdraw({ streamId: "43" }); // throws: Stream is not active
```

## Subscribing to Events

`MockSoroStreamClient` emits the same events as the real client, making it easy to test event-driven UI code.

```ts
const sub = mock.subscribeEvents({ streamId: "42" }, (event) => {
  console.log(event.type); // "StreamWithdrawn", "StreamCompleted", etc.
});

await mock.withdraw({ streamId: "42" });

sub.unsubscribe();
```

## API Reference

| Method | Description |
|--------|-------------|
| `seedStream(stream)` | Inject a pre-built stream fixture |
| `advanceTime(streamId, seconds)` | Advance `lastWithdrawTime` without withdrawing |
| `setSender(address)` | Override the mock's simulated wallet address |
| `createStream(params)` | Creates a new in-memory stream |
| `withdraw(params)` | Withdraws claimable tokens; transitions to `Completed` at end |
| `cancelStream(params)` | Cancels an active stream |
| `topUp(params)` | Extends stream duration |
| `getStream(streamId)` | Returns a copy of the stream |
| `getClaimable(streamId)` | Returns current claimable amount in stroops |
| `getStreamsBySender(sender)` | Lists all streams for a sender |
| `getStreamsByRecipient(recipient)` | Lists all streams for a recipient |
| `subscribeEvents(filter, callback)` | Subscribes to stream events |
