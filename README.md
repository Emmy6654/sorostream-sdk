# @sorostream/sdk

![npm](https://img.shields.io/npm/v/@sorostream/sdk)
![TypeScript](https://img.shields.io/badge/TypeScript-5-blue?logo=typescript)
![License](https://img.shields.io/badge/license-MIT-green)
![CI](https://github.com/SoroStream/sorostream-sdk/actions/workflows/test.yml/badge.svg)

TypeScript SDK for the **SoroStream** payment streaming protocol on Stellar Soroban. Stream USDC by the second for salaries, subscriptions, vesting schedules, and grant disbursements.

## Installation

```bash
npm install @sorostream/sdk
```

## Quick Start

```typescript
import { SoroStreamClient, createFreighterAdapter, toStroops } from "@sorostream/sdk";

// 1. Connect wallet
const walletAdapter = await createFreighterAdapter();

// 2. Create client
const client = new SoroStreamClient({
  network: "testnet",
  contractId: "YOUR_CONTRACT_ID",
  walletAdapter,
});

// 3. Create a stream: 100 USDC over 30 days
const { streamId, txHash } = await client.createStream({
  recipient: "GRECIPIENT_ADDRESS",
  token: "GUSDC_TOKEN_ADDRESS",
  amount: toStroops("100"),
  durationSeconds: 30 * 24 * 60 * 60,
  autoRenew: false,
});

// 4. Check claimable balance
const claimable = await client.getClaimable(streamId);

// 5. Withdraw
await client.withdraw({ streamId });
```

## API Reference

### `SoroStreamClient`

| Method | Description |
|--------|-------------|
| `createStream(params)` | Creates a new payment stream. Returns `{ streamId, txHash }` |
| `withdraw(params)` | Withdraws all claimable tokens. Returns `{ txHash, amount }` |
| `batchWithdraw(streamIds, batchSize?)` | Withdraws from multiple streams in one tx. Returns `BatchWithdrawResult[]` |
| `cancelStream(params)` | Cancels stream, refunds sender remainder. Returns `{ txHash }` |
| `topUp(params)` | Adds tokens, extends duration. Returns `{ txHash, newEndTime }` |
| `bulkCreateStreams(rows, options)` | Creates many streams at once (batched). Returns `BulkCreateResult` |
| `getStream(streamId)` | Returns full `Stream` object |
| `getClaimable(streamId)` | Returns claimable amount in stroops |
| `getStreamsBySender(sender)` | Returns all streams for a sender |
| `getStreamsByRecipient(recipient)` | Returns all streams for a recipient |
| `estimateCreateStreamFee(params)` | Estimates network fee for `createStream`. Returns `{ totalFee, minResourceFee }` |
| `estimateWithdrawFee(params)` | Estimates network fee for `withdraw`. Returns `{ totalFee, minResourceFee }` |
| `estimateCancelStreamFee(params)` | Estimates network fee for `cancelStream`. Returns `{ totalFee, minResourceFee }` |
| `estimateTopUpFee(params)` | Estimates network fee for `topUp`. Returns `{ totalFee, minResourceFee }` |

### Utilities

| Function | Description |
|----------|-------------|
| `toStroops(usdc)` | Converts USDC decimal string to stroops bigint |
| `formatUSDC(stroops)` | Formats stroops bigint to USDC string |
| `calculateFlowRate(amount, duration)` | Returns stroops/second flow rate |
| `claimableNow(stream)` | Estimates current claimable (client-side) |
| `timeUntilStreamEnd(stream)` | Returns seconds until stream ends |
| `calculateVestingSchedule(stream, cliffSeconds, now?)` | Display-only vesting schedule approximating a cliff. **Not enforced on-chain** |
| `watchClaimable(stream, reconcile, onTick, options?)` | Live counting-up ticker for claimable balance. Returns unsubscribe function |

### Client Options

| Option | Default | Description |
|--------|---------|-------------|
| `network` | — | Stellar network (`"mainnet"`, `"testnet"`, `"futurenet"`) |
| `contractId` | — | Deployed stream contract address |
| `walletAdapter` | — | Wallet adapter for signing |
| `rpcUrl?` | Default per network | Custom RPC URL override |
| `txTimeoutMs?` | `120000` | Max time (ms) to wait for transaction confirmation |
| `checkDuplicate?` | `false` | Heuristic check to warn/block duplicate stream creation |

All mutation methods (`createStream`, `withdraw`, `cancelStream`, `topUp`) accept an optional `AbortSignal` as the last argument to cancel in-flight transactions.

| Method | Description |
|--------|-------------|
| `executeBatch(operations)` | Submits multiple operations in a single transaction |
| `aggregateStreamsByToken(streams)` | Groups streams by token, returns per-token totals |
| `parseCsvStreamRows(csv)` | Parses CSV string into `BulkStreamRow[]` |

### Wallet

| Function | Description |
|----------|-------------|
| `createFreighterAdapter()` | Creates a WalletAdapter backed by Freighter extension |
| `createKeypairAdapter(secretKey)` | Creates a WalletAdapter from a Stellar secret key (server-side) |
| `createLedgerAdapter({ transport, path? })` | Creates a WalletAdapter backed by a Ledger device |
| `connectWallet()` | Prompts Freighter connection, returns public key |

The `WalletAdapter` interface (see `src/types.ts`) is the official extension point for custom signing backends. Implement `getPublicKey`, `signTransaction`, and `isConnected` to support any wallet or signing service.

### Deno and Bun Compatibility

The SDK is fully compatible with modern JS/TS runtimes, including **Deno** and **Bun**.

#### Bun Usage
```bash
bun install @sorostream/sdk
```
You can import the SDK and use it directly in Bun scripts:
```typescript
import { SoroStreamClient, createKeypairAdapter } from "@sorostream/sdk";
```

#### Deno Usage
You can run/import the SDK directly using NPM imports:
```typescript
import { SoroStreamClient, createKeypairAdapter } from "npm:@sorostream/sdk";
```

### Server-side Usage

For backend scripts and automated payouts, use `createKeypairAdapter`:

```typescript
import { SoroStreamClient, createKeypairAdapter, toStroops } from "@sorostream/sdk";

const adapter = createKeypairAdapter("SAZ...YOUR...SECRET...KEY...");
const client = new SoroStreamClient({
  network: "testnet",
  contractId: "YOUR_CONTRACT_ID",
  walletAdapter: adapter,
});

// Bulk-create payroll streams from CSV
const csv = `recipient,amount,durationSeconds
GABCD...1,100000000,2592000
GABCD...2,50000000,604800`;
const rows = parseCsvStreamRows(csv);
const { batches } = await client.bulkCreateStreams(rows, {
  token: "GUSDC_TOKEN_ADDRESS",
});
console.log(`Created ${batches.length} batch(es)`);
```

## Documentation

| Document | Description |
|----------|-------------|
| [Stream State Machine](./docs/state-machine.md) | Mermaid diagram of all stream states and valid / invalid transitions |
| [Rate Limiting](./docs/rate-limiting.md) | Default polling intervals, network call frequency, and tuning advice |
| [linear-vesting.ts](./examples/linear-vesting.ts) | Constant-rate stream with no cliff |
| [cliff-linear-vesting.ts](./examples/cliff-linear-vesting.ts) | Cliff period followed by linear release |
| [milestone-vesting.ts](./examples/milestone-vesting.ts) | Fixed tranches released at scheduled dates |

## Local Setup

```bash
npm install
npm test        # run unit tests
npm run lint    # type check
npm run build   # build to dist/
```

## Contributing via Drips Wave

This project participates in the **Stellar Wave Program** on [Drips Wave](https://drips.network/wave). Contributors earn rewards for resolving issues during weekly Wave sprints.

See [CONTRIBUTING.md](./CONTRIBUTING.md) for the full workflow.

> **Note:** Do not start coding until assigned to an issue by a maintainer.
