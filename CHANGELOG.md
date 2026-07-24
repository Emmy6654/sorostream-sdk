# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog and uses the contributor guidance in CONTRIBUTING.md.

## [Unreleased]

### Added
- Add changelog entry format guide for contributors (#153)
- **#224** Add `benchmarks/` directory with a vitest bench suite measuring P50/P95/P99 latency for `getStream`, `getClaimable`, `createStream`, `withdraw`, `batchWithdraw`, and `getStreamsBySender` over 100 iterations per operation. Includes a JSON report generator (`benchmarks/report.ts`) with 20 % regression detection against a stored baseline, and a weekly scheduled CI workflow (`.github/workflows/benchmarks.yml`).
- **#231** Add `nonce` field to `CreateStreamParams` for caller-supplied idempotency keys. The SDK now calls `get_version` on the contract during `createStream` and emits a `console.warn` if the nonce is provided but the contract does not support it. Pass `strict: true` in `WriteOptions` to throw a `NonceNotSupportedError` instead. The `supportsNonce()` method is exposed for pre-flight capability checks.

### Fixed
- **#229** `batchWithdraw` no longer throws on the first failure. It now returns `BatchWithdrawPartialResult` — `{ successes: string[], failures: { id: string, error: Error }[] }` — so callers can safely retry only the failed stream IDs without double-withdrawing the successful ones. **Breaking change:** the return type changed from `BatchWithdrawResult[]` to `BatchWithdrawPartialResult`. Update call sites that caught thrown errors to inspect `result.failures` instead.
- **#230** `getStreamsBySender` and `getStreamsByRecipient` now cache results using a `${network}:${address}` key, preventing stale testnet data from being served after a `setNetwork("mainnet")` call. The sender/recipient caches are flushed alongside the stream cache in `setNetwork`. A mid-flight guard (matching the one on `getStream`) prevents a network switch that occurs between RPC dispatch and cache write from poisoning the new network's slot.

### Changed
- `NonceNotSupportedError` is now exported from `@sorostream/sdk` for use in typed `catch` blocks.
- `BatchWithdrawPartialResult` is now exported from `@sorostream/sdk`.
