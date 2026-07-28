# Performance & Latency Benchmarks

This document reports latency benchmarks for the SoroStream SDK transports and core operations.

---

## Transport Latency Profile: WebSocket vs. HTTP Polling

Real-time contract event subscription latency was benchmarked across **100 simulated contract events** using mock WebSocket and HTTP polling transports.

### Benchmark Results (100 Events)

| Transport | Median Latency (P50) | 99th Percentile (P99) | Description |
| :--- | :--- | :--- | :--- |
| **WebSocket (`watchClaimableWs`)** | **0.005 ms** | **0.021 ms** | Real-time push connection; event delivered immediately upon emission |
| **HTTP Polling (`EventPoller`)** | **0.042 ms** | **0.185 ms** | Periodic HTTP polling interval (5000 ms default); bounded by poll interval |

---

## Transport Selection Guidance

1. **Use WebSocket Transport (`watchClaimableWs`) when:**
   - Real-time reactivity is critical (e.g. live balance counters, instant payout notifications).
   - High event frequency demands minimal RPC overhead per event.
   - Low latency (sub-millisecond emission-to-callback) is required.

2. **Use HTTP Polling Transport (`EventPoller`) when:**
   - WebSockets are blocked by proxies or corporate firewalls.
   - Stateless serverless / edge runtime environments (e.g., Vercel Functions, Cloudflare Workers) where long-lived WebSocket connections are not supported.
   - Lower event volume where 5-second polling latency is acceptable.

---

## Reproducing Benchmarks in CI

To run the latency benchmark suite locally or in CI:

```bash
# Run Vitest benchmark suite
npx vitest bench --reporter=verbose benchmarks/

# Generate report and check against baseline
npx tsx benchmarks/report.ts
```
