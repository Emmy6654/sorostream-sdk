import { SoroStreamRetryExhaustedError } from "./errors.js";
import type { RetryAttempt } from "./errors.js";

export interface RetryOptions {
  /** Maximum number of attempts (default: 3). */
  maxAttempts?: number;
  /** Base delay in ms for exponential backoff (default: 200). */
  baseDelayMs?: number;
  /** Maximum delay cap in ms (default: 5000). */
  maxDelayMs?: number;
  /** Optional AbortSignal to cancel retries mid-flight. */
  signal?: AbortSignal;
}

/**
 * Manages exponential backoff state for retry operations (issue #288).
 *
 * Backoff state is scoped per-request and resets to the base delay after
 * each successful request. This prevents isolated failures from being
 * penalized by previous failure streaks.
 */
export class RetryBackoff {
  private readonly baseDelayMs: number;
  private readonly maxDelayMs: number;
  /** Current backoff attempt counter, keyed by request identifier. */
  private readonly attemptCounts = new Map<string, number>();

  constructor(options?: { baseDelayMs?: number; maxDelayMs?: number }) {
    this.baseDelayMs = options?.baseDelayMs ?? 200;
    this.maxDelayMs = options?.maxDelayMs ?? 5_000;
  }

  /**
   * Records a successful request and resets the backoff for that request key.
   * @param key - The request identifier to reset.
   */
  onSuccess(key: string): void {
    this.attemptCounts.delete(key);
  }

  /**
   * Records a failed request and returns the next backoff delay.
   * @param key - The request identifier.
   * @returns The delay in ms before the next retry attempt.
   */
  onFailure(key: string): number {
    const attempt = this.attemptCounts.get(key) ?? 0;
    this.attemptCounts.set(key, attempt + 1);
    const cap = Math.min(this.maxDelayMs, this.baseDelayMs * 2 ** attempt);
    return Math.floor(Math.random() * cap);
  }

  /**
   * Returns the current attempt count for a request key.
   * @param key - The request identifier.
   * @returns The number of consecutive failures.
   */
  getAttemptCount(key: string): number {
    return this.attemptCounts.get(key) ?? 0;
  }

  /**
   * Resets the backoff state for a specific request key.
   * @param key - The request identifier to reset.
   */
  reset(key: string): void {
    this.attemptCounts.delete(key);
  }

  /**
   * Resets all backoff state.
   */
  resetAll(): void {
    this.attemptCounts.clear();
  }
}

/**
 * Wraps an async function with configurable exponential-backoff retry and full jitter.
 *
 * Uses the AWS "full jitter" formula to spread retry load:
 *   delay = random(0, min(maxDelayMs, baseDelayMs * 2^attempt))
 *
 * When all attempts are exhausted, throws {@link SoroStreamRetryExhaustedError}
 * with a full log of every attempt, the original error, and (when available)
 * the final RPC response body.
 *
 * @param fn - Async function to execute.
 * @param options - Retry configuration.
 */
export async function withRetry<T>(
  fn: () => Promise<T>,
  options?: RetryOptions
): Promise<T> {
  const maxAttempts = options?.maxAttempts ?? 3;
  const baseDelayMs = options?.baseDelayMs ?? 200;
  const maxDelayMs = options?.maxDelayMs ?? 5_000;
  const signal = options?.signal;

  const attempts: RetryAttempt[] = [];
  let lastError: unknown;
  let finalResponseBody: string | null = null;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    if (signal?.aborted) {
      throw new DOMException("Retry aborted", "AbortError");
    }

    try {
      return await fn();
    } catch (err) {
      lastError = err;
      attempts.push({
        attempt: attempt + 1,
        timestamp: Date.now(),
        error: err,
      });
      // Capture response body from RPC errors when available
      if (err && typeof err === "object") {
        const body = (err as Record<string, unknown>)["body"];
        if (body && typeof body === "string") {
          finalResponseBody = body;
        }
        const response = (err as Record<string, unknown>)["response"];
        if (response && typeof response === "object") {
          const responseBody = (response as Record<string, unknown>)["body"];
          if (responseBody && typeof responseBody === "string") {
            finalResponseBody = responseBody;
          }
        }
      }

      if (attempt < maxAttempts - 1) {
        const cap = Math.min(maxDelayMs, baseDelayMs * 2 ** attempt);
        const delay = Math.floor(Math.random() * cap);
        await new Promise<void>((resolve) => setTimeout(resolve, delay));
      }
    }
  }

  throw new SoroStreamRetryExhaustedError(
    lastError,
    maxAttempts,
    attempts,
    finalResponseBody
  );
}
