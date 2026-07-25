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
 * Wraps an async function with configurable exponential-backoff retry and full jitter.
 *
 * Uses the AWS "full jitter" formula to spread retry load:
 *   delay = random(0, min(maxDelayMs, baseDelayMs * 2^attempt))
 *
 * @param fn - Async function to execute.
 * @param options - Retry configuration.
 */
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
