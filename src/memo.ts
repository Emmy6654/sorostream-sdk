import { Memo, MemoHash, MemoID, MemoNone, MemoText, MemoReturn } from '@stellar/stellar-sdk';
import type { MemoType } from '@stellar/stellar-sdk';
import { SoroStreamMemoError } from './errors.js';

/** Maximum byte length of a Soroban/Stellar text memo. */
const TEXT_MEMO_MAX_BYTES = 28;

/** Fixed byte length of a Soroban/Stellar hash memo. */
const HASH_MEMO_BYTES = 32;

/**
 * Encodes a string as a Stellar transaction text memo, validating that it
 * does not exceed the protocol's 28-byte limit.
 *
 * @param text - The memo text to encode.
 * @throws {SoroStreamMemoError} If `text` exceeds 28 bytes (UTF-8 encoded).
 *
 * @example
 * ```ts
 * const memo = encodeMemo("invoice-4821");
 * ```
 */
export function encodeMemo(text: string): Memo<MemoType.Text> {
  const byteLength = new TextEncoder().encode(text).byteLength;
  if (byteLength > TEXT_MEMO_MAX_BYTES) {
    throw new SoroStreamMemoError(
      `Text memo exceeds ${TEXT_MEMO_MAX_BYTES} bytes (got ${byteLength} bytes)`,
    );
  }
  return Memo.text(text);
}

/**
 * Encodes binary data as a Stellar transaction hash memo. Hash memos are
 * always exactly 32 bytes: shorter inputs are zero-padded, longer inputs
 * are truncated (with a warning) to fit.
 *
 * @param data - The bytes to encode as a hash memo.
 *
 * @example
 * ```ts
 * const memo = encodeMemoHash(sha256Digest);
 * ```
 */
export function encodeMemoHash(data: Buffer | Uint8Array): Memo<MemoType.Hash> {
  const input = Buffer.isBuffer(data) ? data : Buffer.from(data);
  if (input.length > HASH_MEMO_BYTES) {
    console.warn(
      `encodeMemoHash: input is ${input.length} bytes, truncating to ${HASH_MEMO_BYTES} bytes`,
    );
  }
  const padded = Buffer.alloc(HASH_MEMO_BYTES);
  input.copy(padded, 0, 0, Math.min(input.length, HASH_MEMO_BYTES));
  return Memo.hash(padded);
}

/**
 * Reads a memo from a transaction record and returns its decoded value.
 *
 * @param memo - The memo to decode.
 * @returns The decoded text/id (`string`) or hash/return value (`Buffer`),
 *   or `null` when the transaction has no memo.
 *
 * @example
 * ```ts
 * const value = decodeMemo(tx.memo); // string | Buffer | null
 * ```
 */
export function decodeMemo(memo: Memo): string | Buffer | null {
  switch (memo.type) {
    case MemoNone:
      return null;
    case MemoText:
    case MemoID:
      return memo.value as string;
    case MemoHash:
    case MemoReturn:
      return memo.value as Buffer;
    default:
      return null;
  }
}
