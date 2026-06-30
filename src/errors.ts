// See ERRORS.md for cause, typical trigger, and recovery guidance for each
// error class below, and which SoroStreamClient methods throw them.

export class SoroStreamError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SoroStreamError";
  }
}

export class InsufficientAmountError extends SoroStreamError {
  constructor(message?: string) {
    super(message ?? "Amount must be > 0");
    this.name = "InsufficientAmountError";
  }
}

export class StreamNotFoundError extends SoroStreamError {
  constructor(streamId: string) {
    super(`Stream not found: ${streamId}`);
    this.name = "StreamNotFoundError";
  }
}

export class StreamNotActiveError extends SoroStreamError {
  constructor(streamId: string) {
    super(`Stream is not active: ${streamId}`);
    this.name = "StreamNotActiveError";
  }
}

export class TransactionFailedError extends SoroStreamError {
  constructor(details: string) {
    super(`Transaction failed: ${details}`);
    this.name = "TransactionFailedError";
  }
}

export class InvalidAddressError extends SoroStreamError {
  constructor(address: string) {
    super(`Invalid Stellar address: ${address}`);
    this.name = "InvalidAddressError";
  }
}

export class AccountNotFoundError extends SoroStreamError {
  constructor(address: string) {
    super(`Account not found on-chain: ${address}`);
    this.name = "AccountNotFoundError";
  }
}

export class InsufficientBalanceError extends SoroStreamError {
  constructor(message?: string) {
    super(message ?? "Insufficient token balance or missing trustline");
    this.name = "InsufficientBalanceError";
  }
}

export class ZeroDurationError extends SoroStreamError {
  constructor(message?: string) {
    super(message ?? "Stream duration must be greater than zero");
    this.name = "ZeroDurationError";
  }
}

/** Describes a single failed slot within a bulk create operation. */
export interface BulkCreateFailedSlot {
  /** Zero-based index of the row in the original `rows` array. */
  index: number;
  /** The row that failed. */
  row: import("./types.js").BulkStreamRow;
  /** The underlying error that caused the failure. */
  error: unknown;
}

/**
 * Thrown by `bulkCreateStreams` when one or more stream slots fail contract
 * validation or transaction submission.
 *
 * `successfulBatches` contains every batch that committed successfully;
 * `failedSlots` describes each row that could not be created.
 */
export class BulkCreatePartialError extends SoroStreamError {
  readonly successfulBatches: import("./types.js").BulkCreateBatchResult[];
  readonly failedSlots: BulkCreateFailedSlot[];

  constructor(
    successfulBatches: import("./types.js").BulkCreateBatchResult[],
    failedSlots: BulkCreateFailedSlot[]
  ) {
    const created = successfulBatches.reduce((n, b) => n + b.streamIds.length, 0);
    super(
      `Bulk create partially failed: ${created} stream(s) created, ${failedSlots.length} slot(s) failed`
    );
    this.name = "BulkCreatePartialError";
    this.successfulBatches = successfulBatches;
    this.failedSlots = failedSlots;
  }
}

export class InsufficientAllowanceError extends SoroStreamError {
  /** Token contract address that was checked. */
  readonly token: string;
  /** Allowance required to create the stream (in stroops). */
  readonly required: bigint;
  /** Current allowance granted to the contract (in stroops). */
  readonly current: bigint;

  constructor(token: string, required: bigint, current: bigint) {
    super(
      `Insufficient allowance for token ${token}: required ${required}, current ${current} (shortfall ${required - current})`
    );
    this.name = "InsufficientAllowanceError";
    this.token = token;
    this.required = required;
    this.current = current;
  }
}

export class DuplicateStreamError extends SoroStreamError {
  constructor(message = "Duplicate stream detected") {
    super(message);
    this.name = "DuplicateStreamError";
  }
}
