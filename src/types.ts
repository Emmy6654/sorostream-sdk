/** Status of a payment stream. */
export type StreamStatus = "Active" | "Cancelled" | "Completed" | "Paused";

// ── Event types (#1) ─────────────────────────────────────────────────────────

export type StreamEventType =
  | "StreamCreated"
  | "StreamWithdrawn"
  | "StreamCancelled"
  | "StreamCompleted"
  | "StreamToppedUp"
  | "StreamPaused"
  | "StreamResumed"
  | "StreamTransferred";

export interface StreamEvent {
  type: StreamEventType;
  streamId: string;
  txHash: string;
  ledger: number;
  timestamp: number;
  data: Record<string, unknown>;
}

export interface StreamSubscription {
  unsubscribe(): void;
}

export interface StreamEventFilter {
  streamId?: string;
  sender?: string;
  recipient?: string;
}

// ── Pagination types (#3) ────────────────────────────────────────────────────

export interface PaginationParams {
  limit?: number;
  cursor?: string;
}

export interface PaginatedStreams {
  streams: Stream[];
  cursor: string | null;
  hasMore: boolean;
}

// ── Multisig types (#16) ─────────────────────────────────────────────────────

export interface MultisigSigner {
  signTransaction(xdr: string, network: Network): Promise<string>;
}

// ── Webhook types (#22) ──────────────────────────────────────────────────────

export interface WebhookConfig {
  url: string;
  headers?: Record<string, string>;
  retries?: number;
  retryDelayMs?: number;
}

/** A single payment stream as returned by the contract. */
export interface Stream {
  /** Unique stream identifier. */
  id: string;
  /** Address of the stream creator / payer. */
  sender: string;
  /** Address of the stream beneficiary. */
  recipient: string;
  /** SAC token contract address (e.g. USDC). */
  token: string;
  /** Total token deposit locked in stroops. */
  deposit: bigint;
  /** Tokens released per second in stroops. */
  flowRate: bigint;
  /** Unix timestamp when the stream started. */
  startTime: number;
  /** Unix timestamp when the stream ends. */
  endTime: number;
  /** Unix timestamp of the last withdrawal. */
  lastWithdrawTime: number;
  /** Current stream status. */
  status: StreamStatus;
  /** Whether the stream auto-renews on completion. */
  autoRenew: boolean;
  /** Unix timestamp when the stream was paused (undefined if not paused). */
  pausedAt?: number;
}

/** Parameters for creating a new stream. */
export interface CreateStreamParams {
  /** Beneficiary address. */
  recipient: string;
  /** SAC token contract address. */
  token: string;
  /** Total amount to stream in stroops. */
  amount: bigint;
  /** Stream duration in seconds. */
  durationSeconds: number;
  /** Whether to auto-renew on completion. */
  autoRenew: boolean;
  /** Opt-in check for duplicate stream creation. */
  checkDuplicate?: boolean;
}

/** Alias for a single stream creation params object. */
export type CreateStreamsParams = CreateStreamParams;

/** Parameters for withdrawing from a stream. */
export interface WithdrawParams {
  /** Stream ID to withdraw from. */
  streamId: string;
}

/** Parameters for cancelling a stream. */
export interface CancelStreamParams {
  /** Stream ID to cancel. */
  streamId: string;
}

/** Parameters for topping up a stream. */
export interface TopUpParams {
  /** Stream ID to top up. */
  streamId: string;
  /** Additional amount to add in stroops. */
  amount: bigint;
}

/** Network configuration. */
export type Network = "mainnet" | "testnet" | "futurenet";

/** Fee estimate returned by prepareTransaction. */
export interface FeeEstimate {
  /** Total fee in stroops (base fee + min resource fee). */
  totalFee: number;
  /** Soroban resource fee in stroops. */
  minResourceFee: number;
}

/** Result of batch cancellation. */
export interface BatchCancelResult {
  txHash: string;
  streamIds: string[];
}

/** Parameters for updating a stream's flow rate. */
export interface UpdateFlowRateParams {
  streamId: string;
  newFlowRate: bigint;
}

/** Parameters for setting an operator on a stream. */
export interface SetOperatorParams {
  streamId: string;
  operator: string;
  approved: boolean;
}

/** Parameters for an operator to top up a stream. */
export interface OperatorTopUpParams {
  streamId: string;
  amount: bigint;
}

/** Parameters for transferring a stream to a new recipient. */
export interface TransferStreamParams {
  streamId: string;
  newRecipient: string;
}

/** Parameters for pausing a stream. */
export interface PauseStreamParams {
  streamId: string;
}

/** Parameters for resuming a paused stream. */
export interface ResumeStreamParams {
  streamId: string;
}

/** A single milestone point in a vesting schedule. */
export interface VestingSchedulePoint {
  /** Unix timestamp of the milestone. */
  time: number;
  /** Amount vested in stroops at this point. */
  vested: bigint;
}

/** Result of a display-only vesting schedule calculation. */
export interface VestingScheduleResult {
  /** Effective claimable amount right now in stroops (0 if still in cliff). */
  effectiveClaimable: bigint;
  /** Total amount that vests over the full duration in stroops. */
  totalAmount: bigint;
  /** Unix timestamp when the cliff period ends. */
  cliffEndTime: number;
  /** Whether we are still in the cliff period. */
  inCliff: boolean;
  /** Schedule milestones for UI display (cliff, 25%, 50%, 75%, 100%). */
  milestones: VestingSchedulePoint[];
}

/** Options for {@link watchClaimable}. */
export interface WatchClaimableOptions {
  /** Interval in ms between interpolation ticks (default: 200). */
  tickMs?: number;
  /** Interval in ms between on-chain reconciliations (default: 5000). */
  reconcileMs?: number;
}


/** Wallet adapter interface. Implement this to support custom signing backends. */
export interface WalletAdapter {
  getPublicKey(): Promise<string>;
  signTransaction(xdr: string, network: Network): Promise<string>;
  isConnected(): Promise<boolean>;
}

/** A single row for bulk stream creation. */
export interface BulkStreamRow {
  recipient: string;
  amount: bigint;
  durationSeconds: number;
}

/** Options for bulkCreateStreams. */
export interface BulkCreateOptions {
  /** SAC token contract address applied to every row. */
  token: string;
  /** Whether auto-renew is enabled (default false). */
  autoRenew?: boolean;
  /** Max operations per transaction (default 8). */
  batchSize?: number;
}

/** Result of one batch within a bulk create. */
export interface BulkCreateBatchResult {
  txHash: string;
  streamIds: string[];
  rows: BulkStreamRow[];
}

/** Full result of bulkCreateStreams. */
export interface BulkCreateResult {
  batches: BulkCreateBatchResult[];
}

/** A stream that was skipped during a batch operation. */
export interface SkippedStream {
  id: string;
  reason: string;
}

/** Result of one transaction within a batchWithdraw call. */
export interface BatchWithdrawResult {
  txHash: string;
  streamIds: string[];
  amounts: string[];
  skipped?: SkippedStream[];
}

/** Per-token aggregate of a set of streams. */
export interface TokenAggregate {
  token: string;
  streamCount: number;
  deposited: bigint;
  claimable: bigint;
  claimedSoFar: bigint;
}

// ── Issue #44: Locale-aware formatUSDC ───────────────────────────────────────

/** Options for locale-aware {@link formatUSDC} formatting. */
export interface FormatUSDCOptions {
  /** BCP 47 locale string (e.g. "en-US", "de-DE"). */
  locale?: string;
  /** Maximum decimal digits to display. */
  maximumFractionDigits?: number;
  /** Minimum decimal digits to display. */
  minimumFractionDigits?: number;
  /** Whether to use grouping separators (e.g. commas in en-US). Default: true. */
  useGrouping?: boolean;
}

// ── Issue #47: Cache reconciliation / drift detection ────────────────────────

/** A single field that differs between cached and on-chain stream state. */
export interface StreamDrift {
  field: keyof Stream;
  cached: unknown;
  onChain: unknown;
}

/** Options for {@link watchStreamDrift}. */
export interface ReconcileStreamOptions {
  /** Interval in ms between on-chain reconciliation checks (default: 30000). */
  intervalMs?: number;
}

// ── Issue #46: WebAuthn passkey adapter ─────────────────────────────────────

/** Configuration for a WebAuthn/passkey-based Soroban smart wallet adapter. */
export interface PasskeyAdapterConfig {
  /** Deployed smart wallet contract address (becomes the wallet's public key). */
  contractId: string;
  /** WebAuthn relying party ID (e.g. "example.com"). */
  rpId: string;
  /**
   * The credential ID of the registered passkey (ArrayBuffer from credential.rawId).
   * Required — without it the browser may select the wrong passkey silently.
   */
  credentialId: ArrayBuffer;
}

// ── Price feed adapter (#Issue 1) ────────────────────────────────────────────

/**
 * Pluggable adapter for converting token amounts to fiat display values.
 * Implement this to back formatToken/toFiatDisplay with a price oracle or API.
 */
export interface PriceFeedAdapter {
  /**
   * Returns the price of one unit of the given token in the display currency.
   * @param tokenAddress - The token contract address (e.g. SAC address).
   * @param displayCurrency - Target currency code (default: "usd").
   * @returns Price per token unit in the display currency.
   */
  getPrice(tokenAddress: string, displayCurrency?: string): Promise<number>;
}

// ── Fee bump types (#Issue 3) ────────────────────────────────────────────────

/**
 * Options for wrapping a transaction in a Stellar fee-bump.
 * Allows an app operator to cover network fees on behalf of end users.
 */
export interface FeeBumpOptions {
  /** The Stellar address of the account paying network fees. */
  sponsorAddress: string;
  /** Wallet adapter for signing the fee-bump envelope. */
  sponsorAdapter: WalletAdapter;
  /** Maximum fee in stroops the sponsor is willing to pay. */
  maxFee?: number;
}

// ── Write options ────────────────────────────────────────────────────────────

/** Options for write operations (create, withdraw, cancel, top-up). */
export interface WriteOptions {
  /** If true, simulate only without submitting. */
  simulateOnly?: boolean;
  /** Override fee-bump for this specific transaction. */
  feeBump?: FeeBumpOptions;
}

// ── Contract versioning (#Issue 4) ───────────────────────────────────────────

/** Supported contract versions for call encoding. */
export type ContractVersion = "v1" | "v2";

// ── Stream filtering ────────────────────────────────────────────────────────

/** Criteria for filtering streams. */
export interface StreamFilterCriteria {
  sender?: string;
  recipient?: string;
  token?: string;
  status?: StreamStatus;
}
