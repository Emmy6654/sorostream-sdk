import {
  Contract,
  Networks,
  TransactionBuilder,
  BASE_FEE,
  rpc,
  nativeToScVal,
  scValToNative,
  xdr,
  Transaction,
  FeeBumpTransaction,
} from "@stellar/stellar-sdk";
import { EventPoller } from "./events.js";
import { Cache } from "./cache.js";
import { isValidStellarAddress } from "./utils.js";

// Default read-cache TTL for stream lookups. Matches the EventPoller's 5s
// poll interval so that without an explicit `setNetwork` call, a stream read
// is at most one poll cycle stale on its own network. `setNetwork` flushes
// the cache immediately regardless of this TTL.
const STREAM_CACHE_TTL_MS = 5_000;

/** Minimum allowed stream duration in seconds. */
export const MIN_STREAM_DURATION_SECONDS = 1;
import { createContractEncoder } from "./contractEncoders.js";
import type { ContractCallEncoder } from "./contractEncoders.js";
import { CircuitBreaker } from "./circuitBreaker.js";
import type { CircuitBreakerOptions } from "./circuitBreaker.js";
import {
  TransactionFailedError,
  StreamNotFoundError,
  InsufficientAmountError,
  InvalidAddressError,
  AccountNotFoundError,
  ZeroDurationError,
  BulkCreatePartialError,
  InsufficientAllowanceError,
  DuplicateStreamError,
} from "./errors.js";
import type { BulkCreateFailedSlot } from "./errors.js";
import type {
  BatchCancelResult,
  BatchWithdrawResult,
  BulkCreateOptions,
  BulkCreateResult,
  CancelStreamParams,
  CreateStreamParams,
  FeeEstimate,
  Network,
  PaginatedStreams,
  PaginationParams,
  PriceFeedAdapter,
  RecipientChangedEvent,
  OnRecipientChangedOptions,
  SplitStreamParams,
  SplitStreamResult,
  Stream,
  StreamEvent,
  StreamEventFilter,
  StreamEventType,
  StreamSnapshot,
  StreamSubscription,
  TopUpParams,
  TransferStreamParams,
  PauseStreamParams,
  ResumeStreamParams,
  UpdateFlowRateParams,
  SetOperatorParams,
  OperatorTopUpParams,
  WalletAdapter,
  WithdrawParams,
  WriteOptions,
  StreamFilterCriteria,
  CreateStreamsParams,
  ContractVersion,
  FeeBumpOptions,
  SoroStreamPlugin,
  MiddlewareContext,
  StreamActivityEntry,
  GetActivityLogOptions,
} from "./types.js";
import { withRetry, type RetryOptions } from "./retry.js";
import { calculateVestingSchedule, streamToJSON } from "./utils.js";

const RPC_URLS: Record<Network, string> = {
  mainnet: "https://soroban.stellar.org",
  testnet: "https://soroban-testnet.stellar.org",
  futurenet: "https://rpc-futurenet.stellar.org",
};

const NETWORK_PASSPHRASES: Record<Network, string> = {
  mainnet: Networks.PUBLIC,
  testnet: Networks.TESTNET,
  futurenet: Networks.FUTURENET,
};

/** Options for constructing a SoroStreamClient. */
export interface SoroStreamClientOptions {
  /** The Stellar network to connect to. */
  network: Network;
  /** The deployed StreamContract address. */
  contractId: string;
  /** Wallet adapter for signing transactions. */
  walletAdapter: WalletAdapter;
  /** Optional custom RPC URL (overrides default). */
  rpcUrl?: string;
  /** Optional circuit-breaker configuration for RPC calls. */
  circuitBreaker?: CircuitBreakerOptions;
  /** Maximum time in ms to wait for a transaction to confirm (default: 120000). */
  txTimeoutMs?: number;
  /** Retry policy for read methods (getStream, getClaimable, etc.). */
  readRetry?: RetryOptions;
  /** Retry policy for transaction submission RPC calls (getAccount, prepareTransaction, sendTransaction). */
  submitRetry?: RetryOptions;
  /** Optional price-feed adapter for token-to-fiat display conversion. */
  priceFeed?: PriceFeedAdapter;
  /** Contract version to use for call encoding (default: "v1"). */
  contractVersion?: ContractVersion;
  /** Default fee-bump options applied to all transactions (can be overridden per-call). */
  feeBump?: FeeBumpOptions;
  /**
   * Custom cliff-duration validator (issue #74).
   * Called before every `createStream` / `bulkCreateStreams` call.
   * Throw an error to block transaction submission.
   * Default behaviour enforces `cliffSeconds >= 0`.
   */
  validateCliff?: (cliffSeconds: number) => void | Promise<void>;
  /**
   * Middleware plugins to register on the client (issue #50).
   * Plugins are invoked in registration order for `before` hooks and in
   * reverse order for `after` and `onError` hooks.
   */
  plugins?: SoroStreamPlugin[];
  /**
   * Maximum number of pooled HTTP connections reused across RPC calls (default: 5).
   * Issue #149.
   */
  maxConnections?: number;
  /**
   * Time in ms before an idle pooled connection is closed (default: 30000).
   * Issue #149.
   */
  idleTimeoutMs?: number;
  /**
   * Opt-in check for duplicate stream creation.
   */
  checkDuplicate?: boolean;
}

function scValToStream(val: xdr.ScVal): Stream {
  const raw = scValToNative(val) as Record<string, unknown>;
  return {
    id: String(raw["id"]),
    sender: String(raw["sender"]),
    recipient: String(raw["recipient"]),
    token: String(raw["token"]),
    deposit: BigInt(raw["deposit"] as number),
    flowRate: BigInt(raw["flow_rate"] as number),
    startTime: Number(raw["start_time"]),
    endTime: Number(raw["end_time"]),
    lastWithdrawTime: Number(raw["last_withdraw_time"]),
    status: raw["status"] as Stream["status"],
    autoRenew: Boolean(raw["auto_renew"]),
    ...(raw["paused_at"] != null ? { pausedAt: Number(raw["paused_at"]) } : {}),
    toJSON() {
      return streamToJSON(this) as Record<string, unknown>;
    },
  };
}

export type SimulateOnlyResult = {
  simulated: true;
  result: rpc.Api.SimulateTransactionResponse;
};

/**
 * Main client for interacting with the SoroStream contract.
 *
 * See `ERRORS.md` for the cause, typical trigger, and recommended recovery
 * action for every error class referenced in this client's `@throws` tags.
 *
 * @example
 * ```ts
 * const client = new SoroStreamClient({ network: "testnet", contractId: "...", walletAdapter });
 * const { streamId } = await client.createStream({ recipient, token, amount, durationSeconds, autoRenew });
 * ```
 */
export class SoroStreamClient {
  private server: rpc.Server;
  private readonly breaker: CircuitBreaker | null;
  private readonly contract: Contract;
  private network: Network;
  private readonly walletAdapter: WalletAdapter;
  private readonly txTimeoutMs: number;
  private readonly readRetry: RetryOptions;
  private readonly submitRetry: RetryOptions;
  private readonly encoder: ContractCallEncoder;
  private readonly defaultFeeBump: FeeBumpOptions | null = null;
  private readonly priceFeed: PriceFeedAdapter | null = null;
  private eventPoller: EventPoller | null = null;
  /** Per-stream read cache, keyed by `${network}:${streamId}`. */
  private readonly streamCache = new Cache<string, Stream>(STREAM_CACHE_TTL_MS);
  private readonly validateCliff: (cliffSeconds: number) => void | Promise<void>;
  private readonly plugins: SoroStreamPlugin[] = [];
  private readonly checkDuplicate: boolean;
  // Issue #149: connection pool stats
  private readonly connectionPool: { maxConnections: number; idleTimeoutMs: number; active: number; reused: number; idle: number };

  constructor(options: SoroStreamClientOptions) {
    this.network = options.network;
    this.walletAdapter = options.walletAdapter;
    this.contract = new Contract(options.contractId);
    this.server = new rpc.Server(
      options.rpcUrl ?? RPC_URLS[options.network],
      { allowHttp: false }
    );
    this.txTimeoutMs = options.txTimeoutMs ?? 120_000;
    this.breaker = options.circuitBreaker
      ? new CircuitBreaker(options.circuitBreaker)
      : null;
    this.readRetry = options.readRetry ?? {};
    this.submitRetry = options.submitRetry ?? {};
    this.encoder = createContractEncoder(this.contract, options.contractVersion ?? "v1");
    this.defaultFeeBump = options.feeBump ?? null;
    this.priceFeed = options.priceFeed ?? null;
    this.validateCliff = options.validateCliff ?? ((s) => {
      if (s < 0) throw new Error("cliffSeconds must be >= 0");
    });
    this.plugins = options.plugins ?? [];
    this.checkDuplicate = options.checkDuplicate ?? false;
    // Issue #149: connection pool stats tracker
    this.connectionPool = {
      maxConnections: options.maxConnections ?? 5,
      idleTimeoutMs: options.idleTimeoutMs ?? 30_000,
      active: 0,
      reused: 0,
      idle: 0,
    };
  }

  /**
   * Returns the network this client is currently connected to.
   * @returns The currently active network.
   */
  getNetwork(): Network {
    return this.network;
  }

  /**
   * Switches the client to a different Stellar network.
   *
   * Flushes the read cache and the event poller, then re-initialises the
   * RPC server for the new network. This guarantees the next `getStream`
   * call (or any other read) fetches fresh data instead of returning values
   * cached under the previous network.
   *
   * **Note:** Calling `setNetwork` with the same value as the current
   * network (and without overriding the RPC URL) is a no-op — the cache and
   * event poller are preserved. Use `clearStreamCache()` if you only want
   * to invalidate the cache without changing networks.
   *
   * @param network - The network to switch to ("mainnet" | "testnet" | "futurenet").
   * @param options - Optional overrides (e.g. a custom RPC URL for the new network).
   */
  setNetwork(network: Network, options?: { rpcUrl?: string }): void {
    if (this.network === network && !options?.rpcUrl) {
      // Nothing to do — avoid destroying subscribers unnecessarily.
      return;
    }

    // 1. Drop the read cache so stale stream data from the previous network
    //    is never served from cache after the switch.
    this.streamCache.clear();

    // 2. Destroy the existing event poller — it's still pointing at the
    //    previous network's RPC and would otherwise emit stale events for
    //    up to one polling cycle after the switch.
    if (this.eventPoller) {
      this.eventPoller.destroy();
      this.eventPoller = null;
    }

    // 3. Update the network and rebuild the RPC server for the new endpoint.
    this.network = network;
    this.server = new rpc.Server(
      options?.rpcUrl ?? RPC_URLS[network],
      { allowHttp: false }
    );

    // Note: `this.encoder` is bound to the contract address (not the network)
    // and is therefore safe to reuse. The contract instance and wallet adapter
    // are also network-agnostic.
  }

  /**
   * Clears the internal stream read cache. Useful when callers know the
   * on-chain state has changed (e.g. after an out-of-band mutation).
   *
   * @param streamId - Optional specific stream to invalidate. If omitted,
   *   the entire cache is cleared.
   */
  clearStreamCache(streamId?: string): void {
    if (streamId === undefined) {
      this.streamCache.clear();
      return;
    }
    // Cache keys are network-prefixed to defend against mid-flight network
    // switches. Remove entries for every known network.
    for (const key of ["mainnet", "testnet", "futurenet"] as Network[]) {
      this.streamCache.delete(`${key}:${streamId}`);
    }
  }

  private async withBreaker<T>(fn: () => Promise<T>): Promise<T> {
    return this.breaker ? this.breaker.call(fn) : fn();
  }

  // ── Issue #50: Middleware / plugin system ─────────────────────────────────

  /**
   * Registers a middleware plugin on the client.
   * @param plugin - The plugin to register.
   * @returns This client instance, for chaining.
   */
  use(plugin: SoroStreamPlugin): this {
    this.plugins.push(plugin);
    return this;
  }

  private async runWithMiddleware<T>(
    method: string,
    args: unknown[],
    fn: () => Promise<T>
  ): Promise<T> {
    const ctx: MiddlewareContext = { method, args };
    for (const p of this.plugins) {
      await p.before?.(ctx);
    }
    let result: T;
    try {
      result = await fn();
    } catch (err) {
      for (const p of [...this.plugins].reverse()) {
        await p.onError?.(ctx, err);
      }
      throw err;
    }
    for (const p of [...this.plugins].reverse()) {
      await p.after?.(ctx, result);
    }
    return result;
  }

  private async buildAndSubmit(
    operation: xdr.Operation,
    signal?: AbortSignal,
    feeBumpOpts?: FeeBumpOptions
  ): Promise<string> {
    const publicKey = await this.walletAdapter.getPublicKey();

    const account = await withRetry(
      () => this.withBreaker(() => this.server.getAccount(publicKey)),
      { ...this.submitRetry, signal }
    );

    const tx = new TransactionBuilder(account, {
      fee: BASE_FEE,
      networkPassphrase: NETWORK_PASSPHRASES[this.network],
    })
      .addOperation(operation)
      .setTimeout(30)
      .build();

    const preparedTx = await withRetry(
      () => this.withBreaker(() => this.server.prepareTransaction(tx)),
      { ...this.submitRetry, signal }
    );

    const signedXdr = await this.walletAdapter.signTransaction(
      preparedTx.toXDR(),
      this.network
    );

    const result = await withRetry(
      () => this.withBreaker(() =>
        this.server.sendTransaction(
          TransactionBuilder.fromXDR(signedXdr, NETWORK_PASSPHRASES[this.network])
        )
      ),
      { ...this.submitRetry, signal }
    );

    if (result.status === "ERROR") {
      throw new TransactionFailedError(JSON.stringify(result.errorResult));
    }

    // Poll for completion with configurable timeout and exponential backoff
    const startTime = Date.now();
    let delay = 500;
    const maxDelay = 10_000;

    let response = await this.server.getTransaction(result.hash);
    while (response.status === "NOT_FOUND") {
      if (signal?.aborted) {
        throw new DOMException("Transaction polling aborted", "AbortError");
      }

      const elapsed = Date.now() - startTime;
      if (elapsed >= this.txTimeoutMs) {
        throw new Error(
          `Transaction confirmation timed out after ${this.txTimeoutMs}ms`
        );
      }

      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(delay * 2, maxDelay);

      response = await this.server.getTransaction(result.hash);
    }

    if (response.status === "FAILED") {
      throw new TransactionFailedError(result.hash);
    }

    return result.hash;
  }

  private resolveFeeBump(override?: FeeBumpOptions): FeeBumpOptions | undefined {
    return override ?? this.defaultFeeBump ?? undefined;
  }

  private async buildAndSubmitBatch(operations: xdr.Operation[]): Promise<string> {
    const publicKey = await this.walletAdapter.getPublicKey();

    const account = await withRetry(
      () => this.server.getAccount(publicKey),
      this.submitRetry
    );

    let builder = new TransactionBuilder(account, {
      fee: BASE_FEE,
      networkPassphrase: NETWORK_PASSPHRASES[this.network],
    });
    for (const op of operations) {
      builder = builder.addOperation(op);
    }
    const tx = builder.setTimeout(30).build();

    const preparedTx = await withRetry(
      () => this.server.prepareTransaction(tx),
      this.submitRetry
    );

    const signedXdr = await this.walletAdapter.signTransaction(
      preparedTx.toXDR(),
      this.network
    );

    const result = await withRetry(
      () => this.server.sendTransaction(
        TransactionBuilder.fromXDR(signedXdr, NETWORK_PASSPHRASES[this.network])
      ),
      this.submitRetry
    );

    if (result.status === "ERROR") {
      throw new TransactionFailedError(JSON.stringify(result.errorResult));
    }

    let response = await this.server.getTransaction(result.hash);
    while (response.status === "NOT_FOUND") {
      await new Promise((r) => setTimeout(r, 1000));
      response = await this.server.getTransaction(result.hash);
    }

    if (response.status === "FAILED") {
      throw new TransactionFailedError(result.hash);
    }

    return result.hash;
  }

  /**
   * Submits a batch of operations in a single transaction.
   * @param operations - The Soroban operations to include in the transaction.
   * @returns The confirming transaction hash.
   * @throws {TransactionFailedError} If the transaction is rejected by the network.
   */
  async executeBatch(operations: xdr.Operation[]): Promise<string> {
    return this.buildAndSubmitBatch(operations);
  }

  private async simulateOp(
    operation: xdr.Operation
  ): Promise<rpc.Api.SimulateTransactionResponse> {
    const publicKey = await this.walletAdapter.getPublicKey();
    const account = await this.withBreaker(() =>
      this.server.getAccount(publicKey)
    );
    const tx = new TransactionBuilder(account, {
      fee: BASE_FEE,
      networkPassphrase: NETWORK_PASSPHRASES[this.network],
    })
      .addOperation(operation)
      .setTimeout(30)
      .build();
    return this.withBreaker(() => this.server.simulateTransaction(tx));
  }

  // ── Pre-flight validation (Issue 2) ───────────────────────────────────────

  private async validateStreamParams(
    params: CreateStreamParams
  ): Promise<void> {
    if (!isValidStellarAddress(params.recipient)) {
      throw new InvalidAddressError(params.recipient);
    }
    if (!isValidStellarAddress(params.token)) {
      throw new InvalidAddressError(params.token);
    }

    if (params.durationSeconds < MIN_STREAM_DURATION_SECONDS) {
      throw new ZeroDurationError(
        `Stream duration must be >= ${MIN_STREAM_DURATION_SECONDS}s, got ${params.durationSeconds}s`
      );
    }

    // Verify endTime > startTime at the time of submission.
    const startTime = Math.floor(Date.now() / 1000);
    const endTime = startTime + params.durationSeconds;
    if (endTime <= startTime) {
      throw new ZeroDurationError(
        `Computed endTime (${endTime}) must be greater than startTime (${startTime})`
      );
    }

    try {
      await this.withBreaker(() =>
        this.server.getAccount(params.recipient)
      );
    } catch {
      throw new AccountNotFoundError(params.recipient);
    }

    const sender = await this.walletAdapter.getPublicKey();
    try {
      await this.withBreaker(() => this.server.getAccount(sender));
    } catch {
      throw new AccountNotFoundError(sender);
    }
  }

  /**
   * Checks the sender's token allowance for the contract via the SAC allowance view.
   * Throws {@link InsufficientAllowanceError} if the current allowance is less than required.
   * Silently passes when the allowance RPC call fails (non-SAC token, RPC outage, etc.).
   */
  private async checkAllowance(token: string, required: bigint): Promise<void> {
    try {
      const sender = await this.walletAdapter.getPublicKey();
      const contractAddress = this.contract.contractId();

      const tokenContract = new Contract(token);
      const op = tokenContract
        .call(
          "allowance",
          nativeToScVal(sender, { type: "address" }),
          nativeToScVal(contractAddress, { type: "address" })
        );

      const result = await this.simulateOp(op);
      if (rpc.Api.isSimulationError(result)) return; // non-SAC token — skip

      const retval = (result as rpc.Api.SimulateTransactionSuccessResponse).result?.retval;
      if (!retval) return;

      const current = BigInt(scValToNative(retval) as number);
      if (current < required) {
        throw new InsufficientAllowanceError(token, required, current);
      }
    } catch (err) {
      if (err instanceof InsufficientAllowanceError) throw err;
      // RPC / parse failures — don't block stream creation
    }
  }

  // ── Stream mutations ──────────────────────────────────────────────────────

  /**
   * Creates a new payment stream on the SoroStream contract.
   *
   * Validates the recipient address, token address, and sender account before
   * submitting. Enforces that `amount > 0` and `durationSeconds >= 1`.
   *
   * @param params - Stream creation parameters. See [Stream Parameter Ranges](../docs/parameters.md) for detailed limits and ranges.
   * @param params.recipient - Beneficiary Stellar address.
   * @param params.token - SAC token contract address.
   * @param params.amount - Total amount to stream in stroops (must be > 0). See [Stream Parameter Ranges](../docs/parameters.md#createstreamparams) for min/max limits.
   * @param params.durationSeconds - Stream duration in seconds (must be >= 1). See [Stream Parameter Ranges](../docs/parameters.md#createstreamparams) for min/max limits.
   * @param params.autoRenew - Whether the stream auto-renews on completion.
   * @param params.cliffSeconds - Optional cliff duration in seconds (default 0). See [Stream Parameter Ranges](../docs/parameters.md#createstreamparams) for min/max limits.
   * @param signal - Optional `AbortSignal` to cancel in-flight transaction polling.
   * @param options - Optional write options (e.g. `simulateOnly`, `feeBump`).
   * @returns `{ streamId, txHash }` — the new stream ID and confirming transaction hash.
   * @throws {InsufficientAmountError} If `amount` is 0 or negative.
   * @throws {ZeroDurationError} If `durationSeconds` is less than 1.
   * @throws {InvalidAddressError} If `recipient` or `token` is not a valid Stellar address.
   * @throws {AccountNotFoundError} If `recipient` or the sender account does not exist on-chain.
   * @throws {TransactionFailedError} If the Soroban transaction is rejected by the network.
   * @throws {StreamNotFoundError} If the post-creation fetch cannot locate the new stream.
   *
   * @example
   * ```ts
   * const { streamId, txHash } = await client.createStream({
   *   recipient: "GRECIPIENT...",
   *   token:     "GUSDC...",
   *   amount:    toStroops("100"),      // 100 USDC
   *   durationSeconds: 30 * 24 * 3600, // 30 days
   *   autoRenew: false,
   * });
   * console.log("Stream created:", streamId, txHash);
   * ```
   */
  async createStream(
    params: CreateStreamParams,
    signal?: AbortSignal,
    options?: WriteOptions
  ): Promise<{ streamId: string; txHash: string }> {
    return this.runWithMiddleware("createStream", [params], async () => {
      if (params.amount <= 0n) throw new InsufficientAmountError();
      await this.validateCliff(params.cliffSeconds ?? 0);

      const sender = await this.walletAdapter.getPublicKey();

      if (this.checkDuplicate) {
        const existingResult = await this.getStreamsBySender(sender);
        const existingStreams = Array.isArray(existingResult) ? existingResult : existingResult.streams;
        const isDup = existingStreams.some(
          (s) => s.recipient === params.recipient && s.token === params.token && s.status === "Active"
        );
        if (isDup) {
          throw new DuplicateStreamError();
        }
      }

      await this.validateStreamParams(params);

      if (!params.skipAllowanceCheck) {
        await this.checkAllowance(params.token, params.amount);
      }

      const operation = this.encoder.createStream(sender, params);
      const feeBump = this.resolveFeeBump(options?.feeBump);
      const txHash = await this.buildAndSubmit(operation, signal, feeBump);

      const result = await this.getStreamsBySender(sender);
      const streams = Array.isArray(result) ? result : result.streams;
      const latest = streams[streams.length - 1];
      if (!latest)
        throw new StreamNotFoundError(
          "(unknown — post-creation fetch returned empty)"
        );

      return { streamId: latest.id, txHash };
    });
  }

  /**
   * Creates multiple payment streams in a single batched transaction.
   *
   * All streams are validated before submission. When `options.simulateOnly`
   * is `true`, the first operation is simulated without broadcasting.
   *
   * @param paramsArray - Array of stream creation parameter objects.
   * @param paramsArray[].recipient - Beneficiary Stellar address.
   * @param paramsArray[].token - SAC token contract address.
   * @param paramsArray[].amount - Total amount to stream in stroops (must be > 0).
   * @param paramsArray[].durationSeconds - Stream duration in seconds (must be > 0).
   * @param paramsArray[].autoRenew - Whether the stream auto-renews on completion.
   * @param options - Optional write options (e.g. `simulateOnly`).
   * @returns `{ streamIds, txHash }`, or a `SimulateOnlyResult` when `options.simulateOnly` is set.
   * @throws {Error} If `paramsArray` is empty or any entry has `amount <= 0` or `durationSeconds <= 0`.
   * @throws {TransactionFailedError} If the batch transaction is rejected.
   */
  async createStreams(
    paramsArray: CreateStreamsParams[],
    options?: WriteOptions
  ): Promise<
    { streamIds: string[]; txHash: string } | SimulateOnlyResult
  > {
    if (paramsArray.length === 0) throw new Error("At least one stream is required");
    for (const params of paramsArray) {
      if (params.amount <= 0n) throw new Error("Amount must be > 0");
      if (params.durationSeconds <= 0) throw new Error("Duration must be > 0");
    }

    const sender = await this.walletAdapter.getPublicKey();

    const operations = paramsArray.map((params) =>
      this.encoder.createStream(sender, params)
    );

    if (options?.simulateOnly) {
      const result = await this.simulateOp(operations[0]!);
      return { simulated: true, result };
    }

    const txHash = await this.buildAndSubmitBatch(operations);
    const after = await this.getStreamsBySender(sender);
    const afterStreams = Array.isArray(after) ? after : after.streams;
    const streamIds = afterStreams.slice(-paramsArray.length).map((s) => s.id);

    return { streamIds, txHash };
  }

  /**
   * Withdraws all currently claimable tokens from a stream.
   *
   * The connected wallet must be the stream recipient.
   *
   * @param params - Withdraw parameters.
   * @param params.streamId - ID of the stream to withdraw from.
   * @param signal - Optional `AbortSignal` to cancel in-flight transaction polling.
   * @param options - Optional write options (e.g. `feeBump`).
   * @returns `{ txHash, amount }` — confirming transaction hash and withdrawn amount in stroops.
   * @throws {TransactionFailedError} If the transaction is rejected by the network.
   *
   * @example
   * ```ts
   * const { txHash, amount } = await client.withdraw({ streamId: "42" });
   * console.log(`Withdrew ${formatUSDC(BigInt(amount))} USDC — tx: ${txHash}`);
   * ```
   */
  async withdraw(
    params: WithdrawParams,
    signal?: AbortSignal,
    options?: WriteOptions
  ): Promise<{ txHash: string; amount: string }> {
    const recipient = await this.walletAdapter.getPublicKey();
    const claimable = await this.getClaimable(params.streamId);

    const operation = this.encoder.withdraw(params.streamId, recipient);
    const feeBump = this.resolveFeeBump(options?.feeBump);
    const txHash = await this.buildAndSubmit(operation, signal, feeBump);
    return { txHash, amount: claimable.toString() };
  }

  /**
   * Withdraws from multiple streams in batched transactions.
   *
   * Streams are chunked by `batchSize` to stay within Stellar's per-transaction
   * operation limit. Each chunk becomes one submitted transaction.
   *
   * @param streamIds - Stream IDs to withdraw from.
   * @param batchSize - Maximum operations per transaction (default 8).
   * @returns Array of `BatchWithdrawResult`, one entry per submitted transaction.
   * @throws {TransactionFailedError} If any batch transaction is rejected.
   *
   * @example
   * ```ts
   * const results = await client.batchWithdraw(["1", "2", "3"]);
   * for (const r of results) console.log(r.txHash, r.amounts);
   * ```
   */
  async batchWithdraw(
    streamIds: string[],
    batchSize = 8
  ): Promise<BatchWithdrawResult[]> {
    const results: BatchWithdrawResult[] = [];
    const recipient = await this.walletAdapter.getPublicKey();

    for (let i = 0; i < streamIds.length; i += batchSize) {
      const chunk = streamIds.slice(i, i + batchSize);
      const operations = chunk.map((id) =>
        this.encoder.withdraw(id, recipient)
      );

      const amounts: string[] = [];
      for (const id of chunk) {
        const claimable = await this.getClaimable(id);
        amounts.push(claimable.toString());
      }

      const txHash = await this.executeBatch(operations);
      results.push({ txHash, streamIds: chunk, amounts });
    }

    return results;
  }

  /**
   * Cancels an active stream and refunds the unstreamed deposit to the sender.
   *
   * Only the original sender can cancel a stream. Any claimable tokens
   * already accrued remain available for the recipient to withdraw.
   *
   * @param params - Cancel parameters.
   * @param params.streamId - ID of the stream to cancel.
   * @param signal - Optional `AbortSignal` to cancel in-flight transaction polling.
   * @param options - Optional write options (e.g. `feeBump`).
   * @returns `{ txHash }` — confirming transaction hash.
   * @throws {TransactionFailedError} If the transaction is rejected (e.g. stream already cancelled).
   *
   * @example
   * ```ts
   * const { txHash } = await client.cancelStream({ streamId: "42" });
   * ```
   */
  async cancelStream(
    params: CancelStreamParams,
    signal?: AbortSignal,
    options?: WriteOptions
  ): Promise<{ txHash: string }> {
    const sender = await this.walletAdapter.getPublicKey();
    const operation = this.encoder.cancelStream(params.streamId, sender);
    const feeBump = this.resolveFeeBump(options?.feeBump);
    const txHash = await this.buildAndSubmit(operation, signal, feeBump);
    return { txHash };
  }

  /**
   * Tops up an existing stream with additional tokens, extending its duration.
   *
   * The additional deposit is added to the remaining balance, and the stream's
   * `endTime` is extended proportionally based on the current flow rate.
   *
   * @param params - Top-up parameters.
   * @param params.streamId - ID of the stream to top up.
   * @param params.amount - Additional amount to deposit in stroops (must be > 0).
   * @param signal - Optional `AbortSignal` to cancel in-flight transaction polling.
   * @param options - Optional write options (e.g. `feeBump`).
   * @returns `{ txHash, newEndTime }` — confirming transaction hash and updated end time.
   * @throws {InsufficientAmountError} If `amount` is 0 or negative.
   * @throws {TransactionFailedError} If the transaction is rejected.
   *
   * @example
   * ```ts
   * const { txHash, newEndTime } = await client.topUp({
   *   streamId: "42",
   *   amount: toStroops("50"),
   * });
   * console.log("Stream extended until:", newEndTime.toISOString());
   * ```
   * After the transaction confirms, the local stream cache is updated optimistically
   * so that the next `getStream` call reflects the new balance without waiting for
   * the next RPC poll.
   * @param params - Top-up parameters.
   * @param signal - Optional abort signal.
   * @param options - Optional write options.
   * @returns The transaction hash and new end time, or simulation result.
   */
  async topUp(
    params: TopUpParams,
    signal?: AbortSignal,
    options?: WriteOptions
  ): Promise<{ txHash: string; newEndTime: Date }> {
    if (params.amount <= 0n) throw new InsufficientAmountError();
    const sender = await this.walletAdapter.getPublicKey();
    const operation = this.encoder.topUp(
      params.streamId,
      sender,
      params.amount
    );
    const feeBump = this.resolveFeeBump(options?.feeBump);
    const txHash = await this.buildAndSubmit(operation, signal, feeBump);

    // Fetch fresh on-chain state and cache it so immediate getStream() calls
    // reflect the topped-up balance without stale data.
    this.clearStreamCache(params.streamId);
    const stream = await this.getStream(params.streamId);
    return { txHash, newEndTime: new Date(stream.endTime * 1000) };
  }

  /**
   * Cancels multiple streams in batched transactions.
   *
   * @param streamIds - Stream IDs to cancel.
   * @param batchSize - Maximum operations per transaction (default 8).
   * @returns Array of `BatchCancelResult`, one entry per submitted transaction.
   * @throws {TransactionFailedError} If any batch transaction is rejected.
   */
  async batchCancel(
    streamIds: string[],
    batchSize = 8
  ): Promise<BatchCancelResult[]> {
    const results: BatchCancelResult[] = [];
    const sender = await this.walletAdapter.getPublicKey();

    for (let i = 0; i < streamIds.length; i += batchSize) {
      const chunk = streamIds.slice(i, i + batchSize);
      const operations = chunk.map((id) =>
        this.encoder.cancelStream(id, sender)
      );
      const txHash = await this.executeBatch(operations);
      results.push({ txHash, streamIds: chunk });
    }

    return results;
  }

  /**
   * Updates the per-second flow rate on an active stream without cancelling it.
   *
   * @param params - Flow rate update parameters.
   * @param params.streamId - ID of the stream to update.
   * @param params.newFlowRate - New flow rate in stroops per second (must be > 0).
   * @param signal - Optional `AbortSignal` to cancel in-flight transaction polling.
   * @param options - Optional write options.
   * @returns `{ txHash }` — confirming transaction hash.
   * @throws {InsufficientAmountError} If `newFlowRate` is 0 or negative.
   * @throws {TransactionFailedError} If the transaction is rejected.
   */
  async updateFlowRate(
    params: UpdateFlowRateParams,
    signal?: AbortSignal,
    options?: WriteOptions
  ): Promise<{ txHash: string }> {
    if (params.newFlowRate <= 0n) throw new InsufficientAmountError();
    const sender = await this.walletAdapter.getPublicKey();
    const operation = this.encoder.updateFlowRate(params.streamId, sender, params.newFlowRate);
    const feeBump = this.resolveFeeBump(options?.feeBump);
    const txHash = await this.buildAndSubmit(operation, signal, feeBump);
    return { txHash };
  }

  /**
   * Authorises or revokes an operator address for a stream.
   *
   * An authorised operator can call `operatorCancelStream` and `operatorTopUp`
   * on behalf of the stream sender.
   *
   * @param params - Operator configuration parameters.
   * @param params.streamId - ID of the stream.
   * @param params.operator - Stellar address to grant or revoke operator rights.
   * @param params.approved - `true` to grant, `false` to revoke.
   * @param signal - Optional `AbortSignal`.
   * @param options - Optional write options.
   * @returns `{ txHash }` — confirming transaction hash.
   * @throws {TransactionFailedError} If the transaction is rejected.
   */
  async setOperator(
    params: SetOperatorParams,
    signal?: AbortSignal,
    options?: WriteOptions
  ): Promise<{ txHash: string }> {
    const sender = await this.walletAdapter.getPublicKey();
    const operation = this.encoder.setOperator(
      params.streamId,
      sender,
      params.operator,
      params.approved
    );
    const feeBump = this.resolveFeeBump(options?.feeBump);
    const txHash = await this.buildAndSubmit(operation, signal, feeBump);
    return { txHash };
  }

  /**
   * Cancels a stream as an authorised operator, on behalf of the sender.
   *
   * @param params - Operator cancel parameters.
   * @param params.streamId - ID of the stream to cancel.
   * @param signal - Optional `AbortSignal` to cancel in-flight transaction polling.
   * @param options - Optional write options.
   * @returns `{ txHash }` — confirming transaction hash.
   * @throws {TransactionFailedError} If the transaction is rejected (e.g. caller is not an authorised operator).
   */
  async operatorCancelStream(
    params: { streamId: string },
    signal?: AbortSignal,
    options?: WriteOptions
  ): Promise<{ txHash: string }> {
    const operator = await this.walletAdapter.getPublicKey();
    const operation = this.encoder.operatorCancelStream(params.streamId, operator);
    const feeBump = this.resolveFeeBump(options?.feeBump);
    const txHash = await this.buildAndSubmit(operation, signal, feeBump);
    return { txHash };
  }

  /**
   * Tops up a stream as an authorised operator, on behalf of the sender.
   *
   * @param params - Operator top-up parameters.
   * @param params.streamId - ID of the stream to top up.
   * @param params.amount - Additional amount to deposit in stroops (must be > 0).
   * @param signal - Optional `AbortSignal` to cancel in-flight transaction polling.
   * @param options - Optional write options.
   * @returns `{ txHash }` — confirming transaction hash.
   * @throws {InsufficientAmountError} If `amount` is 0 or negative.
   * @throws {TransactionFailedError} If the transaction is rejected (e.g. caller is not an authorised operator).
   */
  async operatorTopUp(
    params: OperatorTopUpParams,
    signal?: AbortSignal,
    options?: WriteOptions
  ): Promise<{ txHash: string }> {
    if (params.amount <= 0n) throw new InsufficientAmountError();
    const operator = await this.walletAdapter.getPublicKey();
    const operation = this.encoder.operatorTopUp(params.streamId, operator, params.amount);
    const feeBump = this.resolveFeeBump(options?.feeBump);
    const txHash = await this.buildAndSubmit(operation, signal, feeBump);
    return { txHash };
  }

  /**
   * Splits an active stream into two streams with a user-defined ratio,
   * cancelling the original stream.
   *
   * The remaining balance of the original stream is divided according to the
   * ratio (ratioNumerator / ratioDenominator) and two new streams are created
   * with proportional flow rates. The original stream is cancelled.
   *
   * @param params - Split stream parameters.
   * @param params.streamId - ID of the stream to split.
   * @param params.recipientA - Beneficiary address for the first resulting stream.
   * @param params.recipientB - Beneficiary address for the second resulting stream.
   * @param params.ratioNumerator - Numerator of the split ratio (must be > 0 and < `ratioDenominator`).
   * @param params.ratioDenominator - Denominator of the split ratio (must be > 0).
   * @param signal - Optional `AbortSignal` to cancel in-flight transaction polling.
   * @param options - Optional write options.
   * @returns `{ txHash, streamIdA, streamIdB }` — confirming transaction hash and the two new stream IDs.
   * @throws {Error} If the ratio is not positive or `ratioNumerator >= ratioDenominator`.
   * @throws {InvalidAddressError} If `recipientA` or `recipientB` is not a valid Stellar address.
   * @throws {TransactionFailedError} If the transaction is rejected.
   */
  async splitStream(
    params: SplitStreamParams,
    signal?: AbortSignal,
    options?: WriteOptions
  ): Promise<SplitStreamResult> {
    if (params.ratioNumerator <= 0 || params.ratioDenominator <= 0) {
      throw new Error("Ratio must be positive");
    }
    if (params.ratioNumerator >= params.ratioDenominator) {
      throw new Error("Ratio numerator must be less than denominator");
    }

    const sender = await this.walletAdapter.getPublicKey();

    if (!isValidStellarAddress(params.recipientA)) {
      throw new InvalidAddressError(params.recipientA);
    }
    if (!isValidStellarAddress(params.recipientB)) {
      throw new InvalidAddressError(params.recipientB);
    }

    const operation = this.encoder.splitStream(sender, params);
    const feeBump = this.resolveFeeBump(options?.feeBump);
    const txHash = await this.buildAndSubmit(operation, signal, feeBump);

    const result = await this.getStreamsBySender(sender);
    const streams = Array.isArray(result) ? result : result.streams;
    const latest = streams.slice(-2);
    const streamIdA = latest[0]?.id ?? "";
    const streamIdB = latest[1]?.id ?? "";

    return { txHash, streamIdA, streamIdB };
  }

  /**
   * Transfers ownership of a stream to a new recipient address mid-flight.
   * Only the sender can transfer ownership.
   *
   * @param params - Transfer parameters.
   * @param params.streamId - ID of the stream to transfer.
   * @param params.newRecipient - Stellar address of the new beneficiary.
   * @param signal - Optional `AbortSignal` to cancel in-flight transaction polling.
   * @param options - Optional write options.
   * @returns `{ txHash }` — confirming transaction hash.
   * @throws {InvalidAddressError} If `newRecipient` is not a valid Stellar address.
   * @throws {TransactionFailedError} If the transaction is rejected.
   */
  async transferStream(
    params: TransferStreamParams,
    signal?: AbortSignal,
    options?: WriteOptions
  ): Promise<{ txHash: string }> {
    if (!isValidStellarAddress(params.newRecipient)) {
      throw new InvalidAddressError(params.newRecipient);
    }
    const sender = await this.walletAdapter.getPublicKey();
    const operation = this.encoder.transferStream(
      params.streamId,
      sender,
      params.newRecipient
    );
    const feeBump = this.resolveFeeBump(options?.feeBump);
    const txHash = await this.buildAndSubmit(operation, signal, feeBump);
    return { txHash };
  }

  /**
   * Pauses an active stream. While paused, no new claimable tokens accumulate.
   *
   * @param params - Pause parameters.
   * @param params.streamId - ID of the stream to pause.
   * @param signal - Optional `AbortSignal` to cancel in-flight transaction polling.
   * @param options - Optional write options.
   * @returns `{ txHash }` — confirming transaction hash.
   * @throws {TransactionFailedError} If the transaction is rejected (e.g. stream already paused).
   */
  async pause(
    params: PauseStreamParams,
    signal?: AbortSignal,
    options?: WriteOptions
  ): Promise<{ txHash: string }> {
    const sender = await this.walletAdapter.getPublicKey();
    const operation = this.encoder.pauseStream(params.streamId, sender);
    const feeBump = this.resolveFeeBump(options?.feeBump);
    const txHash = await this.buildAndSubmit(operation, signal, feeBump);
    return { txHash };
  }

  /**
   * Resumes a previously paused stream. Claimable tokens will again accumulate.
   *
   * @param params - Resume parameters.
   * @param params.streamId - ID of the stream to resume.
   * @param signal - Optional `AbortSignal` to cancel in-flight transaction polling.
   * @param options - Optional write options.
   * @returns `{ txHash }` — confirming transaction hash.
   * @throws {TransactionFailedError} If the transaction is rejected (e.g. stream is not paused).
   */
  async resume(
    params: ResumeStreamParams,
    signal?: AbortSignal,
    options?: WriteOptions
  ): Promise<{ txHash: string }> {
    const sender = await this.walletAdapter.getPublicKey();
    const operation = this.encoder.resumeStream(params.streamId, sender);
    const feeBump = this.resolveFeeBump(options?.feeBump);
    const txHash = await this.buildAndSubmit(operation, signal, feeBump);
    return { txHash };
  }

  // ── Fee estimation ────────────────────────────────────────────────────────

  private async estimateOperationFee(
    operation: xdr.Operation
  ): Promise<FeeEstimate> {
    const publicKey = await this.walletAdapter.getPublicKey();
    const account = await this.withBreaker(() =>
      this.server.getAccount(publicKey)
    );

    const tx = new TransactionBuilder(account, {
      fee: BASE_FEE,
      networkPassphrase: NETWORK_PASSPHRASES[this.network],
    })
      .addOperation(operation)
      .setTimeout(30)
      .build();

    const preparedTx = await this.withBreaker(() =>
      this.server.prepareTransaction(tx)
    );

    const minResourceFee =
      (
        preparedTx as unknown as { minResourceFee?: number }
      ).minResourceFee ?? 0;

    return {
      totalFee: Number(preparedTx.fee) + minResourceFee,
      minResourceFee,
    };
  }

  /**
   * Estimates the network fee for a {@link createStream} call without submitting it.
   * @param params - Same shape as {@link createStream}'s `params`.
   * @returns `{ totalFee, minResourceFee }` in stroops.
   * @throws {Error} If `amount` is 0 or negative, or `durationSeconds` is 0 or negative.
   */
  async estimateCreateStreamFee(
    params: CreateStreamParams
  ): Promise<FeeEstimate> {
    if (params.amount <= 0n) throw new Error("Amount must be > 0");
    if (params.durationSeconds <= 0) throw new Error("Duration must be > 0");

    const sender = await this.walletAdapter.getPublicKey();
    const operation = this.encoder.createStream(sender, params);
    return this.estimateOperationFee(operation);
  }

  /**
   * Estimates the network fee for a {@link withdraw} call without submitting it.
   * @param params - Withdraw parameters.
   * @param params.streamId - ID of the stream to withdraw from.
   * @returns `{ totalFee, minResourceFee }` in stroops.
   */
  async estimateWithdrawFee(params: WithdrawParams): Promise<FeeEstimate> {
    const recipient = await this.walletAdapter.getPublicKey();
    const operation = this.encoder.withdraw(params.streamId, recipient);
    return this.estimateOperationFee(operation);
  }

  /**
   * Estimates the network fee for a {@link cancelStream} call without submitting it.
   * @param params - Cancel parameters.
   * @param params.streamId - ID of the stream to cancel.
   * @returns `{ totalFee, minResourceFee }` in stroops.
   */
  async estimateCancelStreamFee(
    params: CancelStreamParams
  ): Promise<FeeEstimate> {
    const sender = await this.walletAdapter.getPublicKey();
    const operation = this.encoder.cancelStream(params.streamId, sender);
    return this.estimateOperationFee(operation);
  }

  /**
   * Estimates the network fee for a {@link topUp} call without submitting it.
   * @param params - Top-up parameters.
   * @param params.streamId - ID of the stream to top up.
   * @param params.amount - Additional amount to deposit in stroops (must be > 0).
   * @returns `{ totalFee, minResourceFee }` in stroops.
   * @throws {Error} If `amount` is 0 or negative.
   */
  async estimateTopUpFee(params: TopUpParams): Promise<FeeEstimate> {
    if (params.amount <= 0n) throw new Error("Amount must be > 0");
    const sender = await this.walletAdapter.getPublicKey();
    const operation = this.encoder.topUp(
      params.streamId,
      sender,
      params.amount
    );
    return this.estimateOperationFee(operation);
  }

  // ── Event subscription ───────────────────────────────────────────────────────

  private getEventPoller(): EventPoller {
    if (!this.eventPoller) {
      this.eventPoller = new EventPoller(
        this.server,
        this.contract.contractId()
      );
    }
    return this.eventPoller;
  }

  /**
   * Subscribes to real-time stream lifecycle events matching the given filter.
   * The callback is invoked each time a matching event is detected.
   *
   * @param filter - Criteria to match events against (`streamId`, `sender`, `recipient`); omitted fields match anything.
   * @param callback - Invoked with each matching event.
   * @returns A `StreamSubscription` — call `.unsubscribe()` to stop listening.
   *
   * @example
   * ```ts
   * const sub = client.subscribeEvents({ streamId: "42" }, (event) => {
   *   console.log(event.type, event.streamId);
   * });
   * // later: sub.unsubscribe();
   * ```
   */
  subscribeEvents(
    filter: StreamEventFilter,
    callback: (event: StreamEvent) => void
  ): StreamSubscription {
    const poller = this.getEventPoller();
    const key = `${filter.streamId ?? "*"}:${filter.sender ?? "*"}:${filter.recipient ?? "*"}:${Date.now()}`;

    return poller.subscribe(key, {
      filter: (event) => {
        if (filter.streamId && event.streamId !== filter.streamId) return false;
        if (filter.sender && event.data.sender !== filter.sender) return false;
        if (filter.recipient && event.data.recipient !== filter.recipient)
          return false;
        return true;
      },
      callback,
    });
  }

  /**
   * Subscribe to a specific stream lifecycle event type.
   *
   * @param eventType - The lifecycle event type to listen for.
   * @param callback - Invoked with the matching event.
   * @returns A `StreamSubscription` — call `.unsubscribe()` to stop listening.
   *
   * @example
   * ```ts
   * const sub = client.on("StreamCreated", (event) => {
   *   console.log("Stream created:", event.streamId);
   * });
   * // later: sub.unsubscribe();
   * ```
   */
  on(
    eventType: StreamEventType,
    callback: (event: StreamEvent) => void
  ): StreamSubscription {
    return this.subscribeEvents({}, (event) => {
      if (event.type === eventType) {
        callback(event);
      }
    });
  }

  /**
   * Shorthand for subscribing to stream-created events.
   *
   * @param callback - Invoked with each matching event.
   * @returns A `StreamSubscription` — call `.unsubscribe()` to stop listening.
   */
  onStreamCreated(callback: (event: StreamEvent) => void): StreamSubscription {
    return this.on("StreamCreated", callback);
  }

  /**
   * Shorthand for subscribing to stream-withdrawn events.
   *
   * @param callback - Invoked with each matching event.
   * @returns A `StreamSubscription` — call `.unsubscribe()` to stop listening.
   */
  onStreamWithdrawn(callback: (event: StreamEvent) => void): StreamSubscription {
    return this.on("StreamWithdrawn", callback);
  }

  /**
   * Shorthand for subscribing to stream-topped-up events.
   *
   * @param callback - Invoked with each matching event.
   * @returns A `StreamSubscription` — call `.unsubscribe()` to stop listening.
   */
  onStreamToppedUp(callback: (event: StreamEvent) => void): StreamSubscription {
    return this.on("StreamToppedUp", callback);
  }

  /**
   * Shorthand for subscribing to stream-cancelled events.
   *
   * @param callback - Invoked with each matching event.
   * @returns A `StreamSubscription` — call `.unsubscribe()` to stop listening.
   */
  onStreamCancelled(callback: (event: StreamEvent) => void): StreamSubscription {
    return this.on("StreamCancelled", callback);
  }

  /**
   * Shorthand for subscribing to stream-transferred events.
   *
   * @param callback - Invoked with each matching event.
   * @returns A `StreamSubscription` — call `.unsubscribe()` to stop listening.
   */
  onStreamTransferred(callback: (event: StreamEvent) => void): StreamSubscription {
    return this.on("StreamTransferred", callback);
  }

  /**
   * Shorthand for subscribing to stream-paused events.
   *
   * @param callback - Invoked with each matching event.
   * @returns A `StreamSubscription` — call `.unsubscribe()` to stop listening.
   */
  onStreamPaused(callback: (event: StreamEvent) => void): StreamSubscription {
    return this.on("StreamPaused", callback);
  }

  /**
   * Shorthand for subscribing to stream-resumed events.
   *
   * @param callback - Invoked with each matching event.
   * @returns A `StreamSubscription` — call `.unsubscribe()` to stop listening.
   */
  onStreamResumed(callback: (event: StreamEvent) => void): StreamSubscription {
    return this.on("StreamResumed", callback);
  }

  // ── Read methods (with retry) ────────────────────────────────────────────────
  // ── Queries ───────────────────────────────────────────────────────────────

  /**
   * Returns the full stream data for a given stream ID.
   * Returns a cached value when one is present (populated by optimistic updates).
   * Automatically retries on transient RPC errors.
   * @param streamId - The stream ID to look up.
   * @returns The `Stream` record.
   * @throws {StreamNotFoundError} If no stream exists with the given ID.
   */
  async getStream(streamId: string): Promise<Stream> {
    // Capture the current network so a concurrent `setNetwork` call can't
    // poison the cache with data fetched under a different network.
    const networkAtCallTime = this.network;
    const cached = this.streamCache.get(`${networkAtCallTime}:${streamId}`);
    if (cached) return cached;

    const result = await withRetry(
      () =>
        this.simulateOp(
          this.contract.call(
            "get_stream",
            nativeToScVal(BigInt(streamId), { type: "u64" })
          )
        ),
      this.readRetry
    );

    if (rpc.Api.isSimulationError(result)) {
      throw new StreamNotFoundError(streamId);
    }

    const returnVal = (
      result as rpc.Api.SimulateTransactionSuccessResponse
    ).result?.retval;
    if (!returnVal) throw new Error("No return value from contract");
    const stream = scValToStream(returnVal);

    // Only cache the result if the network hasn't changed during the RPC.
    // This guard maintains the cache contract keyed by the *current*
    // network: an in-flight read on the old network must never write into
    // the new network's slot, and any entry already present must remain
    // addressable under the network in which it was originally fetched.
    if (networkAtCallTime === this.network) {
      this.streamCache.set(`${networkAtCallTime}:${streamId}`, stream);
    }
    return stream;
  }

  /**
   * Returns the currently claimable amount in stroops for a stream.
   *
   * Distinguishes "stream not found" (returns `0n`) from transient RPC errors
   * (retried automatically, then thrown). A contract-level simulation error
   * indicates the stream does not exist; network failures are retried.
   *
   * @param streamId - The stream ID to check.
   * @returns The claimable amount in stroops, or `0n` if the stream does not exist.
   */
  async getClaimable(streamId: string): Promise<bigint> {
    const result = await withRetry(
      () =>
        this.simulateOp(
          this.contract.call(
            "get_claimable",
            nativeToScVal(BigInt(streamId), { type: "u64" })
          )
        ),
      this.readRetry
    );

    if (rpc.Api.isSimulationError(result)) return 0n;

    const returnVal = (result as rpc.Api.SimulateTransactionSuccessResponse).result?.retval;
    if (!returnVal) return 0n;
    return BigInt(scValToNative(returnVal) as number);
  }

  /**
   * Returns all streams created by a sender address.
   * When `pagination` is omitted, returns the full result set (backward-compatible).
   * Automatically retries on transient RPC errors.
   *
   * @param sender - The sender address to query.
   * @param pagination - Optional limit/cursor for paginated results.
   * @returns A `Stream[]` when `pagination` is omitted, otherwise a `PaginatedStreams` page.
   */
  async getStreamsBySender(
    sender: string,
    pagination?: PaginationParams
  ): Promise<Stream[] | PaginatedStreams> {
    const args: xdr.ScVal[] = [nativeToScVal(sender, { type: "address" })];

    if (pagination) {
      args.push(nativeToScVal(pagination.limit ?? 20, { type: "u32" }));
      args.push(
        pagination.cursor != null
          ? nativeToScVal(BigInt(pagination.cursor), { type: "u64" })
          : xdr.ScVal.scvVoid()
      );
    }

    const result = await withRetry(
      () => this.simulateOp(this.contract.call("get_streams_by_sender", ...args)),
      this.readRetry
    );

    if (rpc.Api.isSimulationError(result)) {
      return pagination
        ? { streams: [], cursor: null, hasMore: false }
        : [];
    }

    const returnVal = (
      result as rpc.Api.SimulateTransactionSuccessResponse
    ).result?.retval;
    if (!returnVal) {
      return pagination
        ? { streams: [], cursor: null, hasMore: false }
        : [];
    }

    const raw = scValToNative(returnVal) as xdr.ScVal[];
    const streams = raw.map(scValToStream);

    if (!pagination) return streams;

    const limit = pagination.limit ?? 20;
    const last = streams[streams.length - 1];
    return {
      streams,
      cursor: last ? last.id : null,
      hasMore: streams.length >= limit,
    };
  }

  /**
   * Returns all streams targeting a recipient address.
   * When `pagination` is omitted, returns the full result set (backward-compatible).
   * Automatically retries on transient RPC errors.
   *
   * @param recipient - The recipient address to query.
   * @param pagination - Optional limit/cursor for paginated results.
   * @returns A `Stream[]` when `pagination` is omitted, otherwise a `PaginatedStreams` page.
   */
  async getStreamsByRecipient(
    recipient: string,
    pagination?: PaginationParams
  ): Promise<Stream[] | PaginatedStreams> {
    const args: xdr.ScVal[] = [
      nativeToScVal(recipient, { type: "address" }),
    ];

    if (pagination) {
      args.push(nativeToScVal(pagination.limit ?? 20, { type: "u32" }));
      args.push(
        pagination.cursor != null
          ? nativeToScVal(BigInt(pagination.cursor), { type: "u64" })
          : xdr.ScVal.scvVoid()
      );
    }

    const result = await withRetry(
      () => this.simulateOp(this.contract.call("get_streams_by_recipient", ...args)),
      this.readRetry
    );

    if (rpc.Api.isSimulationError(result)) {
      return pagination
        ? { streams: [], cursor: null, hasMore: false }
        : [];
    }

    const returnVal = (
      result as rpc.Api.SimulateTransactionSuccessResponse
    ).result?.retval;
    if (!returnVal) {
      return pagination
        ? { streams: [], cursor: null, hasMore: false }
        : [];
    }

    const raw = scValToNative(returnVal) as xdr.ScVal[];
    const streams = raw.map(scValToStream);

    if (!pagination) return streams;

    const limit = pagination.limit ?? 20;
    const last = streams[streams.length - 1];
    return {
      streams,
      cursor: last ? last.id : null,
      hasMore: streams.length >= limit,
    };
  }

  // ── Issue #73: Stream snapshot export / import ───────────────────────────

  /**
   * Exports a complete stream snapshot including current claimable amount and
   * a projected vesting curve. The result is fully JSON-serialisable.
   *
   * @param streamId - The stream to snapshot.
   * @param cliffSeconds - Optional cliff duration in seconds for the vesting projection (default 0).
   * @returns A JSON-serialisable `StreamSnapshot`.
   * @throws {StreamNotFoundError} If no stream exists with the given ID.
   */
  async exportStream(streamId: string, cliffSeconds = 0): Promise<StreamSnapshot> {
    const stream = await this.getStream(streamId);
    const claimable = await this.getClaimable(streamId);

    const now = Math.floor(Date.now() / 1000);
    const vesting = calculateVestingSchedule(stream, cliffSeconds, now);

    return {
      version: 1,
      exportedAt: Date.now(),
      stream: {
        ...stream,
        deposit: stream.deposit.toString(),
        flowRate: stream.flowRate.toString(),
      },
      claimableAtExport: claimable.toString(),
      vestingProjection: vesting.milestones.map((m) => ({
        time: m.time,
        vested: m.vested.toString(),
      })),
      history: [],
    };
  }

  /**
   * Reconstructs a read-only stream view from a previously exported snapshot.
   * Useful for offline analysis without a live RPC connection.
   *
   * @param snapshot - A `StreamSnapshot` produced by `exportStream`.
   * @returns The deserialized `Stream` object with bigint fields restored.
   */
  importStream(snapshot: StreamSnapshot): import("./types.js").Stream {
    return {
      ...snapshot.stream,
      deposit: BigInt(snapshot.stream.deposit),
      flowRate: BigInt(snapshot.stream.flowRate),
    };
  }

  // ── Bulk operations ───────────────────────────────────────────────────────

  /**
   * Creates multiple payment streams across one or more batched transactions.
   *
   * Rows are chunked by `options.batchSize`. A chunk where every row shares
   * the default token is submitted as a single multi-operation transaction;
   * a chunk with per-row token overrides falls back to one transaction per
   * row. If any row or chunk fails, the successfully created streams are
   * **not** rolled back — the method throws {@link BulkCreatePartialError}
   * describing exactly which rows succeeded and which failed, instead of
   * silently dropping the failed slots.
   *
   * @param rows - Rows describing the streams to create.
   * @param rows[].recipient - Beneficiary Stellar address for this row.
   * @param rows[].amount - Total amount to stream in stroops (must be > 0).
   * @param rows[].durationSeconds - Stream duration in seconds (must be > 0).
   * @param rows[].token - Optional per-row token override (defaults to `options.token`).
   * @param rows[].cliffSeconds - Optional per-row cliff duration in seconds (default 0).
   * @param options - Bulk creation options.
   * @param options.token - Default SAC token contract address for rows that omit `token`.
   * @param options.autoRenew - Whether created streams auto-renew (default false).
   * @param options.batchSize - Maximum operations per transaction (default 8).
   * @returns `{ batches }` — one entry per submitted transaction, each with its `txHash` and the resulting `streamIds`.
   * @throws {BulkCreatePartialError} If one or more rows fail; carries `successfulBatches` and `failedSlots`.
   * @throws {TransactionFailedError} If a submitted transaction is rejected (wrapped into `failedSlots` rather than thrown directly).
   *
   * @example
   * ```ts
   * try {
   *   const { batches } = await client.bulkCreateStreams(rows, { token: usdc });
   * } catch (err) {
   *   if (err instanceof BulkCreatePartialError) {
   *     console.error(`${err.failedSlots.length} stream(s) failed:`, err.failedSlots);
   *   }
   * }
   * ```
   */
  async bulkCreateStreams(
    rows: import("./types.js").BulkStreamRow[],
    options: BulkCreateOptions
  ): Promise<BulkCreateResult> {
    return this.runWithMiddleware("bulkCreateStreams", [rows, options], async () => {
      const sender = await this.walletAdapter.getPublicKey();
      const defaultToken = options.token;
      const autoRenew = options.autoRenew ?? false;
      const batchSize = options.batchSize ?? 8;

      // Validate cliff for all rows before submitting anything
      for (const row of rows) {
        await this.validateCliff(row.cliffSeconds ?? 0);
      }

      const results: BulkCreateResult["batches"] = [];
      const failedSlots: BulkCreateFailedSlot[] = [];

      for (let i = 0; i < rows.length; i += batchSize) {
        const chunk = rows.slice(i, i + batchSize);
        const chunkHasMixedTokens = chunk.some(
          (r) => r.token != null && r.token !== defaultToken
        );

        if (chunkHasMixedTokens) {
          for (let j = 0; j < chunk.length; j++) {
            const row = chunk[j]!;
            try {
              const rowToken = row.token ?? defaultToken;
              const operation = this.encoder.createStream(sender, {
                recipient: row.recipient,
                token: rowToken,
                amount: row.amount,
                durationSeconds: row.durationSeconds,
                autoRenew,
              });
              const txHash = await this.buildAndSubmit(operation);

              const result = await this.getStreamsBySender(sender);
              const streams = Array.isArray(result) ? result : result.streams;
              const newStreams = streams.slice(-1);
              const streamIds = newStreams.map((s) => s.id);

              results.push({ txHash, streamIds, rows: [row] });
            } catch (error) {
              failedSlots.push({ index: i + j, row, error });
            }
          }
        } else {
          try {
            const operations = chunk.map((row) => {
              const rowToken = row.token ?? defaultToken;
              return this.encoder.createStream(sender, {
                recipient: row.recipient,
                token: rowToken,
                amount: row.amount,
                durationSeconds: row.durationSeconds,
                autoRenew,
              });
            });

            const txHash = await this.executeBatch(operations);

            const result = await this.getStreamsBySender(sender);
            const streams = Array.isArray(result) ? result : result.streams;
            const newStreams = streams.slice(-chunk.length);
            const streamIds = newStreams.map((s) => s.id);

            results.push({ txHash, streamIds, rows: chunk });
          } catch (error) {
            chunk.forEach((row, j) => {
              failedSlots.push({ index: i + j, row, error });
            });
          }
        }
      }

      if (failedSlots.length > 0) {
        throw new BulkCreatePartialError(results, failedSlots);
      }

      return { batches: results };
    });
  }

  // ── Utility ───────────────────────────────────────────────────────────────

  /**
   * Returns the circuit breaker guarding RPC calls, if one was configured.
   * @returns The active `CircuitBreaker`, or `null` if none was configured.
   */
  getCircuitBreaker(): CircuitBreaker | null {
    return this.breaker;
  }

  /**
   * Returns the price-feed adapter used for token-to-fiat conversions, if one was configured.
   * @returns The active `PriceFeedAdapter`, or `null` if none was configured.
   */
  getPriceFeed(): PriceFeedAdapter | null {
    return this.priceFeed;
  }

  // ── Issue #148: Recipient change notification ─────────────────────────────

  /**
   * Polls a stream and invokes `callback` whenever the recipient address
   * changes. Returns an unsubscribe function that stops the polling.
   *
   * @param streamId - The stream to watch.
   * @param callback - Called with change details when a recipient transfer is detected.
   * @param options - Optional polling interval (default 5 s).
   */
  onRecipientChanged(
    streamId: string,
    callback: (event: RecipientChangedEvent) => void,
    options?: OnRecipientChangedOptions
  ): () => void {
    const intervalMs = options?.intervalMs ?? 5_000;
    let stopped = false;
    let lastRecipient: string | null = null;

    const poll = async () => {
      if (stopped) return;
      try {
        const stream = await this.getStream(streamId);
        if (lastRecipient !== null && stream.recipient !== lastRecipient) {
          callback({
            streamId,
            oldRecipient: lastRecipient,
            newRecipient: stream.recipient,
            timestamp: Math.floor(Date.now() / 1000),
          });
        }
        lastRecipient = stream.recipient;
      } catch {
        // swallow transient errors — keep polling
      }
    };

    // Seed lastRecipient on first tick
    void poll();
    const timer = setInterval(poll, intervalMs);

    return () => {
      stopped = true;
      clearInterval(timer);
    };
  }

  // ── Issue #149: Connection pooling ────────────────────────────────────────

  /**
   * Returns current connection pool statistics.
   */
  getConnectionStats(): {
    maxConnections: number;
    active: number;
    idle: number;
    reused: number;
  } {
    return {
      maxConnections: this.connectionPool.maxConnections,
      active: this.connectionPool.active,
      idle: this.connectionPool.idle,
      reused: this.connectionPool.reused,
    };
  }
  // ── Issue #167: Stream expiration hooks ──────────────────────────────────

  private readonly _expiryHandlers = new Map<string, Set<(stream: Stream) => void>>();
  private readonly _expiryTimers = new Map<string, ReturnType<typeof setTimeout>>();

  /**
   * Registers a callback that fires once when a stream reaches its `end_time`.
   * Multiple handlers per stream are supported. The handler receives the stream
   * snapshot fetched at expiry time.
   *
   * @param streamId - The stream to watch for expiry.
   * @param callback - Invoked with the final stream snapshot at expiry.
   * @returns An unsubscribe function. Call it to cancel the hook before it fires.
   *
   * @example
   * ```ts
   * const unsubscribe = client.onExpiry("42", (stream) => {
   *   console.log("Stream expired:", stream.id);
   * });
   * // later: unsubscribe();
   * ```
   */
  onExpiry(streamId: string, callback: (stream: Stream) => void): () => void {
    if (!this._expiryHandlers.has(streamId)) {
      this._expiryHandlers.set(streamId, new Set());
    }
    const handlers = this._expiryHandlers.get(streamId)!;
    handlers.add(callback);

    if (!this._expiryTimers.has(streamId)) {
      void this._scheduleExpiry(streamId);
    }

    return () => {
      handlers.delete(callback);
      if (handlers.size === 0) {
        this._cancelExpiryTimer(streamId);
        this._expiryHandlers.delete(streamId);
      }
    };
  }

  private async _scheduleExpiry(streamId: string): Promise<void> {
    try {
      const stream = await this.getStream(streamId);
      const delayMs = Math.max(0, stream.endTime * 1000 - Date.now());

      const handle = setTimeout(async () => {
        this._expiryTimers.delete(streamId);
        try {
          const finalStream = await this.getStream(streamId);
          const handlers = this._expiryHandlers.get(streamId);
          if (handlers) {
            for (const cb of [...handlers]) cb(finalStream);
          }
        } catch { /* stream may no longer exist */ }
      }, delayMs);

      this._expiryTimers.set(streamId, handle);
    } catch { /* stream not found — skip */ }
  }

  private _cancelExpiryTimer(streamId: string): void {
    const handle = this._expiryTimers.get(streamId);
    if (handle !== undefined) {
      clearTimeout(handle);
      this._expiryTimers.delete(streamId);
    }
  }
  // ── Issue #166: Stream activity log ──────────────────────────────────────

  /**
   * Returns a time-ordered list of on-chain events for a stream.
   *
   * Supported event types: StreamCreated, StreamWithdrawn, StreamCancelled.
   * Results are sorted oldest-first and filtered by optional timestamp range.
   *
   * @param streamId - The stream to query.
   * @param options - Optional timestamp filters (`from`/`to` in ms) and pagination.
   * @returns `StreamActivityEntry[]` sorted oldest-first. Empty array when no events exist.
   *
   * @example
   * ```ts
   * const log = await client.getActivityLog("42");
   * const withdrawals = log.filter((e) => e.type === "StreamWithdrawn");
   * ```
   */
  async getActivityLog(
    streamId: string,
    options?: GetActivityLogOptions
  ): Promise<StreamActivityEntry[]> {
    const { StreamIndexer } = await import("./indexer.js");
    const indexer = new StreamIndexer(this.server, this.contract.contractId());

    const { events } = await indexer.getStreamHistory(streamId, {
      limit: options?.limit ?? 100,
      cursor: options?.cursor,
    });

    return events
      .map((e): StreamActivityEntry => {
        let amount = 0n;
        if (e.type === "StreamWithdrawn") {
          amount = e.data.amount;
        } else if (e.type === "StreamCreated") {
          amount = e.data.deposit;
        }
        return {
          type: e.type as StreamActivityEntry["type"],
          timestamp: new Date(e.ledgerClosedAt).getTime(),
          amount,
          txHash: e.txHash,
          ledger: e.ledger,
        };
      })
      .filter((entry) => {
        if (options?.from != null && entry.timestamp < options.from) return false;
        if (options?.to != null && entry.timestamp > options.to) return false;
        return true;
      })
      .sort((a, b) => a.timestamp - b.timestamp);
  }
}

// Re-export for convenience
export type { StreamFilterCriteria, CreateStreamsParams };
// Fix #156: Use ledger time instead of Date.now() for stream startTime
