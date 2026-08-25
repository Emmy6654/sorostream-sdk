export { SoroStreamClient, createClient } from './SoroStreamClient.js';
export type {
  SoroStreamClientOptions,
  SimulateOnlyResult,
  SoroStreamConfigUpdate,
  ConfigUpdatedEvent,
} from './SoroStreamClient.js';
export type {
  StorageAdapter,
  WebSocketFactory,
  FetchAdapter,
  SoroStreamAdapters,
} from './adapters.js';

export { createDefaultRpcTransport } from './transport.js';
export type {
  RpcTransportAdapter,
  RpcTransportInitContext,
  RpcTransportGetEventsRequest,
} from './transport.js';

export { MockSoroStreamClient, SoroStreamSandbox } from './mock.js';

export { WebhookForwarder } from './webhook.js';
export {
  toStroops,
  formatUSDC,
  formatToken,
  toFiatDisplay,
  isValidStellarAddress,
  isFederationAddress,
  resolveFederationAddress,
  calculateFlowRate,
  timeUntilStreamEnd,
  claimableNow,
  calculateVestingSchedule,
  watchClaimable,
  watchClaimableWs,
  watchTotalClaimable,
  aggregateStreamsByToken,
  totalValueStreamed,
  aggregateStreamsByStatus,
  averageStreamDuration,
  streamHealthSummary,
  aggregateStreamsByRecipient,
  parseCsvStreamRows,
  detectStreamDrift,
  watchStreamDrift,
  isStreamExpiring,
  isStreamStalled,
  isStreamUnderfunded,
  isExpired,
  streamToJSON,
  jsonStringifyStream,
  jsonStringify,
  serializeStreamToJSON,
  deserializeStreamFromJSON,
  bigintReplacer,
  bigintReviver,
  validateStringLength,
  STRING_FIELD_LIMITS,
  filterStreams,
  sortStreams,
  detectNetworkFromRpcUrl,
  parseMemo,
  encodeStreamId,
  decodeStreamId,
} from './utils.js';
export { templates } from './templates.js';
export { serializeStream, deserializeStream } from './serialization.js';
export type { SerializedStream } from './serialization.js';
export { getTransactionHistory, getAddressActivity } from './horizon.js';
export type {
  StreamTransaction,
  TransactionHistoryPage,
  TransactionHistoryOptions,
} from './horizon.js';
export { CircuitBreaker } from './circuitBreaker.js';
export { withRetry, RetryBackoff } from './retry.js';
export type { RetryOptions } from './retry.js';
export type { CircuitState, CircuitBreakerOptions } from './circuitBreaker.js';
export { ConnectionPool } from './connectionPool.js';
export type { ConnectionPoolOptions, PoolEvent, PoolEventType } from './connectionPool.js';
export { InMemoryEventBus } from './eventBus.js';
export type { IEventBus, Unsubscribe } from './eventBus.js';
export type { StreamRetryPolicy, EventPollerOptions } from './events.js';
export type { BatchingOptions, BatchMetrics, CompressionOptions } from './types.js';
export { createContractEncoder } from './contractEncoders.js';
export type { ContractCallEncoder } from './contractEncoders.js';
export { createSimplePriceFeed } from './priceFeed.js';
export type { SimplePriceFeedOptions } from './priceFeed.js';
export { encodeMemo, encodeMemoHash, decodeMemo } from './memo.js';
export {
  SoroStreamError,
  InsufficientAmountError,
  StreamNotFoundError,
  StreamNotActiveError,
  TransactionFailedError,
  InvalidAddressError,
  AccountNotFoundError,
  InsufficientBalanceError,
  ZeroDurationError,
  BulkCreatePartialError,
  InsufficientAllowanceError,
  FederationResolutionError,
  SoroStreamValidationError,
  StartTimeInPastError,
  NonceNotSupportedError,
  SoroStreamRetryExhaustedError,
  SoroStreamMemoError,
  SelfStreamError,
  SoroStreamTransportError,
} from './errors.js';
export { checkPeerDependencies } from './peerDependencies.js';
export {
  isSoroStreamError,
  isNetworkError,
  isContractError,
  isValidationError,
  isAuthError,
  matchError,
} from './error-guards.js';
export type {
  NetworkError,
  ContractError,
  ValidationError,
  AuthError,
  ErrorMatchHandlers,
} from './error-guards.js';
export type { BulkCreateFailedSlot, RetryAttempt } from './errors.js';
export { parseContractEvent } from './contractEvents.js';
export type {
  ContractEventPayload,
  StreamCreatedPayload,
  WithdrawalPayload,
  CancelledStreamPayload,
  StreamCompletedPayload,
  StreamToppedUpPayload,
  StreamPausedPayload,
  StreamResumedPayload,
  StreamTransferredPayload,
  UnknownEventPayload,
} from './contractEvents.js';
export type {
  Stream,
  StreamStatus,
  CreateStreamParams,
  CloneStreamOverrides,
  EventHandler,
  WithdrawParams,
  CancelStreamParams,
  TopUpParams,
  TransferStreamParams,
  PauseStreamParams,
  ResumeStreamParams,
  UpdateFlowRateParams,
  SetOperatorParams,
  OperatorTopUpParams,
  AddDelegateParams,
  RevokeDelegateParams,
  Network,
  WalletAdapter,
  WalletAdapterSignResult,
  FeeEstimate,
  VestingSchedulePoint,
  VestingScheduleResult,
  WatchClaimableOptions,
  WatchTotalClaimableOptions,
  BulkStreamRow,
  BulkCreateOptions,
  BulkCreateBatchResult,
  BulkCreateResult,
  BatchCancelResult,
  BatchWithdrawResult,
  BatchWithdrawPartialResult,
  TokenAggregate,
  MultisigSigner,
  StreamEvent,
  StreamEventType,
  StreamEventFilter,
  StreamSubscription,
  PaginationParams,
  PaginatedStreams,
  WebhookConfig,
  WriteOptions,
  FormatUSDCOptions,
  StreamDrift,
  ReconcileStreamOptions,
  PasskeyAdapterConfig,
  PriceFeedAdapter,
  FeeBumpOptions,
  ContractVersion,
  CompatibilityResult,
  SplitStreamParams,
  SplitStreamResult,
  StreamTotals,
  StatusBreakdown,
  DurationStats,
  StreamHealthReport,
  RecipientAggregate,
  StreamSnapshot,
  StreamHistoryEntry,
  SnapshotVestingPoint,
  SoroStreamPlugin,
  MiddlewareContext,
  RecipientChangedEvent,
  OnRecipientChangedOptions,
  StreamActivityEntry,
  StreamActivityType,
  GetActivityLogOptions,
  StreamFilterCriteria,
  StreamFilter,
  StreamSortField,
  SortOrder,
  TokenMetadata,
  MemoHash,
  HorizonTransactionRecord,
  ParsedMemo,
  StreamCreatedEventPayload,
  StreamWithdrawnEventPayload,
  StreamCancelledEventPayload,
  RpcErrorEventPayload,
  CacheInvalidatedEventPayload,
  SoroStreamEventMap,
  KmsWalletAdapterConfig,
  HealthCheckResult,
  ExportStreamHistoryOptions,
  OperationExplanation,
  BalanceDelta,
  SoroStreamClientConfig,
  RecipientTrustScore,
  RecipientTrustScoreProvider,
} from './types.js';

export { RecipientValidationError } from './errors.js';
export { PluginRegistry } from './pluginRegistry.js';
export { getPortfolioStats } from './portfolioAnalytics.js';
export { scheduleFeeBumpMonitor } from './feeBump.js';
export { createFeeRetryMiddleware, FeeRetryError } from './feeRetryMiddleware.js';
export type { FeeRetryMiddlewareOptions } from './feeRetryMiddleware.js';
export { createFederationPlugin } from './federationPlugin.js';
export type { FederationPluginOptions } from './federationPlugin.js';

// ── Issue #396: Non-7-decimal token support ────────────────────────────────
export {
  toStroopsForToken,
  fromStroopsForToken,
  getTokenDecimals,
  convertToTokenUnits,
  formatTokenAmount,
} from './decimalAware.js';
export type { TokenMetadataProvider } from './decimalAware.js';

// ── Issue #395: Replay transport ───────────────────────────────────────────
export { ReplayTransport, ReplayFixtureError, ReplayFixtureParseError } from './replayTransport.js';
export type {
  FixtureEntry,
  FixtureFile,
  RecordTransportOptions,
  RecordingRpcTransport,
  ReplayingRpcTransport,
} from './replayTransport.js';

// ── Issue #394: Address book ───────────────────────────────────────────────
export {
  InMemoryAddressBook,
  PersistedAddressBook,
  AddressBookValidationError,
  AddressBookNotFoundError,
} from './addressBook.js';
export type {
  AddressBook,
  AddressBookEntry,
  PersistedAddressBookOptions,
} from './addressBook.js';

// ── Issue #393: Multi-network client ──────────────────────────────────────
export {
  MultiNetworkClient,
  MultiNetworkConfigError,
  MultiNetworkNotFoundError,
} from './multiNetwork.js';
export type { NetworkConfig, NetworkedStream, MultiNetworkStreams } from './multiNetwork.js';
