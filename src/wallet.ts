import { Keypair, TransactionBuilder, xdr, hash, nativeToScVal } from "@stellar/stellar-sdk";
import type { WalletAdapter, Network, MultisigSigner, PasskeyAdapterConfig } from "./types.js";

/**
 * Configuration for a claim-delegation adapter.
 *
 * The pattern lets an automated "claim bot" key call `withdraw` on behalf of the
 * recipient without ever holding the recipient's primary key:
 *
 * 1. On-chain: add the bot key as a co-signer on the recipient's Stellar account
 *    with a weight that meets the low-security threshold (e.g. weight 1 on a 1-of-N
 *    multisig). The primary key retains sole control over high-security operations.
 * 2. In the SDK: pass the recipient address as `recipientAddress` and the bot's
 *    {@link MultisigSigner} as `claimBotSigner`.
 *
 * The resulting adapter always presents `recipientAddress` to `getPublicKey()`
 * (so `withdraw` receives the correct recipient auth), but the transaction envelope
 * is signed exclusively by the claim bot key.
 *
 * @example
 * ```ts
 * // Bot key loaded from env — never has custody of the recipient address.
 * const botSigner: MultisigSigner = {
 *   async signTransaction(xdr, network) {
 *     const kp = Keypair.fromSecret(process.env.CLAIM_BOT_SECRET!);
 *     const tx = TransactionBuilder.fromXDR(xdr, Networks.TESTNET);
 *     tx.sign(kp);
 *     return tx.toEnvelope().toXDR("base64");
 *   },
 * };
 *
 * const adapter = createClaimDelegateAdapter({
 *   recipientAddress: "GRECIPI...",
 *   claimBotSigner: botSigner,
 * });
 *
 * const client = new SoroStreamClient({ network: "testnet", contractId, walletAdapter: adapter });
 * await client.withdraw({ streamId });
 * ```
 */
export interface ClaimDelegateConfig {
  /** The actual recipient address (passed to `require_auth` on-chain). */
  recipientAddress: string;
  /** A signer representing the claim bot key. */
  claimBotSigner: MultisigSigner;
}

const NETWORK_PASSPHRASES: Record<Network, string> = {
  mainnet: "Public Global Stellar Network ; September 2015",
  testnet: "Test SDF Network ; September 2015",
  futurenet: "Test SDF Future Network ; October 2022",
};

/**
 * Creates a WalletAdapter backed by the Freighter browser extension.
 * Dynamically imports @stellar/freighter-api to avoid SSR issues.
 */
export async function createFreighterAdapter(): Promise<WalletAdapter> {
  const freighter = await import("@stellar/freighter-api");

  return {
    async isConnected(): Promise<boolean> {
      const result = await freighter.isConnected();
      return result.isConnected;
    },

    async getPublicKey(): Promise<string> {
      const result = await freighter.getAddress();
      if (result.error) throw new Error(result.error.message);
      return result.address;
    },

    async signTransaction(xdrStr: string, network: Network): Promise<string> {
      const result = await freighter.signTransaction(xdrStr, {
        networkPassphrase: NETWORK_PASSPHRASES[network],
      });
      if (result.error) throw new Error(result.error.message);
      return result.signedTxXdr;
    },
  };
}

/**
 * Creates a server-side WalletAdapter that signs directly with a Stellar Keypair.
 * Suitable for Node.js scripts, backends, and automated payouts.
 *
 * @param secretKey - The Stellar secret key (base-32 encoded seed starting with "S").
 *
 * @example
 * ```ts
 * const adapter = createKeypairAdapter("SAZ...YOUR...SECRET...KEY...");
 * const client = new SoroStreamClient({ network: "testnet", contractId: "...", walletAdapter: adapter });
 * ```
 */
export function createKeypairAdapter(secretKey: string): WalletAdapter {
  const keypair = Keypair.fromSecret(secretKey);

  return {
    async isConnected(): Promise<boolean> {
      return true;
    },

    async getPublicKey(): Promise<string> {
      return keypair.publicKey();
    },

    async signTransaction(xdrStr: string, network: Network): Promise<string> {
      const tx = TransactionBuilder.fromXDR(
        xdrStr,
        NETWORK_PASSPHRASES[network]
      );
      tx.sign(keypair);
      return tx.toEnvelope().toXDR("base64");
    },
  };
}

/**
 * Prompts the user to connect their Freighter wallet.
 * Throws if Freighter is not installed or the user rejects.
 */
export async function connectWallet(): Promise<string> {
  const freighter = await import("@stellar/freighter-api");
  const connected = await freighter.isConnected();
  if (!connected.isConnected) {
    throw new Error("Freighter extension is not installed");
  }
  const result = await freighter.getAddress();
  if (result.error) throw new Error(result.error.message);
  return result.address;
}

/**
 * Creates a {@link WalletAdapter} that presents the recipient's address to the
 * contract but signs transactions with a separate claim-bot key.
 *
 * The bot key must be a co-signer on the recipient's Stellar account (classic
 * multisig) so that Soroban's `require_auth` accepts its signature for `withdraw`.
 *
 * This enables automated claiming daemons that never hold the recipient's primary
 * secret key. See {@link ClaimDelegateConfig} for the full setup guide.
 */
export function createClaimDelegateAdapter(
  config: ClaimDelegateConfig
): WalletAdapter {
  return {
    async isConnected(): Promise<boolean> {
      return true;
    },

    async getPublicKey(): Promise<string> {
      return config.recipientAddress;
    },

    async signTransaction(xdr: string, network: Network): Promise<string> {
      return config.claimBotSigner.signTransaction(xdr, network);
    },
  };
}

/**
 * Creates a WalletAdapter for a multi-sig Stellar account.
 *
 * The adapter collects signatures from each signer and combines them into
 * a single transaction envelope before submission.
 *
 * @param config.address - The multisig source account address.
 * @param config.signers - Array of signers that each independently sign the tx.
 * @param config.threshold - Optional minimum number of signatures required
 *   (defaults to `signers.length`, i.e. all must sign).
 */
export async function createMultisigAdapter(config: {
  address: string;
  signers: MultisigSigner[];
  threshold?: number;
}): Promise<WalletAdapter> {
  const threshold = config.threshold ?? config.signers.length;

  return {
    async isConnected(): Promise<boolean> {
      return true;
    },

    async getPublicKey(): Promise<string> {
      return config.address;
    },

    async signTransaction(xdrStr: string, network: Network): Promise<string> {
      const passphrase = NETWORK_PASSPHRASES[network];

      let combined: ReturnType<typeof TransactionBuilder.fromXDR> | null = null;
      let collected = 0;
      const seen = new Set<string>();

      for (const signer of config.signers) {
        if (collected >= threshold) break;

        const signedXdr = await signer.signTransaction(xdrStr, network);
        const tx = TransactionBuilder.fromXDR(signedXdr, passphrase);

        for (const sig of tx.signatures) {
          const key =
            sig.hint().toString("base64") +
            sig.signature().toString("base64");
          if (!seen.has(key)) {
            seen.add(key);
            if (!combined) {
              combined = TransactionBuilder.fromXDR(xdrStr, passphrase);
            }
            combined.signatures.push(sig);
            collected++;
          }
        }
      }

      if (!combined) {
        throw new Error("No signatures were collected");
      }

      return combined.toEnvelope().toXDR("base64");
    },
  };
}

// ── Issue #46: WebAuthn passkey adapter ──────────────────────────────────────

/**
 * Converts a DER-encoded P-256 ECDSA signature to compact (r || s) form.
 * DER format: 0x30 <total-len> 0x02 <r-len> <r> 0x02 <s-len> <s>
 */
function derToCompact(der: Uint8Array): Uint8Array {
  let offset = 0;
  if (der[offset++] !== 0x30) throw new Error("Invalid DER signature: expected 0x30");
  offset++; // skip total length byte
  if (der[offset++] !== 0x02) throw new Error("Invalid DER signature: expected 0x02 for r");
  const rLen = der[offset++];
  if (rLen === undefined) throw new Error("Invalid DER signature: truncated r length");
  const r = der.slice(offset, offset + rLen);
  offset += rLen;
  if (der[offset++] !== 0x02) throw new Error("Invalid DER signature: expected 0x02 for s");
  const sLen = der[offset++];
  if (sLen === undefined) throw new Error("Invalid DER signature: truncated s length");
  const s = der.slice(offset, offset + sLen);

  const compact = new Uint8Array(64);
  // r and s may have a leading 0x00 padding byte; trim and right-align to 32 bytes
  const rBytes = r[0] === 0 ? r.slice(1) : r;
  const sBytes = s[0] === 0 ? s.slice(1) : s;
  compact.set(rBytes, 32 - rBytes.length);
  compact.set(sBytes, 64 - sBytes.length);
  return compact;
}

/**
 * Creates a WalletAdapter for a Soroban smart-wallet contract that is
 * authenticated via WebAuthn/passkeys rather than a classic Ed25519 keypair.
 *
 * The adapter signs each `invokeHostFunction` auth entry by:
 *  1. Computing the Soroban contract-auth signing challenge (SHA-256 of the
 *     `HashIdPreimageSorobanAuthorization` XDR).
 *  2. Requesting a WebAuthn assertion from the registered passkey.
 *  3. Attaching the response (`authenticator_data`, `client_data_json`,
 *     compact `signature`) as a ScVal map in the auth entry credentials.
 *
 * This follows the Soroban Passkey Kit signature format expected by the
 * `__check_auth` function on standard Soroban smart wallet contracts.
 *
 * **Requirements:** Must be called in a browser environment with WebAuthn
 * support. The contract must already be deployed.
 *
 * @param config - Passkey adapter configuration.
 *
 * @example
 * ```ts
 * const adapter = await createPasskeyAdapter({
 *   contractId: "CA...",
 *   rpId: "myapp.example.com",
 *   credentialId: myCredentialIdArrayBuffer,
 * });
 * const client = new SoroStreamClient({ network: "testnet", contractId: "...", walletAdapter: adapter });
 * ```
 */
export async function createPasskeyAdapter(
  config: PasskeyAdapterConfig
): Promise<WalletAdapter> {
  if (
    typeof window === "undefined" ||
    !("credentials" in navigator) ||
    !("PublicKeyCredential" in window)
  ) {
    throw new Error("WebAuthn is not available in this environment");
  }

  return {
    async isConnected(): Promise<boolean> {
      return (
        typeof window !== "undefined" &&
        "credentials" in navigator &&
        "PublicKeyCredential" in window
      );
    },

    async getPublicKey(): Promise<string> {
      return config.contractId;
    },

    async signTransaction(xdrStr: string, network: Network): Promise<string> {
      const passphrase = NETWORK_PASSPHRASES[network];

      // Parse the raw transaction envelope so we can read and mutate auth entries
      const txEnvelope = xdr.TransactionEnvelope.fromXDR(xdrStr, "base64");
      const v1Body = txEnvelope.v1().tx();
      let modified = false;

      for (const op of v1Body.operations()) {
        const body = op.body();
        if (body.switch().name !== "invokeHostFunction") continue;

        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const invokeOp = (body as any).invokeHostFunction() as xdr.InvokeHostFunctionOp;
        const authArr = invokeOp.auth();

        for (let i = 0; i < authArr.length; i++) {
          const entry = authArr[i];
          if (!entry) continue;
          const creds = entry.credentials();
          if (creds.switch().name !== "sorobanCredentialsAddress") continue;

          const addrCreds = creds.address();

          // Build the Soroban authorization signing preimage
          // (ENVELOPE_TYPE_SOROBAN_AUTHORIZATION)
          const networkId = hash(Buffer.from(passphrase));
          const preimage = xdr.HashIdPreimage.envelopeTypeSorobanAuthorization(
            new xdr.HashIdPreimageSorobanAuthorization({
              networkId,
              nonce: addrCreds.nonce(),
              signatureExpirationLedger: addrCreds.signatureExpirationLedger(),
              invocation: entry.rootInvocation(),
            })
          );
          const challengeHash = hash(preimage.toXDR());
          // Convert to a plain Uint8Array backed by a fresh ArrayBuffer (required by WebAuthn API)
          const challenge = Uint8Array.from(challengeHash);

          // Request WebAuthn assertion using the signing challenge
          const assertion = (await navigator.credentials.get({
            publicKey: {
              challenge,
              rpId: config.rpId,
              allowCredentials: [
                { type: "public-key" as const, id: config.credentialId },
              ],
              userVerification: "required",
            },
          })) as PublicKeyCredential | null;

          if (!assertion) {
            throw new Error("WebAuthn: authentication was cancelled or failed");
          }

          const response = assertion.response as AuthenticatorAssertionResponse;
          const compactSig = derToCompact(new Uint8Array(response.signature));

          // Replace auth entry in-place with the WebAuthn credential.
          // Signature map format required by Soroban Passkey Kit __check_auth:
          //   { authenticator_data: Bytes, client_data_json: Bytes, signature: Bytes }
          authArr[i] = new xdr.SorobanAuthorizationEntry({
            credentials: xdr.SorobanCredentials.sorobanCredentialsAddress(
              new xdr.SorobanAddressCredentials({
                address: addrCreds.address(),
                nonce: addrCreds.nonce(),
                signatureExpirationLedger: addrCreds.signatureExpirationLedger(),
                signature: nativeToScVal({
                  authenticator_data: Buffer.from(response.authenticatorData),
                  client_data_json: Buffer.from(response.clientDataJSON),
                  signature: Buffer.from(compactSig),
                }),
              })
            ),
            rootInvocation: entry.rootInvocation(),
          });

          modified = true;
        }
      }

      if (!modified) return xdrStr;

      return txEnvelope.toXDR("base64");
    },
  };
}

declare const require: any;

export function createLedgerAdapter(options: { transport: any }): WalletAdapter {
  return {
    async isConnected(): Promise<boolean> {
      return true;
    },
    async getPublicKey(): Promise<string> {
      const hwAppStrModule = require("@ledgerhq/hw-app-str");
      const StrClass = hwAppStrModule.default || hwAppStrModule;
      const str = new StrClass(options.transport);
      const res = await str.getPublicKey("44'/148'/0'");
      return res.publicKey;
    },
    async signTransaction(xdrStr: string): Promise<string> {
      return xdrStr;
    },
  };
}
