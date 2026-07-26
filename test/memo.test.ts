/**
 * Tests for issue #201: structured memo support for write transactions.
 *
 * - `WriteOptions.memo` accepts a text string or a 32-byte hash Buffer.
 * - `buildAndSubmit` (used by every single-operation write method) attaches
 *   the memo to the transaction when provided, and is unaffected when omitted.
 * - `parseMemo` decodes both text and hash memos (and "no memo") from a
 *   Horizon transaction record.
 */
import { describe, it, expect, vi } from "vitest";
import { SoroStreamClient } from "../src/SoroStreamClient.js";
import { parseMemo } from "../src/utils.js";
import type { WalletAdapter } from "../src/types.js";

const VALID_CONTRACT = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM";
const VALID_ACCOUNT = "GAXXZ5XSL2VTQPGWB3LPU5273HSJXMK7VHLZTF2XKW65QFZVA3XKULQZ";
const VALID_RECIPIENT = "GAXXZ5XSL2VTQPGWB3LPU5273HSJXMK7VHLZTF2XKW65QFZVA3XKULQZ";
const VALID_TOKEN = "CAVTXNC2WCHINDNP4VBLSOQA2667VE3RPQZNGD5TFI4U2QSHTVAC667T";

function makeAdapter(): WalletAdapter {
  return {
    getPublicKey: vi.fn().mockResolvedValue(VALID_ACCOUNT),
    signTransaction: vi.fn().mockResolvedValue("signed_xdr"),
    isConnected: vi.fn().mockResolvedValue(true),
  };
}

function makeClient() {
  return new SoroStreamClient({
    network: "testnet",
    contractId: VALID_CONTRACT,
    walletAdapter: makeAdapter(),
  });
}

describe("#201 buildMemo (private helper)", () => {
  it("encodes a text memo", () => {
    const client = makeClient();
    const memo = (client as any).buildMemo("invoice-123");
    expect(memo.type).toBe("text");
    expect(memo.value).toBe("invoice-123");
  });

  it("throws when a text memo exceeds 28 bytes", () => {
    const client = makeClient();
    const tooLong = "a".repeat(29);
    expect(() => (client as any).buildMemo(tooLong)).toThrow(/28-byte/);
  });

  it("accepts a text memo at exactly the 28-byte limit", () => {
    const client = makeClient();
    const exact = "a".repeat(28);
    expect(() => (client as any).buildMemo(exact)).not.toThrow();
  });

  it("encodes a 32-byte hash memo as a MEMO_HASH", () => {
    const client = makeClient();
    const hash = Buffer.alloc(32, 7);
    const memo = (client as any).buildMemo(hash);
    expect(memo.type).toBe("hash");
    expect(Buffer.isBuffer(memo.value)).toBe(true);
    expect(memo.value.length).toBe(32);
    expect(memo.value.equals(hash)).toBe(true);
  });

  it("throws when a hash memo is not exactly 32 bytes", () => {
    const client = makeClient();
    expect(() => (client as any).buildMemo(Buffer.alloc(31))).toThrow(/32 bytes/);
    expect(() => (client as any).buildMemo(Buffer.alloc(33))).toThrow(/32 bytes/);
  });
});

describe("#201 memo passthrough on write methods", () => {
  it("createStream forwards options.memo to buildAndSubmit", async () => {
    const client = makeClient();
    vi.spyOn(client as any, "validateStreamParams").mockResolvedValue(undefined);
    vi.spyOn(client as any, "checkAllowance").mockResolvedValue(undefined);
    const buildSpy = vi
      .spyOn(client as any, "buildAndSubmit")
      .mockResolvedValue("txhash");
    vi.spyOn(client, "getStreamsBySender").mockResolvedValue([
      {
        id: "1",
        sender: VALID_ACCOUNT,
        recipient: VALID_RECIPIENT,
        token: VALID_TOKEN,
        deposit: 100n,
        flowRate: 1n,
        startTime: 0,
        endTime: 3600,
        lastWithdrawTime: 0,
        status: "Active",
        autoRenew: false,
      },
    ]);

    await client.createStream(
      {
        recipient: VALID_RECIPIENT,
        token: VALID_TOKEN,
        amount: 1_000_000_000n,
        durationSeconds: 3600,
        autoRenew: false,
      },
      undefined,
      { memo: "order-42" }
    );

    expect(buildSpy).toHaveBeenCalledWith(
      expect.anything(),
      undefined,
      undefined,
      "createStream",
      "order-42"
    );
  });

  it("withdraw forwards options.memo to buildAndSubmit", async () => {
    const client = makeClient();
    vi.spyOn(client, "getClaimable").mockResolvedValue(500n);
    const buildSpy = vi
      .spyOn(client as any, "buildAndSubmit")
      .mockResolvedValue("txhash");

    const hashMemo = Buffer.alloc(32, 9);
    await client.withdraw({ streamId: "1" }, undefined, { memo: hashMemo });

    expect(buildSpy).toHaveBeenCalledWith(
      expect.anything(),
      undefined,
      undefined,
      "withdraw",
      hashMemo
    );
  });

  it("withdraw without a memo option leaves the transaction unaffected", async () => {
    const client = makeClient();
    vi.spyOn(client, "getClaimable").mockResolvedValue(500n);
    const buildSpy = vi
      .spyOn(client as any, "buildAndSubmit")
      .mockResolvedValue("txhash");

    await client.withdraw({ streamId: "1" });

    expect(buildSpy).toHaveBeenCalledWith(
      expect.anything(),
      undefined,
      undefined,
      "withdraw",
      undefined
    );
  });
});

describe("#201 parseMemo", () => {
  it("returns { type: none, value: null } when memo_type is 'none'", () => {
    expect(parseMemo({ memo_type: "none" })).toEqual({ type: "none", value: null });
  });

  it("returns { type: none, value: null } when memo_type is absent", () => {
    expect(parseMemo({})).toEqual({ type: "none", value: null });
  });

  it("decodes a text memo as-is", () => {
    expect(parseMemo({ memo_type: "text", memo: "order-42" })).toEqual({
      type: "text",
      value: "order-42",
    });
  });

  it("decodes an id memo as-is", () => {
    expect(parseMemo({ memo_type: "id", memo: "123456789" })).toEqual({
      type: "id",
      value: "123456789",
    });
  });

  it("decodes a hash memo from base64 into a 32-byte Buffer", () => {
    const raw = Buffer.alloc(32, 3);
    const parsed = parseMemo({ memo_type: "hash", memo: raw.toString("base64") });
    expect(parsed.type).toBe("hash");
    expect(Buffer.isBuffer(parsed.value)).toBe(true);
    expect((parsed.value as Buffer).length).toBe(32);
    expect((parsed.value as Buffer).equals(raw)).toBe(true);
  });

  it("decodes a return-hash memo from base64 into a Buffer", () => {
    const raw = Buffer.alloc(32, 5);
    const parsed = parseMemo({ memo_type: "return", memo: raw.toString("base64") });
    expect(parsed.type).toBe("return");
    expect((parsed.value as Buffer).equals(raw)).toBe(true);
  });
});
