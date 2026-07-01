import { describe, it, expect, vi, beforeEach } from "vitest";
import { isFederationAddress, resolveFederationAddress } from "../src/utils.js";
import { FederationResolutionError } from "../src/errors.js";

describe("isFederationAddress", () => {
  it("returns true for valid federation addresses", () => {
    expect(isFederationAddress("alice*example.com")).toBe(true);
    expect(isFederationAddress("bob*stellar.org")).toBe(true);
  });

  it("returns false for raw Stellar keys", () => {
    expect(isFederationAddress("GABC1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ234567890ABCDEF")).toBe(false);
  });

  it("returns false for invalid formats", () => {
    expect(isFederationAddress("notafedaddress")).toBe(false);
    expect(isFederationAddress("")).toBe(false);
    expect(isFederationAddress("user@domain.com")).toBe(false);
  });
});

describe("resolveFederationAddress", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("resolves a federation address to a Stellar key", async () => {
    const mockPublicKey = "GABC2DEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOPQRSTUV";
    vi.stubGlobal("fetch", vi.fn()
      .mockResolvedValueOnce({
        ok: true,
        text: async () => `FEDERATION_SERVER="https://federation.example.com"`,
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({ account_id: mockPublicKey }),
      })
    );

    const result = await resolveFederationAddress("alice*example.com");
    expect(result).toBe(mockPublicKey);
  });

  it("throws FederationResolutionError when stellar.toml is unreachable", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValueOnce({ ok: false, status: 404 }));

    await expect(resolveFederationAddress("alice*example.com")).rejects.toThrow(
      FederationResolutionError
    );
  });

  it("throws FederationResolutionError when FEDERATION_SERVER is missing", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValueOnce({
      ok: true,
      text: async () => `NETWORK_PASSPHRASE="Test SDF Network ; September 2015"`,
    }));

    await expect(resolveFederationAddress("alice*example.com")).rejects.toThrow(
      FederationResolutionError
    );
  });

  it("throws FederationResolutionError when federation server is unreachable", async () => {
    vi.stubGlobal("fetch", vi.fn()
      .mockResolvedValueOnce({
        ok: true,
        text: async () => `FEDERATION_SERVER="https://federation.example.com"`,
      })
      .mockResolvedValueOnce({ ok: false, status: 503 })
    );

    await expect(resolveFederationAddress("alice*example.com")).rejects.toThrow(
      FederationResolutionError
    );
  });

  it("throws for invalid address format", async () => {
    await expect(resolveFederationAddress("nodomain")).rejects.toThrow(
      FederationResolutionError
    );
  });
});
