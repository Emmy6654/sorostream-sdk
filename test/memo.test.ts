import { describe, it, expect, vi } from "vitest";
import { Memo, MemoNone } from "@stellar/stellar-sdk";
import { encodeMemo, encodeMemoHash, decodeMemo } from "../src/memo.js";
import { SoroStreamMemoError } from "../src/errors.js";

describe("encodeMemo", () => {
  it("encodes a short text memo", () => {
    const memo = encodeMemo("invoice-4821");
    expect(memo.type).toBe("text");
    expect(memo.value).toBe("invoice-4821");
  });

  it("accepts text at exactly the 28-byte limit", () => {
    const text = "a".repeat(28);
    const memo = encodeMemo(text);
    expect(memo.value).toBe(text);
  });

  it("throws SoroStreamMemoError for text over 28 bytes", () => {
    const text = "a".repeat(29);
    expect(() => encodeMemo(text)).toThrow(SoroStreamMemoError);
  });

  it("counts UTF-8 byte length, not character length", () => {
    // Each "€" is 3 bytes in UTF-8, so 10 of them is 30 bytes — over the limit.
    expect(() => encodeMemo("€".repeat(10))).toThrow(SoroStreamMemoError);
  });
});

describe("encodeMemoHash", () => {
  it("encodes a 32-byte input unchanged", () => {
    const data = Buffer.alloc(32, 7);
    const memo = encodeMemoHash(data);
    expect(memo.type).toBe("hash");
    expect((memo.value as Buffer).equals(data)).toBe(true);
  });

  it("pads short input to 32 bytes", () => {
    const data = Buffer.from([1, 2, 3]);
    const memo = encodeMemoHash(data);
    const value = memo.value as Buffer;
    expect(value.length).toBe(32);
    expect(value.subarray(0, 3).equals(data)).toBe(true);
    expect(value.subarray(3).every((b) => b === 0)).toBe(true);
  });

  it("truncates long input to 32 bytes and warns", () => {
    const data = Buffer.alloc(40, 9);
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const memo = encodeMemoHash(data);
    const value = memo.value as Buffer;
    expect(value.length).toBe(32);
    expect(value.equals(data.subarray(0, 32))).toBe(true);
    expect(warnSpy).toHaveBeenCalled();
    warnSpy.mockRestore();
  });

  it("accepts a Uint8Array", () => {
    const data = new Uint8Array([1, 2, 3, 4]);
    const memo = encodeMemoHash(data);
    const value = memo.value as Buffer;
    expect(value.length).toBe(32);
    expect(value.subarray(0, 4)).toEqual(Buffer.from(data));
  });
});

describe("decodeMemo", () => {
  it("returns null for a no-memo transaction", () => {
    expect(decodeMemo(Memo.none())).toBeNull();
  });

  it("decodes a text memo", () => {
    expect(decodeMemo(Memo.text("hello"))).toBe("hello");
  });

  it("decodes an id memo", () => {
    expect(decodeMemo(Memo.id("12345"))).toBe("12345");
  });

  it("decodes a hash memo as a Buffer", () => {
    const data = Buffer.alloc(32, 1);
    const decoded = decodeMemo(Memo.hash(data));
    expect(Buffer.isBuffer(decoded)).toBe(true);
    expect((decoded as Buffer).equals(data)).toBe(true);
  });

  it("decodes a return memo as a Buffer", () => {
    const data = Buffer.alloc(32, 2);
    const decoded = decodeMemo(Memo.return(data));
    expect(Buffer.isBuffer(decoded)).toBe(true);
    expect((decoded as Buffer).equals(data)).toBe(true);
  });

  it("round-trips a memo produced by encodeMemo", () => {
    const memo = encodeMemo("round-trip");
    expect(decodeMemo(memo)).toBe("round-trip");
  });

  it("round-trips a memo produced by encodeMemoHash", () => {
    const data = Buffer.from([9, 9, 9]);
    const memo = encodeMemoHash(data);
    const decoded = decodeMemo(memo) as Buffer;
    expect(decoded.subarray(0, 3).equals(data)).toBe(true);
  });

  it("returns null via MemoNone constant match", () => {
    const memo = new Memo(MemoNone);
    expect(decodeMemo(memo)).toBeNull();
  });
});
