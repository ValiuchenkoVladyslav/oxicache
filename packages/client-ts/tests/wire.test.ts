import { describe, expect, test } from "bun:test";
import {
  DecodeError,
  FrameReader,
  HEADER_LEN,
  MAX_ITEMS,
  Op,
  decodeFlags,
  decodeValues,
  encodeEntriesFrame,
  encodeKeysFrame,
  encodeRawFrame,
} from "../src/wire";

const le32 = (n: number) => [n & 255, (n >>> 8) & 255, (n >>> 16) & 255, (n >>> 24) & 255];

describe("encode", () => {
  test("keys frame", () => {
    const f = encodeKeysFrame(Op.Get, ["a", new Uint8Array([0, 255]), ""]);
    expect([...f]).toEqual([
      1, ...le32(4 + (4 + 1) + (4 + 2) + 4),
      ...le32(3),
      ...le32(1), 0x61,
      ...le32(2), 0, 255,
      ...le32(0),
    ]);
  });

  test("entries frame", () => {
    const f = encodeEntriesFrame([["k", "v"], [new Uint8Array([1]), new Uint8Array([2, 3])]]);
    expect([...f]).toEqual([
      2, ...le32(4 + (4 + 1 + 4 + 1) + (4 + 1 + 4 + 2)),
      ...le32(2),
      ...le32(1), 0x6b, ...le32(1), 0x76,
      ...le32(1), 1, ...le32(2), 2, 3,
    ]);
  });

  test("raw frame and utf-8 strings", () => {
    const f = encodeRawFrame(Op.Auth, new TextEncoder().encode("é"));
    expect([...f]).toEqual([4, ...le32(2), 0xc3, 0xa9]);
    expect(encodeKeysFrame(Op.Del, ["é"]).length).toBe(HEADER_LEN + 4 + 4 + 2);
  });

  test("rejects too many items", () => {
    expect(() => encodeKeysFrame(Op.Get, new Array(MAX_ITEMS + 1).fill(""))).toThrow(RangeError);
  });
});

describe("decode", () => {
  test("values", () => {
    const body = new Uint8Array([...le32(3), 1, ...le32(1), 0x78, 0, 1, ...le32(0)]);
    const v = decodeValues(body);
    expect(v.length).toBe(3);
    expect([...v[0]!]).toEqual([0x78]);
    expect(v[1]).toBeNull();
    expect(v[2]!.length).toBe(0);
  });

  test("flags", () => {
    expect(decodeFlags(new Uint8Array([...le32(3), 1, 0, 1]))).toEqual([true, false, true]);
    expect(decodeFlags(new Uint8Array(le32(0)))).toEqual([]);
  });

  test("rejects malformed", () => {
    expect(() => decodeValues(new Uint8Array([1, 0]))).toThrow(DecodeError);
    expect(() => decodeValues(new Uint8Array([...le32(1), 7]))).toThrow(/invalid tag byte 7/);
    expect(() => decodeValues(new Uint8Array([...le32(1), 1, ...le32(100)]))).toThrow(/needed 100/);
    expect(() => decodeFlags(new Uint8Array([...le32(0), 9]))).toThrow(/trailing 1 bytes/);
  });
});

describe("FrameReader", () => {
  test("reassembles frames split across chunks", () => {
    const r = new FrameReader();
    const frame = [0, ...le32(3), 7, 8, 9];
    const all = new Uint8Array([...frame, ...frame, 1, ...le32(0)]);
    const got: number[][] = [];
    for (let i = 0; i < all.length; i++) {
      r.push(all.subarray(i, i + 1));
      for (let f = r.next(); f; f = r.next()) got.push([f.tag, ...f.body]);
    }
    expect(got).toEqual([[0, 7, 8, 9], [0, 7, 8, 9], [1]]);
  });

  test("body outlives later pushes", () => {
    const r = new FrameReader();
    r.push(new Uint8Array([0, ...le32(1), 42]));
    const f = r.next()!;
    r.push(new Uint8Array([0, ...le32(1), 43]));
    r.next();
    expect([...f.body]).toEqual([42]);
  });

  test("grows past the initial buffer", () => {
    const r = new FrameReader();
    const big = new Uint8Array(1 << 20).fill(5);
    r.push(new Uint8Array([0, ...le32(big.length)]));
    expect(r.next()).toBeNull();
    for (let off = 0; off < big.length; off += 100_000) r.push(big.subarray(off, off + 100_000));
    const f = r.next()!;
    expect(f.body.length).toBe(big.length);
    expect(f.body[big.length - 1]).toBe(5);
  });

  test("rejects oversized frames", () => {
    const r = new FrameReader(16);
    r.push(new Uint8Array([0, ...le32(17)]));
    expect(() => r.next()).toThrow(/exceeds the client limit of 16/);
  });
});
