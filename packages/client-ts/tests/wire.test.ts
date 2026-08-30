import { describe, expect, test } from "bun:test";
import {
  DecodeError,
  decodeReplies,
  encodeBatchFrame,
  encodeDelFrame,
  encodeGetFrame,
  encodeRawFrame,
  encodeSetFrame,
  FrameReader,
  HEADER_LEN,
  MAX_ITEMS,
  Op,
  Status,
} from "../src/wire";
import { must } from "./must";

const NEEDED_100 = /needed 100/;
const TRAILING_1 = /trailing 1 bytes/;
const EXCEEDS_16 = /exceeds the client limit of 16/;

const le32 = (n: number) => [
  n & 255,
  (n >>> 8) & 255,
  (n >>> 16) & 255,
  (n >>> 24) & 255,
];

describe("encode", () => {
  test("get and del frames carry the key", () => {
    expect([...encodeGetFrame("a")]).toEqual([1, ...le32(1), 0x61]);
    expect([...encodeDelFrame(new Uint8Array([0, 255]))]).toEqual([
      3,
      ...le32(2),
      0,
      255,
    ]);
    expect([...encodeGetFrame("")]).toEqual([1, ...le32(0)]);
  });

  test("set frame", () => {
    const f = encodeSetFrame("k", new Uint8Array([2, 3]));
    expect([...f]).toEqual([2, ...le32(4 + 1 + 2), ...le32(1), 0x6b, 2, 3]);
  });

  test("batch frame nests one frame per item", () => {
    const f = encodeBatchFrame([
      { op: Op.Get, key: "a" },
      { op: Op.Set, key: "k", value: new Uint8Array([9]) },
      { op: Op.Del, key: "" },
    ]);
    const get = [1, ...le32(1), 0x61];
    const set = [2, ...le32(4 + 1 + 1), ...le32(1), 0x6b, 9];
    const del = [3, ...le32(0)];
    expect([...f]).toEqual([
      6,
      ...le32(4 + get.length + set.length + del.length),
      ...le32(3),
      ...get,
      ...set,
      ...del,
    ]);
    expect([...encodeBatchFrame([])]).toEqual([6, ...le32(4), ...le32(0)]);
  });

  test("raw frame and utf-8 strings", () => {
    const f = encodeRawFrame(Op.Auth, new TextEncoder().encode("é"));
    expect([...f]).toEqual([4, ...le32(2), 0xc3, 0xa9]);
    expect(encodeGetFrame("é").length).toBe(HEADER_LEN + 2);
  });

  test("rejects too many items", () => {
    const items = new Array(MAX_ITEMS + 1).fill({ op: Op.Get, key: "" });
    expect(() => encodeBatchFrame(items)).toThrow(RangeError);
  });
});

describe("decode", () => {
  test("replies", () => {
    const body = new Uint8Array([
      ...le32(3),
      Status.Ok,
      ...le32(1),
      0x78,
      Status.NotFound,
      ...le32(0),
      Status.TooLarge,
      ...le32(2),
      0x6e,
      0x6f,
    ]);
    const r = decodeReplies(body);
    expect(r.length).toBe(3);
    expect(must(r[0]).status).toBe(Status.Ok);
    expect([...must(r[0]).body]).toEqual([0x78]);
    expect(must(r[1])).toEqual({
      status: Status.NotFound,
      body: new Uint8Array(0),
    });
    expect([...must(r[2]).body]).toEqual([0x6e, 0x6f]);
    expect(decodeReplies(new Uint8Array(le32(0)))).toEqual([]);
  });

  test("rejects malformed", () => {
    expect(() => decodeReplies(new Uint8Array([1, 0]))).toThrow(DecodeError);
    expect(() =>
      decodeReplies(new Uint8Array([...le32(1), 0, ...le32(100)])),
    ).toThrow(NEEDED_100);
    expect(() => decodeReplies(new Uint8Array([...le32(0), 9]))).toThrow(
      TRAILING_1,
    );
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
    const f = must(r.next());
    r.push(new Uint8Array([0, ...le32(1), 43]));
    r.next();
    expect([...f.body]).toEqual([42]);
  });

  test("grows past the initial buffer", () => {
    const r = new FrameReader();
    const big = new Uint8Array(1 << 20).fill(5);
    r.push(new Uint8Array([0, ...le32(big.length)]));
    expect(r.next()).toBeNull();
    for (let off = 0; off < big.length; off += 100_000)
      r.push(big.subarray(off, off + 100_000));
    const f = must(r.next());
    expect(f.body.length).toBe(big.length);
    expect(f.body[big.length - 1]).toBe(5);
  });

  test("rejects oversized frames", () => {
    const r = new FrameReader(16);
    r.push(new Uint8Array([0, ...le32(17)]));
    expect(() => r.next()).toThrow(EXCEEDS_16);
  });
});
