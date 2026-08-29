import { describe, expect, test } from "bun:test";
import { decode } from "@msgpack/msgpack";
import { BIGINT_EXT, decodeValue, encodeValue } from "../src/value";

const rt = <T>(v: T): T => decodeValue<T>(encodeValue(v));

describe("value codec", () => {
  test("primitives round-trip", () => {
    expect(rt(null)).toBeNull();
    expect(rt(true)).toBe(true);
    expect(rt(0)).toBe(0);
    expect(rt(-1)).toBe(-1);
    expect(rt(1.5)).toBe(1.5);
    expect(rt(2 ** 53)).toBe(2 ** 53);
    expect(rt(NaN)).toBeNaN();
    expect(rt(Infinity)).toBe(Infinity);
    expect(rt("")).toBe("");
    expect(rt("héllo wörld ✓")).toBe("héllo wörld ✓");
    expect(rt(5n)).toBe(5n);
    expect(rt(2n ** 63n - 1n)).toBe(2n ** 63n - 1n);
    expect(rt(-(2n ** 63n))).toBe(-(2n ** 63n));
  });

  test("binary is preserved byte for byte", () => {
    const b = new Uint8Array([0, 1, 2, 255, 128]);
    const got = rt(b);
    expect(got).toBeInstanceOf(Uint8Array);
    expect([...got]).toEqual([...b]);
    const big = new Uint8Array(1 << 20).map((_, i) => i & 255);
    expect(Buffer.from(rt(big)).equals(Buffer.from(big))).toBe(true);
  });

  test("dates round-trip with millisecond precision", () => {
    const d = new Date("2026-08-29T12:34:56.789Z");
    const got = rt(d);
    expect(got).toBeInstanceOf(Date);
    expect(got.getTime()).toBe(d.getTime());
  });

  test("nested objects and arrays", () => {
    const v = {
      id: 7,
      name: "x",
      tags: ["a", "b"],
      nested: { deep: [1, [2, [3, null]]], bin: new Uint8Array([9]) },
      empty: {},
      list: [] as number[],
    };
    const got = rt(v);
    expect(got.id).toBe(7);
    expect(got.tags).toEqual(["a", "b"]);
    expect(got.nested.deep).toEqual([1, [2, [3, null]]]);
    expect([...got.nested.bin]).toEqual([9]);
    expect(got.empty).toEqual({});
    expect(got.list).toEqual([]);
    expect(Object.keys(got)).toEqual(Object.keys(v));
  });

  test("class instances become plain objects", () => {
    class P {
      constructor(
        public x: number,
        public y: number,
      ) {}
    }
    expect(rt(new P(1, 2))).toEqual({ x: 1, y: 2 });
  });

  test("bigints travel as a decimal-string extension of any size", () => {
    const huge = 2n ** 200n + 7n;
    expect(rt(huge)).toBe(huge);
    expect(rt(-huge)).toBe(-huge);
    const raw = encodeValue(5n);
    expect(raw[0]).toBe(0xd4); // fixext 1
    expect(raw[1]).toBe(BIGINT_EXT);
    expect(raw[2]).toBe(0x35); // "5"
    // Plain numbers keep msgpack's own integer formats, so a decoder
    // without the extension reads them as numbers, never as bigints.
    expect(decode(encodeValue(2 ** 40))).toBe(2 ** 40);
    expect(rt(2 ** 40)).toBe(2 ** 40);
  });

  test("emits standard msgpack", () => {
    expect([...encodeValue({ a: 1 })]).toEqual([0x81, 0xa1, 0x61, 0x01]);
    expect([...encodeValue([1, "b", null])]).toEqual([
      0x93, 0x01, 0xa1, 0x62, 0xc0,
    ]);
    expect([...encodeValue(new Uint8Array([1, 2]))]).toEqual([
      0xc4, 0x02, 0x01, 0x02,
    ]);
  });

  test("decoding garbage throws", () => {
    expect(() => decodeValue(new Uint8Array(0))).toThrow();
    expect(() => decodeValue(new Uint8Array([0xa5, 0x61]))).toThrow();
    expect(() => decodeValue(new Uint8Array([0x92, 0x01]))).toThrow();
  });
});
