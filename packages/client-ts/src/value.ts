import { decode, ExtensionCodec, encode } from "@msgpack/msgpack";

/** Leaf types a value may contain. */
export type Primitive =
  | null
  | boolean
  | number
  | bigint
  | string
  | Uint8Array
  | Date;

/** Anything the client can store: msgpack-representable data. */
export type Value =
  | Primitive
  | readonly Value[]
  | { readonly [key: string]: Value };

/**
 * `T` if every part of it is msgpack-representable, otherwise a type nothing
 * is assignable to. Unlike a plain `Value` constraint this accepts interfaces
 * and class instances, which lack the implicit index signature `Value` needs.
 */
export type Encodable<T> = T extends Primitive
  ? T
  : T extends readonly (infer U)[]
    ? readonly Encodable<U>[]
    : T extends (...args: never[]) => unknown
      ? never
      : T extends object
        ? { readonly [K in keyof T]: Encodable<T[K]> }
        : never;

/**
 * Extension type carrying a `bigint` as its decimal string, so integers of
 * any size survive. (msgpack's own int64 formats are not used for bigints:
 * decoding them as bigint would also turn every plain number above 2^32
 * into one.)
 */
export const BIGINT_EXT = 0;

const utf8 = new TextEncoder();
const utf8d = new TextDecoder();

const codec = new ExtensionCodec();
codec.register({
  type: BIGINT_EXT,
  encode: (v: unknown) =>
    typeof v === "bigint" ? utf8.encode(v.toString()) : null,
  decode: (data: Uint8Array) => BigInt(utf8d.decode(data)),
});

const options = { extensionCodec: codec } as const;

/** Encode a value as MessagePack (plain, readable by any decoder; `bigint` via {@link BIGINT_EXT}). */
export function encodeValue(value: unknown): Uint8Array {
  return encode(value, options);
}

/** Decode MessagePack produced by {@link encodeValue} (or any other encoder). */
export function decodeValue<T>(bytes: Uint8Array): T {
  return decode(bytes, options) as T;
}
