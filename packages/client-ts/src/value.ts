import { Packr } from "msgpackr";

/** Leaf types a value may contain. */
export type Primitive = null | boolean | number | bigint | string | Uint8Array | Date;

/** Anything the client can store: msgpack-representable data. */
export type Value = Primitive | readonly Value[] | { readonly [key: string]: Value };

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

// Plain msgpack only: no msgpackr record extension, so the bytes are readable
// by any decoder and object key order round-trips as written.
const packr = new Packr({ useRecords: false });

export function encodeValue(value: unknown): Uint8Array {
  return packr.pack(value);
}

export function decodeValue<T>(bytes: Uint8Array): T {
  return packr.unpack(bytes) as T;
}
