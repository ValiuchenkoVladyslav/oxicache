import { Packr } from "msgpackr";

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

export interface CodecOptions {
  /**
   * msgpackr's record extension: within a value, an object's key set is
   * written once as a structure definition and further objects of the same
   * shape refer to it, which is smaller and faster for repeated shapes. Data
   * written this way is msgpack with an extension type that only msgpackr
   * reads back; turn it off for plain MessagePack readable by any decoder.
   * Default `true`.
   */
  useRecords?: boolean;
}

/** Encodes and decodes values; one per client so record structures stay private to it. */
export class Codec {
  private readonly packr: Packr;

  constructor(opts: CodecOptions = {}) {
    // mapsAsObjects is explicit because msgpackr flips its default to Map
    // when records are on, and plain msgpack maps must still come back as
    // objects whichever mode wrote them.
    this.packr = new Packr({
      useRecords: opts.useRecords ?? true,
      mapsAsObjects: true,
    });
  }

  encode(value: unknown): Uint8Array {
    return this.packr.pack(value);
  }

  decode<T>(bytes: Uint8Array): T {
    return this.packr.unpack(bytes) as T;
  }
}
