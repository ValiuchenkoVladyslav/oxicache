import type { Transport } from "./transport.js";
import {
  decodeValue,
  type Encodable,
  encodeValue,
  type Value,
} from "./value.js";
import {
  type Bin,
  decodeFlags,
  decodeValues,
  encodeEntriesFrame,
  encodeKeysFrame,
  Op,
} from "./wire.js";

/** One key/value pair for `set`. */
export type Entry<V = Value> = readonly [key: Bin, value: V];

/** `E` if every entry's value is `Encodable`; each entry is checked on its own. */
export type Entries<E extends readonly Entry<unknown>[]> = E & {
  readonly [I in keyof E]: readonly [
    Bin,
    Encodable<E[I] extends Entry<infer V> ? V : never>,
  ];
};

/** A tuple of `N` copies of `R`. */
export type Fill<
  N extends number,
  R,
  A extends R[] = [],
> = A["length"] extends N ? A : Fill<N, R, [...A, R]>;

/** The type argument of an `N`-key `get`: a tuple of exactly `N` value types, one per key. */
export type Types<N extends number> = Fill<N, unknown>;

/** Result tuple of an `N`-key `get`: each key's type, or `null` when absent. */
export type Results<T extends readonly unknown[]> = {
  -readonly [I in keyof T]: T[I] | null;
};

/** Normalise `(key)`, `(k1, k2, …)` and `(keys[])` into a key list plus whether the result is a list. */
function keyArgs(args: Bin[] | [readonly Bin[]]): [readonly Bin[], boolean] {
  const first = args[0];
  if (args.length === 1 && Array.isArray(first)) return [first, true];
  return [args as Bin[], args.length !== 1];
}

/**
 * A cache client over a {@link Transport}. Calls are independent of each
 * other, so any number of tasks may share one client; what that means on
 * the wire (pipelining, one request per exchange, …) is up to the transport.
 */
export class Client {
  private constructor(private readonly transport: Transport) {}

  /**
   * Wrap a transport, e.g. `Client.connect(tcp({ port: 4433 }))` or
   * `Client.connect(http({ url: "http://cache:4434" }))`.
   */
  static async connect(
    transport: Transport | Promise<Transport>,
  ): Promise<Client> {
    return new Client(await transport);
  }

  /**
   * Fetch one key or many. One key in, one value out; several keys in, a
   * tuple of values out, one per key in argument order (up to 16 literal
   * keys); an array in, an array out, for lists whose length is only known
   * at runtime. `null` marks an absent key.
   *
   * `T` is what the stored values decode to and is not checked at runtime.
   * With several keys it is a tuple with exactly one type per key:
   * `get<[User, number]>(a, b)`.
   */
  get<T = Value>(key: Bin): Promise<T | null>;
  get<T extends Types<2> = Fill<2, Value>>(
    k1: Bin,
    k2: Bin,
  ): Promise<Results<T>>;
  get<T extends Types<3> = Fill<3, Value>>(
    k1: Bin,
    k2: Bin,
    k3: Bin,
  ): Promise<Results<T>>;
  get<T extends Types<4> = Fill<4, Value>>(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
  ): Promise<Results<T>>;
  get<T extends Types<5> = Fill<5, Value>>(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
  ): Promise<Results<T>>;
  get<T extends Types<6> = Fill<6, Value>>(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
  ): Promise<Results<T>>;
  get<T extends Types<7> = Fill<7, Value>>(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
  ): Promise<Results<T>>;
  get<T extends Types<8> = Fill<8, Value>>(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
  ): Promise<Results<T>>;
  get<T extends Types<9> = Fill<9, Value>>(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
  ): Promise<Results<T>>;
  get<T extends Types<10> = Fill<10, Value>>(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
    k10: Bin,
  ): Promise<Results<T>>;
  get<T extends Types<11> = Fill<11, Value>>(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
    k10: Bin,
    k11: Bin,
  ): Promise<Results<T>>;
  get<T extends Types<12> = Fill<12, Value>>(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
    k10: Bin,
    k11: Bin,
    k12: Bin,
  ): Promise<Results<T>>;
  get<T extends Types<13> = Fill<13, Value>>(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
    k10: Bin,
    k11: Bin,
    k12: Bin,
    k13: Bin,
  ): Promise<Results<T>>;
  get<T extends Types<14> = Fill<14, Value>>(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
    k10: Bin,
    k11: Bin,
    k12: Bin,
    k13: Bin,
    k14: Bin,
  ): Promise<Results<T>>;
  get<T extends Types<15> = Fill<15, Value>>(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
    k10: Bin,
    k11: Bin,
    k12: Bin,
    k13: Bin,
    k14: Bin,
    k15: Bin,
  ): Promise<Results<T>>;
  get<T extends Types<16> = Fill<16, Value>>(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
    k10: Bin,
    k11: Bin,
    k12: Bin,
    k13: Bin,
    k14: Bin,
    k15: Bin,
    k16: Bin,
  ): Promise<Results<T>>;
  get<T = Value>(keys: readonly Bin[]): Promise<(T | null)[]>;
  async get<T = Value>(
    ...args: Bin[] | [readonly Bin[]]
  ): Promise<(T | null) | (T | null)[]> {
    const [keys, many] = keyArgs(args);
    const body = await this.transport.request(encodeKeysFrame(Op.Get, keys));
    const values = decodeValues(body).map((v) =>
      v === null ? null : decodeValue<T>(v),
    );
    return many ? values : (values[0] ?? null);
  }

  /**
   * Store one key/value pair, several, or an array of them. Values are
   * msgpack-encoded; anything that fails `Encodable` (functions, symbols,
   * `undefined`, `Map`, …) is rejected at compile time, entry by entry.
   */
  set<V>(key: Bin, value: V & Encodable<V>): Promise<void>;
  set<E extends readonly Entry<unknown>[]>(
    ...entries: Entries<E>
  ): Promise<void>;
  set<E extends readonly Entry<unknown>[]>(entries: Entries<E>): Promise<void>;
  async set(
    ...args: [Bin, unknown] | Entry<unknown>[] | [readonly Entry<unknown>[]]
  ): Promise<void> {
    let entries: readonly Entry<unknown>[];
    const first = args[0];
    if (!Array.isArray(first)) {
      entries = [[first as Bin, args[1]]]; // set(key, value)
    } else if (first.length === 0 || Array.isArray(first[0])) {
      entries = first as readonly Entry<unknown>[]; // set(entries)
    } else {
      entries = args as Entry<unknown>[]; // set([k, v], [k2, v2], ...)
    }
    await this.transport.request(
      encodeEntriesFrame(entries.map(([k, v]) => [k, encodeValue(v)] as const)),
    );
  }

  /** Delete one key or many; returns whether each one existed. Same shapes as `get`. */
  del(key: Bin): Promise<boolean>;
  del(k1: Bin, k2: Bin): Promise<Fill<2, boolean>>;
  del(k1: Bin, k2: Bin, k3: Bin): Promise<Fill<3, boolean>>;
  del(k1: Bin, k2: Bin, k3: Bin, k4: Bin): Promise<Fill<4, boolean>>;
  del(k1: Bin, k2: Bin, k3: Bin, k4: Bin, k5: Bin): Promise<Fill<5, boolean>>;
  del(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
  ): Promise<Fill<6, boolean>>;
  del(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
  ): Promise<Fill<7, boolean>>;
  del(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
  ): Promise<Fill<8, boolean>>;
  del(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
  ): Promise<Fill<9, boolean>>;
  del(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
    k10: Bin,
  ): Promise<Fill<10, boolean>>;
  del(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
    k10: Bin,
    k11: Bin,
  ): Promise<Fill<11, boolean>>;
  del(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
    k10: Bin,
    k11: Bin,
    k12: Bin,
  ): Promise<Fill<12, boolean>>;
  del(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
    k10: Bin,
    k11: Bin,
    k12: Bin,
    k13: Bin,
  ): Promise<Fill<13, boolean>>;
  del(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
    k10: Bin,
    k11: Bin,
    k12: Bin,
    k13: Bin,
    k14: Bin,
  ): Promise<Fill<14, boolean>>;
  del(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
    k10: Bin,
    k11: Bin,
    k12: Bin,
    k13: Bin,
    k14: Bin,
    k15: Bin,
  ): Promise<Fill<15, boolean>>;
  del(
    k1: Bin,
    k2: Bin,
    k3: Bin,
    k4: Bin,
    k5: Bin,
    k6: Bin,
    k7: Bin,
    k8: Bin,
    k9: Bin,
    k10: Bin,
    k11: Bin,
    k12: Bin,
    k13: Bin,
    k14: Bin,
    k15: Bin,
    k16: Bin,
  ): Promise<Fill<16, boolean>>;
  del(keys: readonly Bin[]): Promise<boolean[]>;
  async del(...args: Bin[] | [readonly Bin[]]): Promise<boolean | boolean[]> {
    const [keys, many] = keyArgs(args);
    const flags = decodeFlags(
      await this.transport.request(encodeKeysFrame(Op.Del, keys)),
    );
    return many ? flags : (flags[0] ?? false);
  }

  /** Round-trip an empty request: resolves once the server has answered. */
  ping(): Promise<void> {
    return this.transport.ping();
  }

  /** Whether the transport is still usable. */
  get isOpen(): boolean {
    return this.transport.isOpen;
  }

  /** Close the transport; every in-flight call rejects with ClosedError. */
  close(): void {
    this.transport.close();
  }
}
