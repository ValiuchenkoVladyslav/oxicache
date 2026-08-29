import type { Socket } from "bun";
import { Codec, type CodecOptions, type Encodable, type Value } from "./value";
import {
  type Bin,
  DecodeError,
  decodeFlags,
  decodeValues,
  encodeEntriesFrame,
  encodeKeysFrame,
  encodeRawFrame,
  FrameReader,
  Op,
  Status,
  toBytes,
} from "./wire";

/** The server answered with a non-OK status. */
export class StatusError extends Error {
  override name = "StatusError";
  constructor(
    readonly status: Status,
    message: string,
  ) {
    super(`server returned ${Status[status] ?? status}: ${message}`);
  }
}

/** The connection is closed (or was closed before a reply arrived). */
export class ClosedError extends Error {
  override name = "ClosedError";
  constructor(cause?: unknown) {
    super("connection closed", cause === undefined ? undefined : { cause });
  }
}

export interface ConnectOptions extends CodecOptions {
  hostname?: string;
  port: number;
  /** Shared secret; when given, AUTH is sent before `connect` resolves. */
  token?: Bin;
}

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

interface Pending {
  resolve: (body: Uint8Array) => void;
  reject: (err: Error) => void;
}

const utf8 = new TextDecoder();

/**
 * One TCP connection to an oxicache server. Calls from anywhere are
 * pipelined onto it: everything issued in the same tick goes out in one
 * write, and responses are matched to callers in order.
 */
export class Client {
  private readonly pending: Pending[] = [];
  private head = 0;
  private outbox: Uint8Array[] = [];
  private outboxBytes = 0;
  private unsent: Uint8Array | null = null;
  private flushScheduled: boolean = false;
  private closed: Error | null = null;
  private readonly reader = new FrameReader();
  private readonly codec: Codec;
  private socket!: Socket<undefined>;

  private constructor(opts: CodecOptions) {
    this.codec = new Codec(opts);
  }

  static async connect(opts: ConnectOptions): Promise<Client> {
    const client = new Client(opts);
    // Any way the socket goes away ends with the same bookkeeping.
    const gone = (_s: unknown, err?: unknown) => client.onClose(err);
    client.socket = await Bun.connect({
      hostname: opts.hostname ?? "127.0.0.1",
      port: opts.port,
      socket: {
        data: (_s, chunk) => client.onData(chunk),
        drain: () => client.flush(),
        close: gone,
        error: gone,
        connectError: gone,
        end: gone,
      },
    });
    if (client.closed) throw client.closed;
    if (opts.token !== undefined) {
      try {
        await client.auth(opts.token);
      } catch (e) {
        client.close();
        throw e;
      }
    }
    return client;
  }

  /** Present the server's shared secret. A no-op if the server has none. */
  async auth(token: Bin): Promise<void> {
    await this.call(encodeRawFrame(Op.Auth, toBytes(token)));
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
    const body = await this.call(encodeKeysFrame(Op.Get, keys));
    const values = decodeValues(body).map((v) =>
      v === null ? null : this.codec.decode<T>(v),
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
    await this.call(
      encodeEntriesFrame(
        entries.map(([k, v]) => [k, this.codec.encode(v)] as const),
      ),
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
    const flags = decodeFlags(await this.call(encodeKeysFrame(Op.Del, keys)));
    return many ? flags : (flags[0] ?? false);
  }

  /** Whether the connection is still usable. */
  get isOpen(): boolean {
    return this.closed === null;
  }

  /** Close the connection; every in-flight call rejects with ClosedError. */
  close(): void {
    if (this.closed) return;
    this.socket.end();
    this.onClose();
  }

  private call(frame: Uint8Array): Promise<Uint8Array> {
    if (this.closed) return Promise.reject(this.closed);
    return new Promise((resolve, reject) => {
      this.pending.push({ resolve, reject });
      this.outbox.push(frame);
      this.outboxBytes += frame.length;
      if (!this.flushScheduled) {
        this.flushScheduled = true;
        queueMicrotask(() => {
          this.flushScheduled = false;
          this.flush();
        });
      }
    });
  }

  /** Write what is queued, coalescing multiple frames into one syscall. */
  private flush(): void {
    if (this.closed) return;
    if (this.unsent !== null) {
      const n = this.socket.write(this.unsent);
      if (n < this.unsent.length) {
        this.unsent = this.unsent.subarray(Math.max(n, 0));
        return; // `drain` will call us again
      }
      this.unsent = null;
    }
    if (this.outbox.length === 0) return;
    let data: Uint8Array;
    if (this.outbox.length === 1) {
      // biome-ignore lint/style/noNonNullAssertion: length === 1 was just checked
      data = this.outbox[0]!;
    } else {
      data = new Uint8Array(this.outboxBytes);
      let pos = 0;
      for (const f of this.outbox) {
        data.set(f, pos);
        pos += f.length;
      }
    }
    this.outbox = [];
    this.outboxBytes = 0;
    const n = this.socket.write(data);
    if (n < data.length) this.unsent = data.subarray(Math.max(n, 0));
  }

  private onData(chunk: Uint8Array): void {
    if (this.closed) return;
    this.reader.push(chunk);
    try {
      for (let f = this.reader.next(); f !== null; f = this.reader.next()) {
        const status = f.tag;
        if (!(status in Status))
          throw new DecodeError(`invalid status byte ${status}`);
        const p = this.takePending();
        if (p === undefined)
          throw new DecodeError("unsolicited response from server");
        if (status === Status.Ok) {
          p.resolve(f.body);
        } else {
          p.reject(new StatusError(status as Status, utf8.decode(f.body)));
        }
      }
    } catch (e) {
      // The stream is desynchronised past this point; nothing else can be
      // trusted. The oldest caller learns why, the rest get ClosedError.
      const err = e instanceof Error ? e : new Error(String(e));
      this.takePending()?.reject(err);
      this.socket.terminate();
      this.onClose(err);
    }
  }

  private takePending(): Pending | undefined {
    if (this.head === this.pending.length) return undefined;
    const p = this.pending[this.head++];
    if (this.head === this.pending.length) {
      this.pending.length = 0;
      this.head = 0;
    } else if (this.head > 1024 && this.head * 2 > this.pending.length) {
      this.pending.splice(0, this.head);
      this.head = 0;
    }
    return p;
  }

  private onClose(err?: unknown): void {
    if (this.closed) return;
    this.closed = err instanceof ClosedError ? err : new ClosedError(err);
    this.outbox = [];
    this.outboxBytes = 0;
    this.unsent = null;
    for (let p = this.takePending(); p !== undefined; p = this.takePending()) {
      p.reject(this.closed);
    }
  }
}
