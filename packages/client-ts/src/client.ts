import type { Socket } from "bun";
import {
  type Bin,
  DecodeError,
  FrameReader,
  Op,
  Status,
  decodeFlags,
  decodeValues,
  encodeEntriesFrame,
  encodeKeysFrame,
  encodeRawFrame,
  toBytes,
} from "./wire";
import { type Encodable, type Value, decodeValue, encodeValue } from "./value";

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

export interface ConnectOptions {
  hostname?: string;
  port: number;
  /** Shared secret; when given, AUTH is sent before `connect` resolves. */
  token?: Bin;
}

/** One key/value pair for `set`. */
export type Entry<V = Value> = readonly [key: Bin, value: V];

/** `E` if every entry's value is `Encodable`; each entry is checked on its own. */
export type Entries<E extends readonly Entry<unknown>[]> = E & {
  readonly [I in keyof E]: readonly [Bin, Encodable<E[I] extends Entry<infer V> ? V : never>];
};

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
  private flushScheduled = false;
  private closed: Error | null = null;
  private readonly reader = new FrameReader();
  private socket!: Socket<undefined>;

  private constructor() {}

  static async connect(opts: ConnectOptions): Promise<Client> {
    const client = new Client();
    client.socket = await Bun.connect({
      hostname: opts.hostname ?? "127.0.0.1",
      port: opts.port,
      socket: {
        data: (_s, chunk) => client.onData(chunk),
        drain: () => client.flush(),
        close: (_s, err) => client.onClose(err),
        error: (_s, err) => client.onClose(err),
        connectError: (_s, err) => client.onClose(err),
        end: () => client.onClose(),
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
   * Fetch one key or many. Single key in, single value out; array in, array
   * out (one slot per key, in request order). `null` marks an absent key.
   * `T` is what the stored value decodes to; it is not checked at runtime.
   */
  get<T = Value>(key: Bin): Promise<T | null>;
  get<T = Value>(keys: readonly Bin[]): Promise<(T | null)[]>;
  async get<T = Value>(keys: Bin | readonly Bin[]): Promise<(T | null) | (T | null)[]> {
    const many = Array.isArray(keys);
    const body = await this.call(encodeKeysFrame(Op.Get, many ? keys : [keys as Bin]));
    const values = decodeValues(body).map((v) => (v === null ? null : decodeValue<T>(v)));
    return many ? values : (values[0] ?? null);
  }

  /**
   * Store one key/value pair or many. Values are msgpack-encoded; anything
   * that fails `Encodable` (functions, symbols, `undefined`, `Map`, …) is
   * rejected at compile time.
   */
  set<V>(key: Bin, value: V & Encodable<V>): Promise<void>;
  set<E extends readonly Entry<unknown>[]>(entries: Entries<E>): Promise<void>;
  async set(a: Bin | readonly Entry<unknown>[], ...rest: [unknown?]): Promise<void> {
    const entries: readonly Entry<unknown>[] =
      rest.length === 1 ? [[a as Bin, rest[0]]] : (a as readonly Entry<unknown>[]);
    await this.call(encodeEntriesFrame(entries.map(([k, v]) => [k, encodeValue(v)] as const)));
  }

  /** Delete one key or many; returns whether each one existed. */
  del(key: Bin): Promise<boolean>;
  del(keys: readonly Bin[]): Promise<boolean[]>;
  async del(keys: Bin | readonly Bin[]): Promise<boolean | boolean[]> {
    const many = Array.isArray(keys);
    const flags = decodeFlags(await this.call(encodeKeysFrame(Op.Del, many ? keys : [keys as Bin])));
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
        if (!(status in Status)) throw new DecodeError(`invalid status byte ${status}`);
        const p = this.takePending();
        if (p === undefined) throw new DecodeError("unsolicited response from server");
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
