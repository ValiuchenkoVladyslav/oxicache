/**
 * The typed API over a {@link Transport}: one key per call, or a
 * {@link Client.batch} of any number of GET/SET/DEL requests answered in
 * one exchange. Values are MessagePack ({@link encodeValue}); keys are
 * strings (UTF-8) or bytes.
 */
import { isAnswer, StatusError, type Transport } from "./transport.js";
import {
  decodeValue,
  type Encodable,
  encodeValue,
  type Value,
} from "./value.js";
import {
  type BatchItem,
  type Bin,
  DecodeError,
  encodeBatchFrame,
  encodeDelFrame,
  encodeGetFrame,
  encodePingFrame,
  encodeSetFrame,
  Op,
  Reader,
  Status,
} from "./wire.js";

/** A GET in a batch, as the transport sees it. */
export interface AnyGetOp {
  readonly op: Op.Get;
  readonly key: Bin;
}

/** A GET in a batch; `T` is what the value decodes to (not checked at runtime). */
export interface GetOp<T = Value> extends AnyGetOp {
  /** Type-level only: carries `T` so the result tuple can name it. */
  readonly __type?: T;
}

/** A SET in a batch; the value is encoded when the op is built. */
export interface SetOp {
  readonly op: Op.Set;
  readonly key: Bin;
  readonly value: Uint8Array;
}

/** A DEL in a batch. */
export interface DelOp {
  readonly op: Op.Del;
  readonly key: Bin;
}

/**
 * Any op a batch takes. `AnyGetOp` rather than `GetOp<unknown>` so that
 * the batch's element type does not steer `op.get`'s inference away from
 * its default.
 */
export type BatchOp = AnyGetOp | SetOp | DelOp;

/** What one batch op resolves to: get → value or null, set → undefined, del → whether the key existed. */
export type BatchResult<O> =
  O extends GetOp<infer T>
    ? T | null
    : O extends SetOp
      ? undefined
      : O extends DelOp
        ? boolean
        : never;

/** The results of a batch, positionally typed after its ops. */
export type BatchResults<O extends readonly BatchOp[]> = {
  -readonly [I in keyof O]: BatchResult<O[I]>;
};

/**
 * Builders for {@link Client.batch}:
 * `c.batch([op.get<User>("u:1"), op.set("seen", 1), op.del("tmp")])`.
 * `op.set` encodes its value at once, so a value that cannot be encoded
 * throws here rather than inside the batch.
 */
export const op = {
  get<T = Value>(key: Bin): GetOp<T> {
    return { op: Op.Get, key };
  },
  set<V>(key: Bin, value: V & Encodable<V>): SetOp {
    return { op: Op.Set, key, value: encodeValue(value) };
  },
  del(key: Bin): DelOp {
    return { op: Op.Del, key };
  },
} as const;

const utf8 = new TextDecoder();

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
   * The value stored under `key`, or `null` if there is none. `T` is what
   * the value decodes to and is not checked at runtime.
   */
  async get<T = Value>(key: Bin): Promise<T | null> {
    const r = await this.transport.request(encodeGetFrame(key));
    return r.status === Status.NotFound ? null : decodeValue<T>(r.body);
  }

  /** Store `value` under `key`, replacing whatever was there. */
  async set<V>(key: Bin, value: V & Encodable<V>): Promise<void> {
    await this.transport.request(encodeSetFrame(key, encodeValue(value)));
  }

  /** Remove `key`; whether it existed. */
  async del(key: Bin): Promise<boolean> {
    const r = await this.transport.request(encodeDelFrame(key));
    return r.status === Status.Ok;
  }

  /**
   * Send `ops` in one request and get one result per op, in order and
   * typed after it (see {@link op}). Every op is answered on its own: a
   * missing key is `null`/`false`, not an error; but an op the server
   * refuses (a value too large, say) rejects the whole call with a
   * `StatusError` naming its index. More than `MAX_ITEMS` ops throws a
   * `RangeError` before anything is sent.
   */
  async batch<const O extends readonly BatchOp[]>(
    ops: O,
  ): Promise<BatchResults<O>> {
    const frame = encodeBatchFrame(ops as readonly BatchItem[]);
    // Replies are turned into results in one pass over the response body,
    // with no intermediate reply objects.
    const r = new Reader((await this.transport.request(frame)).body);
    const n = r.u32();
    if (n !== ops.length) {
      throw new DecodeError(
        `server answered ${n} replies for ${ops.length} ops`,
      );
    }
    const out = new Array<unknown>(n);
    for (let i = 0; i < n; i++) {
      // biome-ignore lint/style/noNonNullAssertion: i < n === ops.length
      out[i] = result(ops[i]!, r.u8(), r.blob(), i);
    }
    r.finish();
    return out as BatchResults<O>;
  }

  /** Round-trip an empty request; resolves once the server has answered. */
  async ping(): Promise<void> {
    await this.transport.request(encodePingFrame());
  }

  /** Whether the transport can still be used. */
  get isOpen(): boolean {
    return this.transport.isOpen;
  }

  /** Release the transport; every in-flight and later call rejects with `ClosedError`. */
  close(): void {
    this.transport.close();
  }
}

/** One batch reply as its op's result. */
function result(
  o: BatchOp,
  status: number,
  body: Uint8Array,
  i: number,
): unknown {
  if (!isAnswer(status) || (o.op === Op.Set && status !== Status.Ok)) {
    throw new StatusError(status as Status, `item ${i}: ${utf8.decode(body)}`);
  }
  switch (o.op) {
    case Op.Get:
      return status === Status.NotFound ? null : decodeValue(body);
    case Op.Set:
      return undefined;
    case Op.Del:
      return status === Status.Ok;
  }
}
