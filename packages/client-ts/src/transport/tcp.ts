/**
 * TCP transport for the Bun runtime: one `Bun.connect` socket, requests
 * pipelined onto it. Everything issued in the same tick goes out in one
 * write and responses are matched to callers in order. The server closes a
 * connection that sends nothing for its idle timeout, so after `keepaliveMs`
 * without a write the transport pings by itself.
 */
import type { Socket } from "bun";
import { ClosedError, StatusError, type Transport } from "../transport.js";
import {
  type Bin,
  DecodeError,
  encodePingFrame,
  encodeRawFrame,
  FrameReader,
  KEEPALIVE_MS,
  Op,
  Status,
  toBytes,
} from "../wire.js";

export interface TcpOptions {
  hostname?: string;
  port: number;
  /** Shared secret; AUTH is sent before `tcp` resolves and a refusal rejects it. */
  token: Bin;
  /**
   * Send a PING after this long without a write, so the server's idle
   * timeout (300 s by default) never closes a quiet connection. Must be
   * positive; defaults to `KEEPALIVE_MS` (100 s, a third of that timeout).
   */
  keepaliveMs?: number;
}

interface Pending {
  resolve: (body: Uint8Array) => void;
  reject: (err: Error) => void;
}

const utf8 = new TextDecoder();

/** A heartbeat's outcome is not anyone's business: a dead connection is reported by the next call. */
const ignore = () => {
  // nothing to do
};

class TcpTransport implements Transport {
  private readonly pending: Pending[] = [];
  private head = 0;
  private outbox: Uint8Array[] = [];
  private outboxBytes = 0;
  private unsent: Uint8Array | null = null;
  private flushScheduled: boolean = false;
  private closed: Error | null = null;
  private readonly reader: FrameReader;
  private readonly keepaliveMs: number;
  private keepalive: ReturnType<typeof setTimeout> | undefined;
  private socket!: Socket<undefined>;

  // Explicit rather than a field initialiser: Bun's coverage counts a
  // synthesised constructor as a function it never sees run.
  private constructor(keepaliveMs: number) {
    this.reader = new FrameReader();
    this.keepaliveMs = keepaliveMs;
  }

  static async connect(opts: TcpOptions): Promise<TcpTransport> {
    const keepaliveMs = opts.keepaliveMs ?? KEEPALIVE_MS;
    // setTimeout clamps anything above 2^31-1 to 1 ms: a ping storm.
    if (!(keepaliveMs > 0 && keepaliveMs <= 2147483647)) {
      throw new RangeError(
        `keepaliveMs must be in 1..2147483647, got ${keepaliveMs}`,
      );
    }
    const t = new TcpTransport(keepaliveMs);
    // Any way the socket goes away ends with the same bookkeeping.
    const gone = (_s: unknown, err?: unknown) => t.onClose(err);
    t.socket = await Bun.connect({
      hostname: opts.hostname ?? "127.0.0.1",
      port: opts.port,
      socket: {
        data: (_s, chunk) => t.onData(chunk),
        drain: () => t.flush(),
        close: gone,
        error: gone,
        connectError: gone,
        end: gone,
      },
    });
    if (t.closed) throw t.closed;
    try {
      await t.request(encodeRawFrame(Op.Auth, toBytes(opts.token)));
    } catch (e) {
      t.close();
      throw e;
    }
    return t;
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

  async ping(): Promise<void> {
    await this.request(encodePingFrame());
  }

  request(frame: Uint8Array): Promise<Uint8Array> {
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
    this.armKeepalive();
  }

  /**
   * (Re)start the idle clock from this write. The timer is unref'd so a
   * forgotten client does not keep the process alive; the PING goes through
   * `request` like any call, so its reply is matched in order and dropped.
   */
  private armKeepalive(): void {
    clearTimeout(this.keepalive);
    this.keepalive = setTimeout(() => {
      this.ping().catch(ignore);
    }, this.keepaliveMs);
    this.keepalive.unref();
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
    clearTimeout(this.keepalive);
    this.outbox = [];
    this.outboxBytes = 0;
    this.unsent = null;
    for (let p = this.takePending(); p !== undefined; p = this.takePending()) {
      p.reject(this.closed);
    }
  }
}

/** Open a connection to the server's TCP listener; resolves once it is usable (and authenticated, with a token). */
export function tcp(opts: TcpOptions): Promise<Transport> {
  return TcpTransport.connect(opts);
}
