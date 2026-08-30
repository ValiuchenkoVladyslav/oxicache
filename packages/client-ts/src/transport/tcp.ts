/**
 * TCP transport on `node:net` (`node:tls` with `tls`), so it runs on Node
 * and Bun alike: one socket, requests pipelined onto it. Everything issued
 * in the same tick goes out in one write and responses are matched to
 * callers in order. The server closes a
 * connection that sends nothing for its idle timeout, so after `keepaliveMs`
 * without a write the transport pings by itself.
 *
 * A lost connection — closed by the server (idle timeout, restart) or the
 * network — is reconnected lazily by the next call, which re-authenticates
 * and re-issues the calls that were in flight once (every op is
 * idempotent). Failed attempts back off from `BACKOFF_MIN_MS` to
 * `BACKOFF_MAX_MS`. A refused token or a server that does not speak the
 * protocol is permanent: the transport is dead and every call rejects with
 * that error, as does `close()`. Status and encoding errors are the call's
 * alone; the connection stays.
 */
import { isIP, connect as netConnect, type Socket } from "node:net";
import { checkServerIdentity, connect as tlsConnect } from "node:tls";
import {
  ClosedError,
  isAnswer,
  StatusError,
  type TlsOptions,
  type Transport,
} from "../transport.js";
import {
  type Bin,
  DecodeError,
  encodePingFrame,
  encodeRawFrame,
  FrameReader,
  KEEPALIVE_MS,
  Op,
  type Reply,
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
  /**
   * Speak TLS, for a server started with `OXICACHE_TLS_CERT`. `true` trusts
   * the system roots and checks the certificate against `hostname`; an
   * object names a private CA (`ca`) or another name (`serverName`). Every
   * reconnect repeats the handshake.
   */
  tls?: boolean | TlsOptions;
}

/** Wait before the second reconnect attempt in a row; the first is immediate. */
export const BACKOFF_MIN_MS = 100;
/** Every failed attempt doubles the wait, up to this. */
export const BACKOFF_MAX_MS = 5_000;

interface Pending {
  /** Kept until the reply arrives, so a lost connection can re-issue it. */
  frame: Uint8Array;
  resolve: (reply: Reply) => void;
  reject: (err: Error) => void;
  /**
   * Whether losing the connection re-issues this call on the next one; a
   * call gets that once, a heartbeat or AUTH never (nothing waits on a
   * heartbeat, and AUTH belongs to the connection it opened).
   */
  retry: boolean;
}

const utf8 = new TextDecoder();

/** A heartbeat's outcome is not anyone's business: a dead connection is reported by the next call. */
const ignore = () => {
  // nothing to do
};

/**
 * A connected socket: plain, or with the TLS handshake done and the
 * certificate checked. Rejects with the socket's error (`ECONNREFUSED`, a
 * refused certificate) if it never gets there.
 */
function open(
  hostname: string,
  port: number,
  tls: TlsOptions | null,
): Promise<Socket> {
  return new Promise((resolve, reject) => {
    let socket: Socket;
    if (tls) {
      // SNI cannot carry an IP, so an IP name is checked against the
      // certificate directly instead of being sent.
      const name = tls.serverName ?? hostname;
      // node:tls takes PEM as text or a Buffer, not a bare Uint8Array.
      const pem = (c: string | Uint8Array) =>
        typeof c === "string" ? c : Buffer.from(c);
      socket = tlsConnect(
        {
          host: hostname,
          port,
          ...(tls.ca !== undefined && {
            ca: Array.isArray(tls.ca) ? tls.ca.map(pem) : pem(tls.ca),
          }),
          ...(!isIP(name) && { servername: name }),
          checkServerIdentity: (_host, cert) => checkServerIdentity(name, cert),
        },
        () => resolve(socket),
      );
    } else {
      socket = netConnect({ host: hostname, port }, () => resolve(socket));
    }
    socket.setNoDelay(true);
    // Until it is open the socket has no link to report to.
    const failed = (err?: unknown) =>
      reject(err instanceof Error ? err : new ClosedError(err));
    socket.once("error", failed);
    socket.once("close", failed);
  });
}

/**
 * One live socket with its own in-order pending queue, write coalescing
 * and heartbeat. It reports to its owner when it is lost, handing over
 * the calls that were waiting on it.
 */
class Link {
  private readonly pending: Pending[] = [];
  private head = 0;
  private outbox: Uint8Array[] = [];
  private outboxBytes = 0;
  private flushScheduled: boolean = false;
  private lost: ClosedError | null = null;
  private readonly reader: FrameReader;
  private readonly keepaliveMs: number;
  private keepalive: ReturnType<typeof setTimeout> | undefined;
  private lastWrite = 0;
  private socket!: Socket;

  // Explicit rather than a field initialiser: Bun's coverage counts a
  // synthesised constructor as a function it never sees run.
  private constructor(
    private readonly owner: TcpTransport,
    keepaliveMs: number,
  ) {
    this.reader = new FrameReader();
    this.keepaliveMs = keepaliveMs;
  }

  /** Connect and authenticate; the link is usable when this resolves. */
  static async open(
    owner: TcpTransport,
    hostname: string,
    port: number,
    tls: TlsOptions | null,
    token: Uint8Array,
    keepaliveMs: number,
  ): Promise<Link> {
    const link = new Link(owner, keepaliveMs);
    const socket = await open(hostname, port, tls);
    link.socket = socket;
    socket.removeAllListeners("error").removeAllListeners("close");
    socket.on("data", (chunk: Uint8Array) => link.onData(chunk));
    // Any way the socket goes away ends with the same bookkeeping.
    const gone = (err?: unknown) => link.lose(err);
    socket.on("error", gone);
    socket.on("close", gone);
    socket.on("end", gone);
    try {
      await link.call(encodeRawFrame(Op.Auth, token), false);
    } catch (e) {
      link.discard();
      throw e;
    }
    return link;
  }

  /** Queue one frame; `retry` says whether a lost connection re-issues it. */
  call(frame: Uint8Array, retry: boolean): Promise<Reply> {
    return new Promise((resolve, reject) => {
      this.send({ frame, resolve, reject, retry });
    });
  }

  send(p: Pending): void {
    this.pending.push(p);
    this.outbox.push(p.frame);
    this.outboxBytes += p.frame.length;
    if (!this.flushScheduled) {
      this.flushScheduled = true;
      queueMicrotask(() => {
        this.flushScheduled = false;
        this.flush();
      });
    }
  }

  /** Hang up on purpose: the transport is closing, or this link is surplus. */
  discard(): void {
    this.socket.end();
    this.lose();
  }

  /**
   * Write what is queued, coalescing multiple frames into one write; the
   * socket buffers whatever the kernel does not take at once.
   */
  private flush(): void {
    if (this.lost || this.outbox.length === 0) return;
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
    this.socket.write(data);
    this.armKeepalive();
  }

  /**
   * (Re)start the idle clock from this write. One persistent timer checks
   * the last write when it fires, instead of a clearTimeout/setTimeout
   * pair on every flush. The timer is unref'd so a forgotten client does
   * not keep the process alive; the PING is queued like any call, so its
   * reply is matched in order and dropped. It is not retried: a heartbeat
   * must never be what reconnects.
   */
  private armKeepalive(): void {
    this.lastWrite = Date.now();
    this.keepalive ??= this.keepaliveTimer(this.keepaliveMs);
  }

  private keepaliveTimer(ms: number): ReturnType<typeof setTimeout> {
    const t = setTimeout(() => {
      const left = this.lastWrite + this.keepaliveMs - Date.now();
      if (left > 0) {
        this.keepalive = this.keepaliveTimer(left);
      } else {
        this.keepalive = undefined;
        this.call(encodePingFrame(), false).catch(ignore);
      }
    }, ms);
    t.unref();
    return t;
  }

  private onData(chunk: Uint8Array): void {
    if (this.lost) return;
    this.reader.push(chunk);
    try {
      for (let f = this.reader.next(); f !== null; f = this.reader.next()) {
        const status = f.tag;
        // Statuses are dense from Ok: anything past the last is unknown.
        if (status > Status.NotFound)
          throw new DecodeError(`invalid status byte ${status}`);
        const p = this.takePending();
        if (p === undefined)
          throw new DecodeError("unsolicited response from server");
        if (isAnswer(status)) {
          p.resolve({ status, body: f.body });
        } else {
          p.reject(new StatusError(status as Status, utf8.decode(f.body)));
        }
      }
    } catch (e) {
      // The stream is desynchronised past this point; nothing else can be
      // trusted, from this server ever: the transport dies with it. The
      // oldest caller learns why, the rest get ClosedError.
      const err = e instanceof Error ? e : new Error(String(e));
      this.takePending()?.reject(err);
      this.owner.condemn(new ClosedError(err));
      this.socket.destroy();
      this.lose(err);
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

  /** The connection is gone: stop the heartbeat and hand the waiting calls back. */
  private lose(err?: unknown): void {
    if (this.lost) return;
    this.lost = new ClosedError(err);
    clearTimeout(this.keepalive);
    this.keepalive = undefined;
    this.outbox = [];
    this.outboxBytes = 0;
    const waiting = this.pending.slice(this.head);
    this.pending.length = 0;
    this.head = 0;
    this.owner.onLost(this, this.lost, waiting);
  }
}

/**
 * The connection supervisor: the current link, or the reason there will
 * never be one again, plus the one reconnect attempt in progress that
 * every call finding the link down waits on.
 */
export class TcpTransport implements Transport {
  private link: Link | null = null;
  /** Set once, by `close()` or a fatal error; reported by every later call. */
  private dead: Error | null = null;
  private attempt: Promise<void> | null = null;
  /** Ends the backoff wait early; set only while an attempt is in it. */
  private wake: (() => void) | null = null;
  private backoffMs = BACKOFF_MIN_MS;
  private notBefore = 0;
  private reconnected = 0;

  private constructor(
    private readonly hostname: string,
    private readonly port: number,
    private readonly tls: TlsOptions | null,
    private readonly token: Uint8Array,
    private readonly keepaliveMs: number,
  ) {}

  static async connect(opts: TcpOptions): Promise<TcpTransport> {
    const keepaliveMs = opts.keepaliveMs ?? KEEPALIVE_MS;
    // setTimeout clamps anything above 2^31-1 to 1 ms: a ping storm.
    if (!(keepaliveMs > 0 && keepaliveMs <= 2147483647)) {
      throw new RangeError(
        `keepaliveMs must be in 1..2147483647, got ${keepaliveMs}`,
      );
    }
    const t = new TcpTransport(
      opts.hostname ?? "127.0.0.1",
      opts.port,
      opts.tls === true ? {} : opts.tls === false ? null : (opts.tls ?? null),
      toBytes(opts.token),
      keepaliveMs,
    );
    t.link = await t.open();
    return t;
  }

  /** Whether a connection is up right now; false while one is being re-established. */
  get isOpen(): boolean {
    return this.link !== null;
  }

  /** How many times the connection has been re-established. */
  get reconnects(): number {
    return this.reconnected;
  }

  /** Close for good; every in-flight and later call rejects with ClosedError. */
  close(): void {
    if (this.dead) return;
    this.dead = new ClosedError();
    this.link?.discard();
    this.wake?.();
  }

  async ping(): Promise<void> {
    await this.request(encodePingFrame());
  }

  request(frame: Uint8Array): Promise<Reply> {
    if (this.dead) return Promise.reject(this.dead);
    return new Promise((resolve, reject) => {
      this.enqueue({ frame, resolve, reject, retry: true });
    });
  }

  /** Remember a fatal error; the first one stands. */
  condemn(err: Error): void {
    this.dead ??= err;
  }

  /**
   * A link is gone. The calls it was carrying are re-issued if they still
   * may be — on the next link, once it exists — and rejected otherwise.
   */
  onLost(link: Link, err: ClosedError, waiting: Pending[]): void {
    if (link === this.link) this.link = null;
    for (const p of waiting) {
      if (p.retry) {
        p.retry = false;
        this.enqueue(p);
      } else {
        p.reject(err);
      }
    }
  }

  /** The hot path is the `send`; everything else is a connection down. */
  private enqueue(p: Pending): void {
    if (this.dead) p.reject(this.dead);
    else if (this.link) this.link.send(p);
    else
      this.recover().then(
        () => this.enqueue(p),
        (e: Error) => p.reject(e),
      );
  }

  /** The reconnect in progress, started if there is none. */
  private recover(): Promise<void> {
    this.attempt ??= this.reconnect().finally(() => {
      this.attempt = null;
    });
    return this.attempt;
  }

  private async reconnect(): Promise<void> {
    const wait = this.notBefore - Date.now();
    if (wait > 0) await this.backOff(wait);
    if (this.dead) throw this.dead;
    let link: Link;
    try {
      link = await this.open();
    } catch (e) {
      // A refused token will be refused again; anything else is worth
      // another try, later.
      if (e instanceof StatusError && e.status === Status.Unauthorized) {
        this.dead = e;
      } else {
        this.notBefore = Date.now() + this.backoffMs;
        this.backoffMs = Math.min(this.backoffMs * 2, BACKOFF_MAX_MS);
      }
      throw e;
    }
    if (this.dead) {
      // Closed while connecting: nobody wants this link.
      link.discard();
      throw this.dead;
    }
    this.link = link;
    this.backoffMs = BACKOFF_MIN_MS;
    this.reconnected++;
  }

  /** Sleep `ms`, or less if `close()` comes first: nobody waits on a closed transport. */
  private backOff(ms: number): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.wake = null;
        resolve();
      }, ms);
      this.wake = () => {
        clearTimeout(timer);
        this.wake = null;
        resolve();
      };
    });
  }

  private open(): Promise<Link> {
    return Link.open(
      this,
      this.hostname,
      this.port,
      this.tls,
      this.token,
      this.keepaliveMs,
    );
  }
}

/** Open a connection to the server's TCP listener; resolves once it is usable (and authenticated, with a token). */
export function tcp(opts: TcpOptions): Promise<TcpTransport> {
  return TcpTransport.connect(opts);
}
