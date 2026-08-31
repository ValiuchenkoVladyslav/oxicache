/**
 * HTTP transport: one `fetch` per request, the frame body as the request
 * body, the op as the path. Runs anywhere `fetch` does (Bun, Node 18+, edge
 * runtimes, lambdas); nothing here touches a socket API.
 */
import {
  ClosedError,
  StatusError,
  type TlsOptions,
  type Transport,
} from "../transport.js";
import {
  encodePingFrame,
  HEADER_LEN,
  Op,
  type Reply,
  Status,
} from "../wire.js";

export interface HttpOptions {
  /** Base URL of the server's HTTP listener, e.g. `http://127.0.0.1:4434`. */
  url: string | URL;
  /** Shared secret, sent as `Authorization: Bearer <token>` on every request. */
  token: string;
  /** `fetch` to use instead of the global one (custom agents, tests). */
  fetch?: Fetch;
  /**
   * TLS settings for an `https://` URL, passed to `fetch` as its `tls`
   * option. Bun honours it (`{ ca }` with the PEM of a private CA,
   * `serverName`); other runtimes ignore it, so there trust a private CA
   * through the runtime (`NODE_EXTRA_CA_CERTS`) or a custom `fetch`
   * instead. A publicly trusted certificate needs nothing here.
   */
  tls?: TlsOptions;
}

/**
 * The part of `fetch` the transport uses. The transport reuses one `init`
 * object across calls (only `body` changes), so a custom `fetch` must read
 * `init` before its first `await`, as real `fetch` does.
 */
export type Fetch = (
  url: string,
  init: RequestInit & { tls?: TlsOptions },
) => Promise<Response>;

const PATH: Readonly<Record<Op, string>> = {
  [Op.Get]: "/get",
  [Op.Set]: "/set",
  [Op.Del]: "/del",
  [Op.Auth]: "",
  [Op.Ping]: "/ping",
  [Op.Batch]: "/batch",
};

/** HTTP status codes the server uses for each frame status. */
const STATUS: Readonly<Record<number, Status>> = {
  400: Status.BadRequest,
  401: Status.Unauthorized,
  404: Status.UnknownOp,
  405: Status.UnknownOp,
  413: Status.TooLarge,
};

const utf8 = new TextDecoder();
const TRAILING_SLASHES = /\/+$/;

class HttpTransport implements Transport {
  private readonly base: string;
  private readonly fetch: Fetch;
  /** Built once and reused; `request` only swaps the body in. */
  private readonly init: RequestInit & { tls?: TlsOptions };
  private closed: ClosedError | null = null;

  constructor(opts: HttpOptions) {
    this.base = String(opts.url).replace(TRAILING_SLASHES, "");
    this.fetch = opts.fetch ?? globalThis.fetch;
    // Only what the server reads; no content type — neither side looks
    // at one, and the body is the raw frame body either way.
    this.init = {
      method: "POST",
      headers: { authorization: `Bearer ${opts.token}` },
      ...(opts.tls && { tls: opts.tls }),
    };
  }

  async request(frame: Uint8Array): Promise<Reply> {
    if (this.closed) throw this.closed;
    // The op byte picks the path. The token travels as a header on every
    // request instead of an AUTH frame, so sending one is a caller bug.
    const op = frame[0] as Op;
    if (op === Op.Auth) {
      throw new Error(
        "AUTH is not a request over HTTP: the token rides as the Authorization header",
      );
    }
    // `fetch` consumes `init` before it returns (the Request is built
    // synchronously), so one reused object is safe across concurrent
    // calls. The view is over a plain ArrayBuffer; the cast narrows the
    // generic.
    this.init.body = frame.subarray(HEADER_LEN) as Uint8Array<ArrayBuffer>;
    const res = await this.fetch(this.base + PATH[op], this.init);
    const body = new Uint8Array(await res.arrayBuffer());
    if (res.ok) return { status: Status.Ok, body };
    // The path is known, so an empty 404 is the key's absence; an unknown
    // path's 404 carries a message.
    if (res.status === 404 && body.length === 0)
      return { status: Status.NotFound, body };
    const status = STATUS[res.status];
    const message = utf8.decode(body);
    if (status === undefined)
      throw new Error(`server returned HTTP ${res.status}: ${message}`);
    throw new StatusError(status, message);
  }

  async ping(): Promise<void> {
    await this.request(encodePingFrame());
  }

  get isOpen(): boolean {
    return this.closed === null;
  }

  close(): void {
    this.closed ??= new ClosedError();
  }
}

/** A transport that talks to the server's HTTP listener. */
export function http(opts: HttpOptions): Transport {
  return new HttpTransport(opts);
}
