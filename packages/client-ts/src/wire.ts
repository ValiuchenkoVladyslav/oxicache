/**
 * Binary wire format shared with the Rust server and client.
 * Every integer is little-endian; values are opaque bytes.
 *
 *   request  := u8 op, u32 len, body      response := u8 status, u32 len, body
 *   GET(1)   key                  -> Ok value | NotFound
 *   SET(2)   u32 klen, key, value -> Ok
 *   DEL(3)   key                  -> Ok | NotFound
 *   AUTH(4)  token                -> Ok
 *   PING(5)  (body ignored)       -> Ok
 *   BATCH(6) u32 count, count × (u8 op, u32 len, body)
 *            -> Ok, u32 count, count × (u8 status, u32 len, body)
 */

export const HEADER_LEN = 5;
/** Largest frame body either side accepts. */
export const MAX_FRAME = 64 << 20;
/** Most items in one batch. */
export const MAX_ITEMS = 1 << 16;

export enum Op {
  Get = 1,
  Set = 2,
  Del = 3,
  Auth = 4,
  /** Keeps a quiet connection open; the server ignores the body. */
  Ping = 5,
  Batch = 6,
}

export enum Status {
  Ok = 0,
  BadRequest = 1,
  UnknownOp = 2,
  TooLarge = 3,
  Unauthorized = 4,
  NotFound = 5,
}

/**
 * How long the TCP transport lets a connection go without a write before it
 * sends a PING: a third of the server's default idle timeout (300 s), so two
 * lost or late heartbeats still leave the connection open.
 */
export const KEEPALIVE_MS = 100_000;

/** Anything accepted as a key or value. Strings are UTF-8 encoded. */
export type Bin = string | Uint8Array;

const utf8 = new TextEncoder();

export function toBytes(b: Bin): Uint8Array {
  return typeof b === "string" ? utf8.encode(b) : b;
}

export class DecodeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DecodeError";
  }
}

const U32 = 4;

function storeU32(b: Uint8Array, pos: number, n: number): void {
  b[pos] = n & 0xff;
  b[pos + 1] = (n >>> 8) & 0xff;
  b[pos + 2] = (n >>> 16) & 0xff;
  b[pos + 3] = n >>> 24;
}

/**
 * Scratch a string key is UTF-8-encoded into on its way to a frame — no
 * allocation, one copy into the frame. Valid until the next call.
 */
let scratch = new Uint8Array(256);

function scratchKey(key: Bin): Uint8Array {
  if (typeof key !== "string") return key;
  if (scratch.length < key.length * 3) {
    // Three bytes per UTF-16 code unit bounds any string's UTF-8 form.
    scratch = new Uint8Array(Math.max(key.length * 3, scratch.length * 2));
  }
  const { written } = utf8.encodeInto(key, scratch);
  return scratch.subarray(0, written);
}

/** Sequential little-endian reads over one frame body, bounds-checked. */
export class Reader {
  private readonly view: DataView;
  pos = 0;
  constructor(private readonly buf: Uint8Array) {
    this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  }
  get remaining(): number {
    return this.buf.length - this.pos;
  }
  need(n: number): void {
    if (this.remaining < n) {
      throw new DecodeError(
        `unexpected end of frame: needed ${n - this.remaining} more bytes`,
      );
    }
  }
  u8(): number {
    this.need(1);
    // biome-ignore lint/style/noNonNullAssertion: need(1) just checked pos < end
    return this.buf[this.pos++]!;
  }
  u32(): number {
    this.need(U32);
    const n = this.view.getUint32(this.pos, true);
    this.pos += U32;
    return n;
  }
  blob(): Uint8Array {
    const len = this.u32();
    this.need(len);
    const b = this.buf.subarray(this.pos, this.pos + len);
    this.pos += len;
    return b;
  }
  finish(): void {
    if (this.remaining !== 0) {
      throw new DecodeError(`trailing ${this.remaining} bytes after frame`);
    }
  }
}

function checkCount(n: number): void {
  if (n > MAX_ITEMS) {
    throw new RangeError(
      `${n} items in one batch exceeds the limit of ${MAX_ITEMS}`,
    );
  }
}

/** A GET or DEL frame: the body is the key. */
function keyFrame(op: Op, key: Bin): Uint8Array {
  const k = scratchKey(key);
  const f = new Uint8Array(HEADER_LEN + k.length);
  f[0] = op;
  storeU32(f, 1, k.length);
  f.set(k, HEADER_LEN);
  return f;
}

/** `GET key`. */
export function encodeGetFrame(key: Bin): Uint8Array {
  return keyFrame(Op.Get, key);
}

/** `DEL key`. */
export function encodeDelFrame(key: Bin): Uint8Array {
  return keyFrame(Op.Del, key);
}

/** `SET u32 klen, key, value`. */
export function encodeSetFrame(key: Bin, value: Uint8Array): Uint8Array {
  const k = scratchKey(key);
  const f = new Uint8Array(HEADER_LEN + U32 + k.length + value.length);
  f[0] = Op.Set;
  storeU32(f, 1, U32 + k.length + value.length);
  storeU32(f, HEADER_LEN, k.length);
  f.set(k, HEADER_LEN + U32);
  f.set(value, HEADER_LEN + U32 + k.length);
  return f;
}

/** One request of a batch, ready to encode. */
export type BatchItem =
  | { readonly op: Op.Get | Op.Del; readonly key: Bin }
  | { readonly op: Op.Set; readonly key: Bin; readonly value: Uint8Array };

/**
 * A BATCH frame: `u32 count, count × (u8 op, u32 len, body)`, each body
 * exactly what the standalone op would carry. Throws RangeError past
 * `MAX_ITEMS`.
 */
export function encodeBatchFrame(items: readonly BatchItem[]): Uint8Array {
  checkCount(items.length);
  const keys: Uint8Array[] = new Array(items.length);
  let size = HEADER_LEN + U32;
  for (let i = 0; i < items.length; i++) {
    // biome-ignore lint/style/noNonNullAssertion: i < items.length
    const item = items[i]!;
    const k = toBytes(item.key);
    keys[i] = k;
    size += HEADER_LEN + k.length;
    if (item.op === Op.Set) size += U32 + item.value.length;
  }
  const f = new Uint8Array(size);
  f[0] = Op.Batch;
  storeU32(f, 1, size - HEADER_LEN);
  storeU32(f, HEADER_LEN, items.length);
  let pos = HEADER_LEN + U32;
  for (let i = 0; i < items.length; i++) {
    // biome-ignore-start lint/style/noNonNullAssertion: same bounds as the sizing pass
    const item = items[i]!;
    const k = keys[i]!;
    // biome-ignore-end lint/style/noNonNullAssertion: end
    f[pos] = item.op;
    if (item.op === Op.Set) {
      storeU32(f, pos + 1, U32 + k.length + item.value.length);
      storeU32(f, pos + HEADER_LEN, k.length);
      f.set(k, pos + HEADER_LEN + U32);
      pos += HEADER_LEN + U32 + k.length;
      f.set(item.value, pos);
      pos += item.value.length;
    } else {
      storeU32(f, pos + 1, k.length);
      f.set(k, pos + HEADER_LEN);
      pos += HEADER_LEN + k.length;
    }
  }
  return f;
}

/** One answer inside a BATCH response. */
export interface Reply {
  status: number;
  body: Uint8Array;
}

/** The replies of a BATCH response: `u32 count, count × (u8 status, u32 len, body)`. */
export function decodeReplies(body: Uint8Array): Reply[] {
  const r = new Reader(body);
  const n = r.u32();
  const out: Reply[] = [];
  for (let i = 0; i < n; i++) {
    const status = r.u8();
    out.push({ status, body: r.blob() });
  }
  r.finish();
  return out;
}

export function encodeRawFrame(op: Op, body: Uint8Array): Uint8Array {
  const f = new Uint8Array(HEADER_LEN + body.length);
  f[0] = op;
  storeU32(f, 1, body.length);
  f.set(body, HEADER_LEN);
  return f;
}

/** A complete PING request: header only. */
export function encodePingFrame(): Uint8Array {
  return encodeRawFrame(Op.Ping, new Uint8Array(0));
}

export interface Frame {
  tag: number;
  body: Uint8Array;
}

/**
 * Accumulates stream chunks and yields complete frames. Bodies are copied
 * out of the buffer they arrived in, so callers may hold them indefinitely.
 */
export class FrameReader {
  private buf = new Uint8Array(64 << 10);
  private start = 0;
  private end = 0;
  /**
   * The chunk being parsed in place: frames are usually whole inside one
   * chunk and then never touch the accumulation buffer — only a trailing
   * partial frame is copied there.
   */
  private direct: Uint8Array | null = null;
  private dpos = 0;

  constructor(private readonly maxFrame = MAX_FRAME) {}

  /** Append `chunk`, then call `next()` until it returns null. */
  push(chunk: Uint8Array): void {
    if (this.direct !== null) {
      // Pushed without draining: carry the unparsed tail over first.
      this.copyIn(this.direct.subarray(this.dpos));
      this.direct = null;
    }
    if (this.end === this.start) {
      this.direct = chunk;
      this.dpos = 0;
      return;
    }
    this.copyIn(chunk);
  }

  private copyIn(chunk: Uint8Array): void {
    const need = this.end + chunk.length;
    if (need > this.buf.length) {
      const live = this.end - this.start;
      if (live + chunk.length <= this.buf.length) {
        this.buf.copyWithin(0, this.start, this.end);
      } else {
        const grown = new Uint8Array(
          Math.max(this.buf.length * 2, live + chunk.length),
        );
        grown.set(this.buf.subarray(this.start, this.end));
        this.buf = grown;
      }
      this.start = 0;
      this.end = live;
    }
    this.buf.set(chunk, this.end);
    this.end += chunk.length;
  }

  /** The next complete frame, or null if more input is needed. */
  next(): Frame | null {
    const d = this.direct;
    if (d !== null) {
      const avail = d.length - this.dpos;
      if (avail >= HEADER_LEN) {
        const s = this.dpos;
        const len = this.frameLen(d, s);
        if (avail >= HEADER_LEN + len) {
          this.dpos = s + HEADER_LEN + len;
          if (this.dpos === d.length) this.direct = null;
          return {
            // biome-ignore lint/style/noNonNullAssertion: avail >= HEADER_LEN
            tag: d[s]!,
            body: d.slice(s + HEADER_LEN, s + HEADER_LEN + len),
          };
        }
      }
      // A partial frame: move it to the accumulation buffer and wait.
      this.copyIn(d.subarray(this.dpos));
      this.direct = null;
      return null;
    }
    const avail = this.end - this.start;
    if (avail < HEADER_LEN) return null;
    const b = this.buf;
    const s = this.start;
    const len = this.frameLen(b, s);
    if (avail < HEADER_LEN + len) return null;
    const body = b.slice(s + HEADER_LEN, s + HEADER_LEN + len);
    this.start = s + HEADER_LEN + len;
    if (this.start === this.end) this.start = this.end = 0;
    // biome-ignore lint/style/noNonNullAssertion: avail >= HEADER_LEN
    return { tag: b[s]!, body };
  }

  /** Body length of the frame header at `s`; the caller checked the bound. */
  private frameLen(b: Uint8Array, s: number): number {
    // biome-ignore-start lint/style/noNonNullAssertion: s + 4 is within the buffer
    const len =
      b[s + 1]! |
      (b[s + 2]! << 8) |
      (b[s + 3]! << 16) |
      ((b[s + 4]! << 24) >>> 0);
    // biome-ignore-end lint/style/noNonNullAssertion: end of the header read
    if (len > this.maxFrame) {
      throw new DecodeError(
        `response of ${len} bytes exceeds the client limit of ${this.maxFrame}`,
      );
    }
    return len;
  }
}
