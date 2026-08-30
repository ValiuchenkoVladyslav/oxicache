/**
 * Binary wire format shared with the Rust server and client.
 * Every integer is little-endian; values are opaque bytes.
 *
 *   request  := u8 op, u32 len, body      response := u8 status, u32 len, body
 *   keys     := u32 count, count × (u32 len, bytes)
 *   entries  := u32 count, count × (u32 klen, key, u32 vlen, value)
 *   GET(1) keys    -> u32 count, count × (u8 0 | u8 1, u32 len, value)
 *   SET(2) entries -> empty
 *   DEL(3) keys    -> u32 count, count × u8 found
 *   AUTH(4) token  -> empty
 *   PING(5) empty  -> empty
 */

export const HEADER_LEN = 5;
/** Largest frame body either side accepts. */
export const MAX_FRAME = 64 << 20;
/** Most keys or entries one request may name. */
export const MAX_ITEMS = 1 << 16;

export enum Op {
  Get = 1,
  Set = 2,
  Del = 3,
  Auth = 4,
  /** Keeps a quiet connection open; the server ignores the body. */
  Ping = 5,
}

export enum Status {
  Ok = 0,
  BadRequest = 1,
  UnknownOp = 2,
  TooLarge = 3,
  Unauthorized = 4,
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

class Writer {
  readonly buf: Uint8Array;
  private readonly view: DataView;
  pos = 0;
  constructor(size: number) {
    this.buf = new Uint8Array(size);
    this.view = new DataView(this.buf.buffer);
  }
  u8(n: number): void {
    this.buf[this.pos++] = n;
  }
  u32(n: number): void {
    this.view.setUint32(this.pos, n, true);
    this.pos += U32;
  }
  blob(b: Uint8Array): void {
    this.u32(b.length);
    this.buf.set(b, this.pos);
    this.pos += b.length;
  }
}

class Reader {
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
      `${n} items in one request exceeds the limit of ${MAX_ITEMS}`,
    );
  }
}

/** Encode a whole request frame: header followed by a key list (GET, DEL). */
export function encodeKeysFrame(op: Op, keys: readonly Bin[]): Uint8Array {
  checkCount(keys.length);
  const ks = keys.map(toBytes);
  let size = HEADER_LEN + U32;
  for (const k of ks) size += U32 + k.length;
  const w = new Writer(size);
  w.u8(op);
  w.u32(size - HEADER_LEN);
  w.u32(ks.length);
  for (const k of ks) w.blob(k);
  return w.buf;
}

/** Encode a whole SET frame: header followed by key/value entries. */
export function encodeEntriesFrame(
  entries: readonly (readonly [Bin, Bin])[],
): Uint8Array {
  checkCount(entries.length);
  const es = entries.map(([k, v]) => [toBytes(k), toBytes(v)] as const);
  let size = HEADER_LEN + U32;
  for (const [k, v] of es) size += 2 * U32 + k.length + v.length;
  const w = new Writer(size);
  w.u8(Op.Set);
  w.u32(size - HEADER_LEN);
  w.u32(es.length);
  for (const [k, v] of es) {
    w.blob(k);
    w.blob(v);
  }
  return w.buf;
}

/** Encode a whole frame whose body is one raw blob without a length prefix (AUTH). */
export function encodeRawFrame(op: Op, body: Uint8Array): Uint8Array {
  const w = new Writer(HEADER_LEN + body.length);
  w.u8(op);
  w.u32(body.length);
  w.buf.set(body, w.pos);
  return w.buf;
}

/** A complete PING request: header only. */
export function encodePingFrame(): Uint8Array {
  return encodeRawFrame(Op.Ping, new Uint8Array(0));
}

/** Decode a GET response body; slices are views into `body`. */
export function decodeValues(body: Uint8Array): (Uint8Array | null)[] {
  const r = new Reader(body);
  const n = r.u32();
  const out: (Uint8Array | null)[] = new Array(Math.min(n, body.length));
  out.length = 0;
  for (let i = 0; i < n; i++) {
    const tag = r.u8();
    if (tag === 0) out.push(null);
    else if (tag === 1) out.push(r.blob());
    else throw new DecodeError(`invalid tag byte ${tag}`);
  }
  r.finish();
  return out;
}

/** Decode a DEL response body. */
export function decodeFlags(body: Uint8Array): boolean[] {
  const r = new Reader(body);
  const n = r.u32();
  r.need(n);
  const out: boolean[] = new Array(n);
  for (let i = 0; i < n; i++) out[i] = r.u8() !== 0;
  r.finish();
  return out;
}

export interface Frame {
  tag: number;
  body: Uint8Array;
}

/**
 * Accumulates stream chunks and yields complete frames. Bodies are copied out
 * of the accumulation buffer so callers may hold them indefinitely.
 */
export class FrameReader {
  private buf = new Uint8Array(64 << 10);
  private start = 0;
  private end = 0;

  constructor(private readonly maxFrame = MAX_FRAME) {}

  /** Append `chunk`, then call `next()` until it returns null. */
  push(chunk: Uint8Array): void {
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
    const avail = this.end - this.start;
    if (avail < HEADER_LEN) return null;
    const b = this.buf;
    const s = this.start;
    // biome-ignore-start lint/style/noNonNullAssertion: avail >= HEADER_LEN, so s..s+4 are within the buffer
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
    if (avail < HEADER_LEN + len) return null;
    const body = b.slice(s + HEADER_LEN, s + HEADER_LEN + len);
    this.start = s + HEADER_LEN + len;
    if (this.start === this.end) this.start = this.end = 0;
    // biome-ignore lint/style/noNonNullAssertion: same bound as above
    return { tag: b[s]!, body };
  }
}
