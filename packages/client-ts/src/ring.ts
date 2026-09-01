/**
 * The consistent-hash ring a {@link cluster} picks a server with: every key
 * belongs to one server, and adding or losing a server moves only the keys
 * that server owns instead of reshuffling all of them.
 *
 * Every client of one cluster has to agree on the mapping, so the recipe is
 * fixed and spelled out rather than tuned:
 *
 * - a hash is MurmurHash3's 32-bit x86 variant with seed 0, which spreads
 *   near-identical names — every server of a cluster looks like every other —
 *   over the whole word;
 * - each server puts {@link POINTS} points on the ring, at `hash("<name>#<i>")`
 *   for `i` in `0..POINTS`, where the name is the server's address as it was
 *   given (`10.0.0.1:4433`);
 * - points are sorted by hash, and of two points with the same hash only the
 *   one whose server name sorts first survives, so the ring does not depend on
 *   the order the servers were listed in;
 * - a key belongs to the first point at or after its own hash, wrapping round
 *   the end.
 *
 * The Rust client's `crates/client-rs/src/ring.rs` is the same algorithm, and
 * `testdata/ring.tsv` pins both to the same answers.
 */

/**
 * Points one server puts on the ring. Ketama uses 160; measured over 3 to 16
 * servers and 100 000 keys, 160 leaves one server up to 16 % off its fair
 * share and 512 keeps every server within 9 %.
 */
export const POINTS = 512;

/** MurmurHash3's two mixing constants. */
const C1 = 0xcc9e2d51;
const C2 = 0x1b873593;

const utf8 = new TextEncoder();

function rotl(x: number, r: number): number {
  return (x << r) | (x >>> (32 - r));
}

function scramble(k: number): number {
  return Math.imul(rotl(Math.imul(k, C1), 15), C2);
}

/** Murmur3's finaliser: a bijection that spreads every input bit over the whole word. */
function fmix32(h: number): number {
  let x = h ^ (h >>> 16);
  x = Math.imul(x, 0x85ebca6b);
  x ^= x >>> 13;
  x = Math.imul(x, 0xc2b2ae35);
  return (x ^ (x >>> 16)) >>> 0;
}

/** Where `bytes` land on the ring: MurmurHash3's 32-bit x86 variant with seed 0. */
export function hash(bytes: Uint8Array): number {
  const n = bytes.length;
  const blocks = n - (n % 4);
  let h = 0;
  for (let i = 0; i < blocks; i += 4) {
    const k =
      (bytes[i] as number) |
      ((bytes[i + 1] as number) << 8) |
      ((bytes[i + 2] as number) << 16) |
      ((bytes[i + 3] as number) << 24);
    h ^= scramble(k);
    h = (Math.imul(rotl(h, 13), 5) + 0xe6546b64) | 0;
  }
  if (blocks !== n) {
    let k = 0;
    for (let i = n - 1; i >= blocks; i--) k = (k << 8) | (bytes[i] as number);
    h ^= scramble(k);
  }
  return fmix32(h ^ n);
}

/** One server's point on the ring, with what it takes to order it. */
interface Point {
  h: number;
  node: number;
  name: Uint8Array;
}

/** Bytewise, the order the Rust client compares names in. */
function compare(a: Uint8Array, b: Uint8Array): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const d = (a[i] as number) - (b[i] as number);
    if (d !== 0) return d;
  }
  return a.length - b.length;
}

/** The servers of a cluster as points on a circle: built once, read by every call. */
export class Ring {
  /** Point hashes, ascending and free of duplicates. */
  private readonly hashes: Uint32Array;
  /** The server each point belongs to, by its index in `names`. */
  private readonly owners: Uint32Array;

  /** The ring `names` describe; `names` must not be empty. */
  constructor(names: readonly string[]) {
    const points: Point[] = [];
    for (const [node, name] of names.entries()) {
      const raw = utf8.encode(name);
      for (let p = 0; p < POINTS; p++) {
        points.push({ h: hash(utf8.encode(`${name}#${p}`)), node, name: raw });
      }
    }
    points.sort((a, b) => a.h - b.h || compare(a.name, b.name));
    this.hashes = new Uint32Array(points.length);
    this.owners = new Uint32Array(points.length);
    let n = 0;
    for (const p of points) {
      if (n > 0 && this.hashes[n - 1] === p.h) continue;
      this.hashes[n] = p.h;
      this.owners[n] = p.node;
      n++;
    }
    this.hashes = this.hashes.subarray(0, n);
    this.owners = this.owners.subarray(0, n);
  }

  /**
   * The server `key` belongs to: the first one clockwise from the key's own
   * hash that `live` accepts. With every server refused — a whole cluster that
   * is down — the key's own server, because trying it is what finds out it is
   * back.
   */
  locate(key: Uint8Array, live: (node: number) => boolean): number {
    const n = this.hashes.length;
    const h = hash(key);
    let lo = 0;
    let hi = n;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if ((this.hashes[mid] as number) < h) lo = mid + 1;
      else hi = mid;
    }
    const start = lo % n;
    for (let i = 0; i < n; i++) {
      const node = this.owners[(start + i) % n] as number;
      if (live(node)) return node;
    }
    return this.owners[start] as number;
  }
}
