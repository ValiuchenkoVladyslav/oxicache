/**
 * The ring is only useful if every client agrees on it, so besides the
 * properties it must have (an even spread, keys that stay put when a server
 * leaves) it is checked against `testdata/ring.tsv`, which the Rust client
 * writes and checks too.
 */
import { describe, expect, test } from "bun:test";
import { resolve } from "node:path";
import { hash, POINTS, Ring } from "../src/ring";

const utf8 = new TextEncoder();
const key = (s: string) => utf8.encode(s);

/** `n` servers named the way an address is written. */
function names(n: number): string[] {
  return Array.from({ length: n }, (_, i) => `10.0.0.${i}:4433`);
}

/** The fixture as `[tag, ...fields]` rows, comments dropped. */
async function fixture(): Promise<string[][]> {
  const text = await Bun.file(
    resolve(import.meta.dir, "../../../testdata/ring.tsv"),
  ).text();
  return text
    .split("\n")
    .filter((line) => line !== "" && !line.startsWith("#"))
    .map((line) => line.split("\t"));
}

describe("hash", () => {
  test("MurmurHash3's published test vectors, every tail length", () => {
    expect(hash(key(""))).toBe(0);
    expect(hash(key("a"))).toBe(0x3c2569b2);
    expect(hash(key("ab"))).toBe(0x9bbfd75f);
    expect(hash(key("abc"))).toBe(0xb3dd93fa);
    expect(hash(key("abcd"))).toBe(0x43ed676a);
    expect(hash(key("Hello, world!"))).toBe(0xc0363e43);
  });

  test("hashes are unsigned 32-bit and collide with nothing here", () => {
    const seen = new Set<number>();
    for (let i = 0; i < 1000; i++) {
      const h = hash(key(`k${i}`));
      expect(Number.isInteger(h)).toBe(true);
      expect(h).toBeGreaterThanOrEqual(0);
      expect(h).toBeLessThanOrEqual(0xffffffff);
      seen.add(h);
    }
    expect(seen.size).toBe(1000);
  });
});

describe("ring", () => {
  test("the order servers are listed in does not matter", () => {
    const forward = names(5);
    const backward = [...forward].reverse();
    const a = new Ring(forward);
    const b = new Ring(backward);
    for (let i = 0; i < 2000; i++) {
      const k = key(`key:${i}`);
      expect(forward[a.locate(k, () => true)]).toBe(
        backward[b.locate(k, () => true)] as string,
      );
    }
  });

  test("keys are spread evenly", () => {
    const nodes = 8;
    const keys = 100_000;
    const ring = new Ring(names(nodes));
    const counts = new Array<number>(nodes).fill(0);
    for (let i = 0; i < keys; i++) {
      const at = ring.locate(key(`user:${i}`), () => true);
      counts[at] = (counts[at] as number) + 1;
    }
    const fair = keys / nodes;
    for (const c of counts) {
      expect(Math.abs(c - fair) / fair).toBeLessThan(0.12);
    }
  });

  test("losing a server moves only its own keys", () => {
    const all = names(5);
    const dead = 2;
    const short = all.filter((_, i) => i !== dead);
    const ring = new Ring(all);
    const smaller = new Ring(short);
    const keys = Array.from({ length: 5000 }, (_, i) => key(`key:${i}`));
    const rows = keys.map((k) => ({
      owner: all[ring.locate(k, () => true)] as string,
      // The same ring with the server skipped, and the ring it would have
      // been built as without it: both name where the key goes now.
      skipped: all[ring.locate(k, (n) => n !== dead)] as string,
      without: short[smaller.locate(k, () => true)] as string,
    }));
    const stayed = rows.filter((r) => r.owner !== all[dead]);
    expect(stayed.length).toBeGreaterThan(0);
    expect(rows.length - stayed.length).toBeGreaterThan(0);
    expect(stayed.filter((r) => r.owner !== r.without)).toEqual([]);
    expect(rows.filter((r) => r.skipped !== r.without)).toEqual([]);
  });

  test("a ring with nothing live still points at the key's own server", () => {
    const ring = new Ring(names(3));
    for (let i = 0; i < 100; i++) {
      const k = key(`key:${i}`);
      expect(ring.locate(k, () => false)).toBe(ring.locate(k, () => true));
    }
  });

  test("colliding points keep the lower name", () => {
    // `10.0.0.14:4433#494` and `10.0.0.3:4433#186` both hash to 21872133, so
    // one of the two points is dropped — the same one whichever order the
    // servers are listed in.
    const low = "10.0.0.14:4433";
    const high = "10.0.0.3:4433";
    expect(hash(key(`${low}#494`))).toBe(21872133);
    expect(hash(key(`${high}#186`))).toBe(21872133);
    const names = [low, high];
    const reversed = [high, low];
    const a = new Ring(names);
    const b = new Ring(reversed);
    for (let i = 0; i < 2000; i++) {
      const k = key(`key:${i}`);
      expect(names[a.locate(k, () => true)]).toBe(
        reversed[b.locate(k, () => true)] as string,
      );
    }
  });

  test("one server owns everything", () => {
    const ring = new Ring(names(1));
    expect(ring.locate(key("anything"), () => true)).toBe(0);
    expect(ring.locate(key("anything"), () => false)).toBe(0);
  });
});

describe("the shared fixture", () => {
  test("hashes and routes exactly as the Rust client does", async () => {
    const rows = await fixture();
    const nodes = rows
      .filter((r) => r[0] === "node")
      .map((r) => r[1] as string);
    expect(Number(rows.find((r) => r[0] === "points")?.[1])).toBe(POINTS);
    expect(nodes.length).toBeGreaterThan(1);

    const hashes = rows.filter((r) => r[0] === "hash");
    expect(hashes.length).toBeGreaterThan(0);
    for (const [, input, want] of hashes) {
      expect(hash(key(input as string))).toBe(Number(want));
    }

    const ring = new Ring(nodes);
    const routes = rows.filter((r) => r[0] === "route");
    expect(routes.length).toBeGreaterThan(0);
    for (const [, k, want] of routes) {
      expect(nodes[ring.locate(key(k as string), () => true)]).toBe(
        want as string,
      );
    }

    const down = rows.find((r) => r[0] === "down")?.[1] as string;
    const alive = (i: number) => nodes[i] !== down;
    const rerouted = rows.filter((r) => r[0] === "route-down");
    expect(rerouted.length).toBe(routes.length);
    for (const [, k, want] of rerouted) {
      expect(nodes[ring.locate(key(k as string), alive)]).toBe(want as string);
    }
  });
});
