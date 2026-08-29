/**
 * Compile-time assertions. `bun test` runs the trivial body; the real checks
 * are the `@ts-expect-error` lines, verified by `tsc --noEmit` (tsconfig
 * includes this directory), which fails if any of them stops erroring.
 */
import { expect, test } from "bun:test";
import type { Client, Value } from "../src/index";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
const assertType = <_T extends true>(): void => {};

interface User {
  id: number;
  name: string;
  tags: string[];
  joined: Date;
  avatar: Uint8Array | null;
  prefs: { theme: "dark" | "light"; limits: Record<string, number> };
}
class Point {
  constructor(
    public x: number,
    public y: number,
  ) {}
}

declare const c: Client;
declare const user: User;

// Single in, single out; array in, array out; return generic first.
async function _shapes() {
  // One key: one value. Several keys: a tuple of that length. Array: a list.
  const one = await c.get<User>("k");
  assertType<Equal<typeof one, User | null>>();
  const two = await c.get<User>("a", new Uint8Array(1));
  assertType<Equal<typeof two, [User | null, User | null]>>();
  const [x, y] = two;
  assertType<Equal<typeof x, User | null>>();
  assertType<Equal<typeof y, User | null>>();
  const three = await c.get<number>("a", "b", "c");
  assertType<Equal<typeof three, [number | null, number | null, number | null]>>();
  const sixteen = await c.get<number>("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16");
  assertType<Equal<typeof sixteen["length"], 16>>();
  const dyn: string[] = ["a", "b"];
  const fromArray = await c.get<User>(dyn);
  assertType<Equal<typeof fromArray, (User | null)[]>>();
  const fromTuple = await c.get<User>(["a", "b"]);
  assertType<Equal<typeof fromTuple, (User | null)[]>>();
  const dflt = await c.get("k");
  assertType<Equal<typeof dflt, Value | null>>();
  const d1 = await c.del("k");
  assertType<Equal<typeof d1, boolean>>();
  const dn = await c.del("k", "j");
  assertType<Equal<typeof dn, [boolean, boolean]>>();
  const dArr = await c.del(dyn);
  assertType<Equal<typeof dArr, boolean[]>>();
  // @ts-expect-error several keys in must not be assignable to single out
  const wrong: User | null = await c.get<User>("k", "j");
  void wrong;
  // @ts-expect-error a spread of unknown length must use the array form
  await c.get<User>(...dyn);
  // @ts-expect-error more than 16 literal keys must use the array form
  await c.get<number>("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17");

  // Accepted values: literals, interfaces, classes, primitives, mixed batches.
  await c.set("u", user);
  await c.set("p", new Point(1, 2));
  await c.set("lit", { a: 1, b: [1, "2", null, { c: new Date() }] });
  await c.set("big", 10n);
  await c.set("bin", new Uint8Array(2));
  await c.set(new Uint8Array([1]), null);
  await c.set(["a", 1], ["b", "two"], ["c", user]);
  await c.set([["a", 1], ["b", "two"], ["c", user]]);
  const dynEntries: [string, number][] = [["a", 1]];
  await c.set(dynEntries);

  // Rejected values.
  // @ts-expect-error functions
  await c.set("f", { id: 1, fn: () => {} });
  // @ts-expect-error symbols
  await c.set("s", Symbol("s"));
  // @ts-expect-error undefined
  await c.set("u", undefined);
  // @ts-expect-error undefined inside an object
  await c.set("u2", { a: undefined });
  // @ts-expect-error Map
  await c.set("m", new Map<string, number>());
  // @ts-expect-error Set
  await c.set("st", new Set<number>());
  // @ts-expect-error function-valued property in a variadic batch
  await c.set(["a", 1], ["b", { fn: () => 1 }]);
  // @ts-expect-error function-valued property in an array batch
  await c.set([["a", { fn: () => 1 }]]);
  // @ts-expect-error keys are Bin, not arbitrary values
  await c.get(42);
}

test("types compile", () => {
  expect(true).toBe(true);
});
