/**
 * Compile-time assertions. `bun test` runs the trivial body; the real checks
 * are the `@ts-expect-error` lines, verified by `tsc --noEmit` (tsconfig
 * includes this directory), which fails if any of them stops erroring.
 */
import { expect, test } from "bun:test";
import { type Client, op, type Value } from "../src/index";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? true
    : false;
const assertType = <_T extends true>(): void => {
  // type-level only
};

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

async function _shapes() {
  // One key in, one value out; the type is the generic, `Value` by default.
  const one = await c.get<User>("k");
  assertType<Equal<typeof one, User | null>>();
  const dflt = await c.get("k");
  assertType<Equal<typeof dflt, Value | null>>();
  const d1 = await c.del("k");
  assertType<Equal<typeof d1, boolean>>();
  // @ts-expect-error keys are Bin, not arbitrary values
  await c.get(42);
  // @ts-expect-error one key per call; several go in a batch
  await c.get("a", "b");
  // @ts-expect-error one key per call; several go in a batch
  await c.del("a", "b");

  // A batch is typed positionally after its ops.
  const [u, s, gone] = await c.batch([
    op.get<User>("u"),
    op.set("k", 1),
    op.del("t"),
  ]);
  assertType<Equal<typeof u, User | null>>();
  assertType<Equal<typeof s, undefined>>();
  assertType<Equal<typeof gone, boolean>>();
  const mixed = await c.batch([op.get<[User, number]>("a"), op.get("b")]);
  assertType<Equal<typeof mixed, [[User, number] | null, Value | null]>>();
  await c.batch([]);
  // A list built at runtime is a list of results.
  const dyn = ["a", "b"].map((k) => op.get<User>(k));
  const fromArray = await c.batch(dyn);
  assertType<Equal<typeof fromArray, (User | null)[]>>();
  // @ts-expect-error a batch takes ops, not keys
  await c.batch(["a", "b"]);
  // @ts-expect-error several results must not be assignable to one
  const _wrong: User | null = await c.batch([op.get<User>("a")]);

  // Accepted values: literals, interfaces, classes, primitives.
  await c.set("u", user);
  await c.set("p", new Point(1, 2));
  await c.set("lit", { a: 1, b: [1, "2", null, { c: new Date() }] });
  await c.set("big", 10n);
  await c.set("bin", new Uint8Array(2));
  await c.set(new Uint8Array([1]), null);
  op.set("u", user);
  op.set("p", new Point(1, 2));

  // Rejected values.
  // @ts-expect-error functions
  await c.set("f", { id: 1, fn: () => 0 });
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
  // @ts-expect-error function-valued property in a batch set
  op.set("b", { fn: () => 1 });
  // @ts-expect-error one key and value per set; several go in a batch
  await c.set(["a", 1], ["b", 2]);
}

test("types compile", () => {
  expect(true).toBe(true);
});
