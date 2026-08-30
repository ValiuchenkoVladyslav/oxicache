/**
 * The package is meant to be tree-shaken into edge and lambda bundles, so
 * the main entry and the HTTP transport must not reach Bun's socket API:
 * neither by importing the TCP transport nor by touching the `Bun` global.
 */
import { describe, expect, test } from "bun:test";
import { dirname, resolve } from "node:path";

const src = resolve(import.meta.dir, "../src");
const transpiler = new Bun.Transpiler({ loader: "ts" });
const JS_EXT = /\.js$/;
const BUN_GLOBAL = /\bBun\./;
const BUN_IMPORT = /from "bun"/;
const SRC_TS = /^\.\/src\/.*\.ts$/;
const DIST_JS = /^\.\/dist\/.*\.js$/;
const DIST_DTS = /^\.\/dist\/.*\.d\.ts$/;

/** Every source file reachable from `entries` through relative imports. */
async function reachable(...entries: string[]): Promise<Map<string, string>> {
  const seen = new Map<string, string>();
  const queue = entries.map((e) => resolve(src, e));
  for (let file = queue.shift(); file !== undefined; file = queue.shift()) {
    if (seen.has(file)) continue;
    // biome-ignore lint/performance/noAwaitInLoops: a breadth-first walk is sequential
    const text = await Bun.file(file).text();
    seen.set(file, text);
    for (const { path, kind } of transpiler.scanImports(text)) {
      if (kind !== "import-statement" || !path.startsWith(".")) continue;
      // Source imports carry the emitted `.js` name; Bun maps it back to `.ts`.
      queue.push(resolve(dirname(file), path.replace(JS_EXT, ".ts")));
    }
  }
  return seen;
}

describe("module graph", () => {
  test("index + http transport never reach Bun sockets", async () => {
    const files = await reachable("index.ts", "transport/http.ts");
    const names = [...files.keys()].map((f) => f.slice(src.length + 1));
    expect(names).not.toContain("transport/tcp.ts");
    for (const [name, text] of files) {
      expect(text, name).not.toMatch(BUN_GLOBAL);
      expect(text, name).not.toMatch(BUN_IMPORT);
    }
    expect(names).toContain("value.ts");
  });

  test("the tcp transport is where node sockets live", async () => {
    const files = await reachable("transport/tcp.ts");
    const tcp = files.get(resolve(src, "transport/tcp.ts"));
    expect(tcp).toContain('from "node:net"');
    expect(tcp).not.toMatch(BUN_GLOBAL);
  });

  test("package.json is side-effect free with a subpath per transport", async () => {
    const pkg = await Bun.file(resolve(src, "../package.json")).json();
    expect(pkg.sideEffects).toBe(false);
    expect(Object.keys(pkg.exports)).toEqual([
      ".",
      "./transport/tcp",
      "./transport/http",
    ]);
    for (const sub of Object.values<Record<string, string>>(pkg.exports)) {
      // The bun condition serves the TypeScript source; everything else the build.
      expect(sub.bun).toMatch(SRC_TS);
      expect(sub.default).toMatch(DIST_JS);
      expect(sub.types).toMatch(DIST_DTS);
    }
  });
});
