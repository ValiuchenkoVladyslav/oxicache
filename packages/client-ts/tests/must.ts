/** Narrow away null/undefined in tests, failing with a clear message. */
export function must<T>(v: T | null | undefined): T {
  if (v === null || v === undefined) throw new Error("expected a value");
  return v;
}
