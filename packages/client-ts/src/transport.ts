import { Status } from "./wire.js";

/** The server answered with a non-OK status. */
export class StatusError extends Error {
  override name = "StatusError";
  constructor(
    readonly status: Status,
    message: string,
  ) {
    super(`server returned ${Status[status] ?? status}: ${message}`);
  }
}

/** The transport is closed (or was closed before a reply arrived). */
export class ClosedError extends Error {
  override name = "ClosedError";
  constructor(cause?: unknown) {
    super("connection closed", cause === undefined ? undefined : { cause });
  }
}

/**
 * Carries request frames to a server and brings response bodies back.
 * `@oxicache/client/transport/tcp` (Bun sockets, pipelined) and
 * `@oxicache/client/transport/http` (`fetch`) are the built-in ones; anything
 * with this shape can be handed to `Client.connect`.
 */
export interface Transport {
  /**
   * Send one complete request frame (`u8 op, u32 len, body`) and resolve
   * with the response body. A non-OK status rejects with `StatusError`, a
   * dead transport with `ClosedError`.
   */
  request(frame: Uint8Array): Promise<Uint8Array>;
  /** Whether requests can still be made. */
  readonly isOpen: boolean;
  /** Release the transport; in-flight and later requests reject with `ClosedError`. */
  close(): void;
}
