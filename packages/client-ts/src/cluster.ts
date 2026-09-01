/**
 * A cluster of independent servers, the way a memcached client uses one:
 * every key belongs to exactly one server, picked by consistent hashing (see
 * `./ring.ts`), and the servers know nothing of each other. There is no
 * replication and no data failover — a key that moves to another server is
 * simply cold there.
 *
 * The cluster is itself a {@link Transport}, so it is used like any other:
 * `Client.connect(cluster({ nodes }))`. It routes each request by the key in
 * its frame and splits a batch into one request per server, answered in the
 * order it was built.
 */
import { Ring } from "./ring.js";
import { ClosedError, StatusError, type Transport } from "./transport.js";
import {
  type Bin,
  DecodeError,
  decodeReplies,
  encodeBatchOf,
  encodePingFrame,
  encodeReplies,
  HEADER_LEN,
  Op,
  Reader,
  type Reply,
  Status,
  toBytes,
} from "./wire.js";

/** Bytes in the `u32` length that a SET body puts its key behind. */
const U32 = 4;

/**
 * What a cluster does with a server that stops answering, the way memcached
 * clients that eject hosts do it: after `failures` connection failures in a
 * row the server is dropped from the ring, so its keys go to the next server
 * round, and `retryMs` later it is let back in. One answer of any kind — a
 * value, a miss, even a refusal — puts it back at once and clears the count.
 *
 * Dropping a server moves its keys, and moves them back when it returns, so a
 * value written while it was out is not the one a later read finds: a cache
 * entry can go stale across an outage. Cluster-wide that is the same trade
 * memcached makes; pass `failover: null` for a cluster that would rather see
 * the error than the stale value.
 */
export interface Failover {
  /** Connection failures in a row before the server is dropped. */
  readonly failures: number;
  /** How long a dropped server stays out of the ring. */
  readonly retryMs: number;
}

/** Two failures in a row, back in after 30 s. */
export const DEFAULT_FAILOVER: Failover = { failures: 2, retryMs: 30_000 };

/** One server of a cluster. */
export interface ClusterNode {
  /**
   * The server's identity on the ring, and what every other client of this
   * cluster must call it too: the address as written, `10.0.0.1:4433`, which
   * is what the Rust client hashes.
   */
  readonly name: string;
  /**
   * Open the connection to this server — `() => tcp({ hostname, port, token })`,
   * or any other transport. Called again when a dropped server is retried and
   * the last attempt never got as far as a connection.
   */
  open(): Transport | Promise<Transport>;
}

export interface ClusterOptions {
  /** The servers, each named once. */
  readonly nodes: readonly ClusterNode[];
  /** How a server that stops answering is treated; `null` never drops one. */
  readonly failover?: Failover | null;
}

/** Whether an error says the server is not answering, as opposed to answering
 * something the call did not want. A refused token counts: the transport
 * behind it is dead, so this server is out until it is replaced. */
function unreachable(e: unknown): boolean {
  return e instanceof StatusError ? e.status === Status.Unauthorized : true;
}

/** One server's connection and what the failure policy knows about it. */
class Member {
  private transport: Transport | null = null;
  private opening: Promise<Transport> | null = null;
  // Explicit rather than inferred: `false` on its own narrows the field to
  // the literal type and reads as a condition that can never be true.
  private closed: boolean = false;
  /** Connection failures since the last answer. */
  private failures = 0;
  /** `Date.now()` until which this server stays out of the ring. */
  private downUntil = 0;

  constructor(readonly node: ClusterNode) {}

  isUp(now: number): boolean {
    return this.downUntil <= now;
  }

  /**
   * The connection to this server, opened if there is not one yet; every
   * call that wants it while an attempt runs waits on that one attempt. The
   * attempt is forgotten in a `finally` on the promise rather than inside
   * `start`, whose body runs — and would clear it — before it has been
   * remembered when a factory throws where it could have rejected.
   */
  connect(): Promise<Transport> {
    if (this.transport !== null) return Promise.resolve(this.transport);
    this.opening ??= this.start().finally(() => {
      this.opening = null;
    });
    return this.opening;
  }

  private async start(): Promise<Transport> {
    const transport = await this.node.open();
    // Opened after the cluster was closed: nobody wants it.
    if (this.closed) transport.close();
    else this.transport = transport;
    return transport;
  }

  /** The server answered, whatever it answered: it is up, and whatever it
   * failed with before does not count any more. */
  answered(): void {
    // `downUntil` is only ever set together with `failures`, so a clear count
    // means there is nothing to clear.
    if (this.failures !== 0) {
      this.failures = 0;
      this.downUntil = 0;
    }
  }

  /** One connection failure; drops the server once they add up. */
  failed(policy: Failover | null): void {
    this.failures++;
    if (policy !== null && this.failures >= policy.failures)
      this.dropOut(policy);
  }

  /** Out of the ring for a whole retry window, with the count at the limit so
   * that one failure after it re-drops the server. */
  dropOut(policy: Failover): void {
    this.failures = policy.failures;
    this.downUntil = Date.now() + policy.retryMs;
  }

  close(): void {
    this.closed = true;
    this.transport?.close();
  }
}

/**
 * The key a request body is routed by: GET and DEL carry it alone, a SET puts
 * it behind its `u32` length. Read here rather than through a `Reader` so
 * that routing a call allocates nothing.
 */
function keyOf(op: number, body: Uint8Array): Uint8Array {
  if (op !== Op.Set) return body;
  const len =
    (body[0] as number) |
    ((body[1] as number) << 8) |
    ((body[2] as number) << 16) |
    ((body[3] as number) << 24);
  return body.subarray(U32, U32 + len);
}

/** The items of a batch that go to one server, and where in the batch they came from. */
interface Group {
  items: Uint8Array[];
  at: number[];
}

const EMPTY = new Uint8Array(0);

/**
 * A set of servers with the keyspace spread over them. Hand it to
 * `Client.connect`; keep the reference to ask {@link nodeFor} where a key
 * lives or {@link live} which servers are still in the ring.
 */
export class ClusterTransport implements Transport {
  private readonly ring: Ring;
  private dead: ClosedError | null = null;

  /** Built by {@link cluster}, which is what opens it. */
  private constructor(
    private readonly members: readonly Member[],
    private readonly failover: Failover | null,
  ) {
    this.ring = new Ring(members.map((m) => m.node.name));
  }

  /** Connect to every server of `nodes` at once. One that is down starts out
   * of the ring rather than costing the first keys that hash to it a timeout. */
  static async connect(
    nodes: readonly ClusterNode[],
    failover: Failover | null,
  ): Promise<ClusterTransport> {
    const t = new ClusterTransport(
      nodes.map((n) => new Member(n)),
      failover,
    );
    await t.open();
    return t;
  }

  private async open(): Promise<void> {
    // Opened *and* pinged: a transport is free to connect lazily (`http`
    // does no I/O until its first request), so only a round trip tells a
    // server that is up from one that is not.
    const settled = await Promise.allSettled(
      this.members.map((m) => m.connect().then((t) => t.ping())),
    );
    let failure: { e: unknown } | null = null;
    let up = 0;
    for (const [i, result] of settled.entries()) {
      if (result.status === "fulfilled") {
        up++;
        continue;
      }
      // biome-ignore lint/style/noNonNullAssertion: one result per member
      const member = this.members[i]!;
      // A token one server refuses is not weather: every server of a cluster
      // shares one, so the cluster is misconfigured.
      if (result.reason instanceof StatusError) {
        this.close();
        throw result.reason;
      }
      if (this.failover !== null) member.dropOut(this.failover);
      failure ??= { e: result.reason };
    }
    // Nothing to spread keys over is not a cluster; the caller hears why
    // rather than getting one that fails every call.
    if (up === 0 && failure !== null) {
      this.close();
      throw failure.e;
    }
  }

  /** Which server holds `key` right now: its own, or the one that took it
   * over while that server is out of the ring. */
  nodeFor(key: Bin): string {
    return this.pick(toBytes(key)).node.name;
  }

  /** The servers in the ring right now, in the order they were given; the
   * ones dropped after failing are missing until they are retried. */
  live(): string[] {
    const now = Date.now();
    return this.members.filter((m) => m.isUp(now)).map((m) => m.node.name);
  }

  /** Whether the cluster is usable; its servers come and go under it. */
  get isOpen(): boolean {
    return this.dead === null;
  }

  /** Close every connection; in-flight and later calls reject with ClosedError. */
  close(): void {
    this.dead ??= new ClosedError();
    for (const m of this.members) m.close();
  }

  async ping(): Promise<void> {
    await this.request(encodePingFrame());
  }

  /** Send one request to the server its key belongs to. A batch is split
   * over the servers its keys belong to and answered as one. */
  request(frame: Uint8Array): Promise<Reply> {
    if (this.dead) return Promise.reject(this.dead);
    const op = frame[0] as Op;
    if (op === Op.Auth) {
      return Promise.reject(
        new Error(
          "AUTH is not a cluster request: every server is authenticated by its own transport",
        ),
      );
    }
    // A ping is for the cluster, not for one key: every server answers it,
    // which is also how a dropped server is found to be back early.
    if (op === Op.Ping) return this.pingAll(frame);
    if (op === Op.Batch) return this.batch(frame);
    return this.call(this.pick(keyOf(op, frame.subarray(HEADER_LEN))), frame);
  }

  private pick(key: Uint8Array): Member {
    const now = Date.now();
    const at =
      this.members.length === 1
        ? 0
        : this.ring.locate(key, (i) =>
            // biome-ignore lint/style/noNonNullAssertion: the ring holds one point set per member
            this.members[i]!.isUp(now),
          );
    // biome-ignore lint/style/noNonNullAssertion: an index the ring took from `members`
    return this.members[at]!;
  }

  /** One request to one server, with its outcome fed to the failure policy. */
  private async call(member: Member, frame: Uint8Array): Promise<Reply> {
    if (this.dead) throw this.dead;
    try {
      const transport = await member.connect();
      const reply = await transport.request(frame);
      member.answered();
      return reply;
    } catch (e) {
      if (unreachable(e)) member.failed(this.failover);
      else member.answered();
      throw e;
    }
  }

  private async pingAll(frame: Uint8Array): Promise<Reply> {
    const settled = await Promise.allSettled(
      this.members.map((m) => this.call(m, frame)),
    );
    for (const result of settled) {
      if (result.status === "rejected") throw result.reason;
    }
    return { status: Status.Ok, body: EMPTY };
  }

  /**
   * Split a batch by the server each item's key belongs to, send one request
   * per server at once, and put the answers back in the order the batch was
   * built. The whole call fails if any of those requests does, so a batch is
   * all-or-nothing about the servers it reaches, not about the writes: the
   * servers that did answer have applied their share.
   */
  private async batch(frame: Uint8Array): Promise<Reply> {
    const body = frame.subarray(HEADER_LEN);
    const r = new Reader(body);
    const n = r.u32();
    const groups = new Map<Member, Group>();
    for (let i = 0; i < n; i++) {
      const start = r.pos;
      const op = r.u8();
      const item = r.blob();
      const member = this.pick(keyOf(op, item));
      let group = groups.get(member);
      if (group === undefined) {
        group = { items: [], at: [] };
        groups.set(member, group);
      }
      group.items.push(body.subarray(start, r.pos));
      group.at.push(i);
    }
    r.finish();
    // An empty batch asks nobody anything.
    if (groups.size === 0)
      return { status: Status.Ok, body: encodeReplies([]) };
    const parts = [...groups.entries()];
    // biome-ignore lint/style/noNonNullAssertion: size === 1
    if (parts.length === 1) return this.call(parts[0]![0], frame);
    const settled = await Promise.allSettled(
      parts.map(([member, group]) =>
        this.call(member, encodeBatchOf(group.items)),
      ),
    );
    const out = new Array<Reply>(n);
    let failure: { e: unknown } | null = null;
    for (const [i, result] of settled.entries()) {
      if (result.status === "rejected") {
        failure ??= { e: result.reason };
        continue;
      }
      // biome-ignore lint/style/noNonNullAssertion: one result per part
      const group = parts[i]![1];
      const replies = decodeReplies(result.value.body);
      if (replies.length !== group.at.length) {
        throw new DecodeError(
          `server answered ${replies.length} replies for ${group.at.length} ops`,
        );
      }
      for (const [j, at] of group.at.entries()) {
        // biome-ignore lint/style/noNonNullAssertion: as many replies as items
        out[at] = replies[j]!;
      }
    }
    if (failure !== null) throw failure.e;
    return { status: Status.Ok, body: encodeReplies(out) };
  }
}

/**
 * Connect to every server and spread the keyspace over them:
 *
 * ```ts
 * const nodes = ["10.0.0.1:4433", "10.0.0.2:4433"];
 * const c = await Client.connect(
 *   cluster({
 *     nodes: nodes.map((name) => ({
 *       name,
 *       open: () => tcp({ hostname: name.split(":")[0], port: 4433, token }),
 *     })),
 *   }),
 * );
 * ```
 *
 * Servers that are down are dropped from the ring and retried by the
 * {@link Failover} policy, but a refused token (or anything else a server
 * answers the handshake with) rejects here, and so does having no server up
 * at all.
 */
export async function cluster(opts: ClusterOptions): Promise<ClusterTransport> {
  const names = opts.nodes.map((n) => n.name);
  if (names.length === 0) {
    throw new RangeError("a cluster needs at least one server");
  }
  // The ring is built from the names, and two servers with one name would sit
  // on top of each other.
  if (new Set(names).size !== names.length) {
    throw new RangeError(`duplicate server in a cluster: ${names.join(", ")}`);
  }
  // A default that fills in for a missing policy but not for an explicit
  // `null`, which is the way to turn dropping servers off.
  const { failover = DEFAULT_FAILOVER } = opts;
  // `failures: 0` would drop a server on its first failure and leave the
  // count at zero, which no answer clears; the Rust client makes it
  // unrepresentable with a `NonZeroU32`.
  if (failover !== null) {
    if (!Number.isInteger(failover.failures) || failover.failures < 1) {
      throw new RangeError(
        `failover.failures must be a whole number of at least 1, got ${failover.failures}`,
      );
    }
    if (!(failover.retryMs >= 0)) {
      throw new RangeError(
        `failover.retryMs must not be negative, got ${failover.retryMs}`,
      );
    }
  }
  return ClusterTransport.connect(opts.nodes, failover);
}
