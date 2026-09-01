# The oxicache wire protocol

Everything a third client needs: the frame format, the six ops, the batch envelope, the
connection lifecycle, the HTTP mapping, and the two conventions the shipped clients share on
top of it (MessagePack values, the cluster ring).

The reference implementation is `crates/wire` (`oxicache-wire`), shared by the server and the
Rust client; `packages/client-ts/src/wire.ts` is the same protocol in TypeScript.

## Frames

Every message, in both directions, is one frame:

```
request   := u8 op, u32 len, body[len]
response  := u8 status, u32 len, body[len]
```

The header is 5 bytes, the length is little-endian, and every integer in this protocol is
little-endian. A request's tag is its op, a response's tag is its status. Values are opaque
bytes — the server never looks inside one.

Requests on a connection are answered in order and nothing else is ever sent, so a client can
pipeline freely: write as many frames as it likes and match responses to requests by position.
There is no request id and no server-initiated frame; a response arriving that no request is
waiting for is a desync, and the shipped clients treat it as a fatal protocol error.

### Ops

| op | name | body | answers |
|---|---|---|---|
| 1 | GET | `key` | `Ok, value` \| `NotFound`, empty |
| 2 | SET | `u32 klen, key, value` | `Ok`, empty \| `TooLarge` |
| 3 | DEL | `key` | `Ok`, empty (deleted) \| `NotFound`, empty |
| 4 | AUTH | `token` | `Ok`, empty \| `Unauthorized` |
| 5 | PING | ignored | `Ok`, empty |
| 6 | BATCH | `u32 count, count × (u8 op, u32 len, body)` | `Ok, u32 count, count × (u8 status, u32 len, body)` |

An unknown op byte is `UnknownOp`, and the connection stays open.

`AUTH` sent by a connection that is already authenticated is a no-op that answers `Ok`. `PING`'s
body is ignored rather than validated, so a later client can attach something to it without
being refused.

### Statuses

| status | name | meaning |
|---|---|---|
| 0 | Ok | the op's answer; the body is what the op returns |
| 1 | BadRequest | the body did not parse |
| 2 | UnknownOp | no such op byte (or not one a batch allows) |
| 3 | TooLarge | the entry does not fit, or the response would exceed the frame limit |
| 4 | Unauthorized | wrong or missing token |
| 5 | NotFound | the key is not cached |

Statuses 1–4 carry a UTF-8 message as their body; it is for humans and logs, and its wording is
not part of the protocol. `NotFound` is an answer, not an error: a client reports it as
`None`/`null` for a GET and `false` for a DEL, never as a failure. A status byte above 5 is a
desync.

## Batches

A `BATCH` body is a count followed by that many nested frames, each one a complete request in
the same `u8 op, u32 len, body` shape. Only GET, SET and DEL are allowed as items — a nested
AUTH, PING or BATCH is answered `UnknownOp` for that item alone.

The response is a `count`, then that many nested response frames, one per item, in the order the
items were sent. Each item is answered exactly as the same op would be answered on its own, so a
refused item (a value that does not fit, a body that does not parse) is its own status and its
neighbours still go through. Items apply in order, so a GET after a SET of the same key in one
batch sees the write.

The envelope is validated as a whole before anything runs: a truncated item, trailing bytes
after the last one, or a count above `MAX_ITEMS` is `BadRequest` for the entire frame and no
item is applied. A batch whose replies would exceed the response limit is `TooLarge` as a whole,
with nothing partial written.

Runs of the same op are served together (one lookup pass with prefetching for GETs, one epoch
pin for a run of writes), which is what makes a batch of GETs cheaper on the server than the
same GETs pipelined as individual frames. That is an implementation detail of the server, not a
requirement on the client — but it is the reason to prefer a batch.

## Authentication and the connection lifecycle

The server requires a non-empty token. `AUTH` must be the **first** frame on every connection;
anything else — `PING` included — is answered `Unauthorized` and the connection is closed
immediately after that reply is flushed. A wrong token is answered the same way: one attempt per
connection, no retry. The token is compared in constant time, so a wrong one's reply time does
not reveal how many leading bytes matched (the length check does short-circuit; a token's length
is not secret).

Before `AUTH` succeeds, two limits apply that do not apply afterwards:

- frames are capped at `MAX_AUTH_FRAME` (4 KiB) instead of `MAX_FRAME`, so an unauthenticated
  peer cannot make the server buffer much. A larger frame is `TooLarge` and the connection
  closes.
- the connection has `AUTH_TIMEOUT` (30 s) from being accepted to a successful `AUTH`, TLS
  handshake included. That cap is fixed and not configurable, because the idle timeout — which
  an unauthenticated peer could keep resetting with partial input — does not start until it has
  authenticated.

After that, a connection that sends nothing for `OXICACHE_IDLE_TIMEOUT` (default 300 s) is
closed. Clients keep a quiet connection open by sending `PING` after `KEEPALIVE` (100 s) without
a write — a third of the server's default, so two lost heartbeats still leave the connection
open.

There is no unauthenticated mode and no way to drop authentication once made. Without TLS the
token travels in clear text; see the README's TLS section.

### Limits

| constant | value | what it bounds |
|---|---|---|
| `HEADER_LEN` | 5 | tag byte plus `u32` length |
| `MAX_FRAME` | 64 MiB | largest request body accepted from an authenticated peer |
| `MAX_RESPONSE` | 64 MiB | largest response body produced; matches what the clients accept |
| `MAX_ITEMS` | 65 536 | items in one batch |
| `MAX_AUTH_FRAME` | 4 KiB | largest frame accepted before `AUTH` |
| `AUTH_TIMEOUT` | 30 s | accept to successful `AUTH`, handshake included |
| `KEEPALIVE` | 100 s | how long a client waits without a write before it pings |

A single GET can name a key whose value is large, and a batch can name it many times, so the
response is bounded on its own rather than inferred from the request size: a value larger than
`MAX_RESPONSE` is `TooLarge` rather than truncated.

## HTTP

With `OXICACHE_HTTP_ADDR` set, the server also serves the same protocol over HTTP/1.1, one
request per HTTP exchange instead of one per frame. It carries the same binary bodies — nothing
is JSON. The op is the path, the request body is the frame body, the response body is the frame
body, and the frame status becomes the HTTP status.

```
POST /get    body: key                  -> 200, body: value | 404, empty
POST /set    body: u32 klen, key, value -> 200, empty
POST /del    body: key                  -> 200, empty (deleted) | 404, empty
POST /batch  body: items                -> 200, body: replies
POST /ping                              -> 200, empty
GET  /health                            -> 200, no body; never needs a token
GET  /metrics                           -> 200, body: Prometheus text metrics
```

| frame status | HTTP status |
|---|---|
| Ok | 200 |
| BadRequest | 400 |
| Unauthorized | 401 |
| NotFound | 404 |
| TooLarge | 413 |

An unknown path is 404 with a message, and the wrong method on a known path is 405 — a missing
key and an unknown path are both 404, and only the latter has a body. No content type is
declared on either side; neither reads one.

There is no `AUTH` op here. Every request except `/health` carries `Authorization: Bearer
<token>`, checked the same way per request. Every response sent before the token has been
verified — `/health`, 401, 404, 405 — carries `Connection: close`, so an unauthenticated peer
never gets to keep a connection alive. Otherwise keep-alive is on, and hyper's per-request header
timeout uses the idle timeout.

Request bodies are refused by the announced `Content-Length` before anything is read, and by the
actual length while reading, so a chunked body is bounded too. Both listeners serve one cache, so
a value written over TCP is readable over HTTP. There is no CORS handling — put a reverse proxy
in front for that. With `OXICACHE_TLS_CERT` the listener is `https://`.

### Metrics

`GET /metrics` is authenticated like every other path and answers in the Prometheus text
exposition format. Only the HTTP listener serves it, so a TCP-only deployment sets
`OXICACHE_HTTP_ADDR` (on localhost or a private interface, say) to be scrapeable.

| metric | type | meaning |
|---|---|---|
| `oxicache_get_hits_total` | counter | GET requests answered with a value |
| `oxicache_get_misses_total` | counter | GET requests answered not found |
| `oxicache_del_hits_total` | counter | DEL requests that removed an entry |
| `oxicache_del_misses_total` | counter | DEL requests answered not found |
| `oxicache_evictions_total` | counter | live entries evicted to make room; deletes and replacements do not count, so sustained growth means the capacity is short |
| `oxicache_set_too_large_total` | counter | SET requests refused because the entry exceeds the per-shard capacity |
| `oxicache_auth_failures_total` | counter | requests refused for a wrong or missing token |
| `oxicache_accept_waits_total` | counter | accepts that had to wait for a free connection slot |
| `oxicache_used_bytes` | gauge | bytes held by cached entries, bookkeeping included |
| `oxicache_capacity_bytes` | gauge | configured memory budget |
| `oxicache_items` | gauge | entries currently cached |
| `oxicache_open_connections` | gauge | connections open, TCP and HTTP together |
| `oxicache_max_connections` | gauge | configured cap on open connections |
| `oxicache_uptime_seconds` | gauge | seconds since the server started |
| `oxicache_build_info{version="…"}` | gauge | always 1; the version is the label |

The three connection metrics are absent when the connection cap is lifted. Hits, misses,
evictions and the byte/item gauges are summed from the cache's shards when rendered; per-key
results inside a batch count individually.

## Value encoding

The server stores opaque bytes, so this part is a convention between clients rather than
protocol — but both shipped clients follow it, which is what lets a Rust client read a value a
TypeScript client wrote:

- values are **plain MessagePack**, no custom framing around them;
- Rust encodes with `rmp-serde` in named mode (`write_named`), so structs are maps keyed by
  field name rather than positional arrays. A `Vec<u8>` is a MessagePack array; wrap it in
  `serde_bytes` for a `bin`;
- TypeScript encodes with `@msgpack/msgpack`. `bigint` goes through **extension type 0** holding
  the number's decimal string, because msgpack's own int64 formats would make every plain number
  above 2^32 decode as a `bigint`.

Keys are raw bytes on the wire; the clients accept a string (encoded UTF-8) or a byte array.

## The cluster ring

A cluster is entirely client-side: the servers know nothing of each other, and each key belongs
to exactly one of them. Every client of one cluster has to agree on the mapping, so the recipe is
fixed rather than tuned:

- a hash is **MurmurHash3's 32-bit x86 variant with seed 0** (matched against Murmur3's published
  test vectors, so it is anchored to the real function);
- each server puts **512 points** on the ring, at `hash("<name>#<i>")` for `i` in `0..512`, where
  `<name>` is the server's name as given and `<i>` is written in decimal. Ketama uses 160; over
  3 to 16 servers and 100 000 keys, 160 leaves one server up to 16 % off its fair share while 512
  keeps every server within 9 %, for a ring that is still only 4 KiB per server;
- points are sorted by hash, and of two points with the same hash only the one whose server name
  sorts first (by bytes) survives, so the ring does not depend on the order the servers were
  listed in;
- a key belongs to the first point at or after `hash(key)`, wrapping round the end. When failover
  has taken a server out of the ring, the key goes to the next point clockwise whose server is
  live.

The **name** is the ring identity and is hashed character for character, so two clients agree
only if they are given the same servers under the same names. The Rust client names a server by
its `SocketAddr` printed — `10.0.0.1:4433`, IPv6 in canonical short form as
`[2001:db8::1]:4433` — and the TypeScript client by the `name` it was given. Naming one server
`cache-a:4433` in one client and `10.0.0.1:4433` in another gives two different rings, and so
does `[2001:0db8::1]:4433`.

`testdata/ring.tsv` pins both implementations to the same answers; a third client can use it as
its own fixture. Regenerate it with `UPDATE_RING_FIXTURE=1 cargo test -p oxicache-client`.

## Writing a client

1. Connect, send `AUTH` with the token, and expect `Ok` — within 30 s of the connection being
   accepted, in a frame no larger than 4 KiB.
2. Write request frames as fast as you like; read responses in order and match them to requests
   by position.
3. Treat `NotFound` as an answer. Treat statuses 1–4 as this call's own failure, and keep the
   connection: it is still usable.
4. Treat an unknown status byte, or a response with no request waiting, as fatal — close the
   connection rather than guess.
5. Send `PING` after 100 s without a write, or set `OXICACHE_IDLE_TIMEOUT` high enough that you
   do not have to.
6. Batch what you can: a batch of GETs costs the server less than the same GETs sent as
   individual frames.
7. Refuse to send more than `MAX_ITEMS` items or a body over `MAX_FRAME`, so the server never has
   to.
