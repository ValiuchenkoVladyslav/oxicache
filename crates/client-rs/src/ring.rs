//! The consistent-hash ring a [`Cluster`](crate::Cluster) picks a server
//! with: every key belongs to one server, and adding or losing a server
//! moves only the keys that server owns instead of reshuffling all of
//! them.
//!
//! Every client of one cluster has to agree on the mapping, so the recipe
//! is fixed and spelled out rather than tuned:
//!
//! * a hash is MurmurHash3's 32-bit x86 variant with seed 0, which spreads
//!   near-identical names — every server of a cluster looks like every
//!   other — over the whole word;
//! * each server puts [`POINTS`] points on the ring, at
//!   `hash("<name>#<i>")` for `i` in `0..POINTS`, where the name is the
//!   server's address as it was given (`10.0.0.1:4433`);
//! * points are sorted by hash, and of two points with the same hash only
//!   the one whose server name sorts first survives, so the ring does not
//!   depend on the order the servers were listed in;
//! * a key belongs to the first point at or after its own hash, wrapping
//!   round the end.
//!
//! The TypeScript client's `src/ring.ts` is the same algorithm, and
//! `testdata/ring.tsv` pins both to the same answers.

use std::io::Write;

/// Points one server puts on the ring. Ketama uses 160; measured over
/// 3 to 16 servers and 100 000 keys, 160 leaves one server up to 16 % off
/// its fair share and 512 keeps every server within 9 %, for a ring that
/// is still only 4 KiB a server and built in microseconds.
pub(crate) const POINTS: u32 = 512;

/// MurmurHash3's two mixing constants.
const C1: u32 = 0xcc9e_2d51;
const C2: u32 = 0x1b87_3593;

fn scramble(k: u32) -> u32 {
    k.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2)
}

/// Murmur3's finaliser: a bijection that spreads every input bit over the
/// whole word.
fn fmix32(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^ (h >> 16)
}

/// Where `bytes` land on the ring: MurmurHash3's 32-bit x86 variant with
/// seed 0.
pub(crate) fn hash(bytes: &[u8]) -> u32 {
    let mut h = 0u32;
    let (blocks, tail) = bytes.as_chunks::<4>();
    for block in blocks {
        let k = u32::from_le_bytes(*block);
        h ^= scramble(k);
        h = h.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    if !tail.is_empty() {
        let mut k = 0u32;
        for (i, &b) in tail.iter().enumerate() {
            k |= u32::from(b) << (8 * i);
        }
        h ^= scramble(k);
    }
    fmix32(h ^ bytes.len() as u32)
}

/// The servers of a cluster as points on a circle. Built once, read by
/// every call.
pub(crate) struct Ring {
    /// `(hash, server index)`, sorted by hash and free of duplicates.
    points: Vec<(u32, u32)>,
}

impl Ring {
    /// The ring `names` describe; `names` must not be empty and must have
    /// no duplicates, which [`Cluster`](crate::Cluster) checks.
    pub(crate) fn new(names: &[String]) -> Self {
        let mut points = Vec::with_capacity(names.len() * POINTS as usize);
        let mut point = Vec::new();
        for (i, name) in names.iter().enumerate() {
            for p in 0..POINTS {
                point.clear();
                point.extend_from_slice(name.as_bytes());
                write!(point, "#{p}").expect("a Vec never fails to write");
                points.push((hash(&point), i as u32));
            }
        }
        points.sort_unstable_by(|a, b| {
            a.0.cmp(&b.0).then_with(|| {
                names[a.1 as usize]
                    .as_bytes()
                    .cmp(names[b.1 as usize].as_bytes())
            })
        });
        points.dedup_by_key(|p| p.0);
        Self { points }
    }

    /// The server `key` belongs to: the first one clockwise from the key's
    /// own hash that `live` accepts. With every server refused — a whole
    /// cluster that is down — the key's own server, because trying it is
    /// what finds out it is back.
    pub(crate) fn locate(&self, key: &[u8], live: impl Fn(usize) -> bool) -> usize {
        let n = self.points.len();
        let h = hash(key);
        let start = self.points.partition_point(|p| p.0 < h) % n;
        for i in 0..n {
            let node = self.points[(start + i) % n].1 as usize;
            if live(node) {
                return node;
            }
        }
        self.points[start].1 as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("10.0.0.{i}:4433")).collect()
    }

    /// MurmurHash3's published vectors, so the ring is anchored to the
    /// real function and not just to itself; the four inputs of 1 to 4
    /// bytes cover every tail length.
    #[test]
    fn the_hash_matches_murmur3s_test_vectors() {
        assert_eq!(hash(b""), 0);
        assert_eq!(hash(b"a"), 0x3c25_69b2);
        assert_eq!(hash(b"ab"), 0x9bbf_d75f);
        assert_eq!(hash(b"abc"), 0xb3dd_93fa);
        assert_eq!(hash(b"abcd"), 0x43ed_676a);
        assert_eq!(hash(b"Hello, world!"), 0xc036_3e43);
        // fmix32 keeps zero and is a bijection, so no two of these collide.
        assert_eq!(fmix32(0), 0);
        let mut seen: Vec<u32> = (0..1000).map(fmix32).collect();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), 1000);
    }

    #[test]
    fn every_server_gets_its_points() {
        let ring = Ring::new(&names(4));
        assert_eq!(ring.points.len(), 4 * POINTS as usize, "no collisions here");
        let mut sorted = ring.points.clone();
        sorted.sort_unstable_by_key(|p| p.0);
        assert_eq!(sorted, ring.points, "sorted by hash");
        for i in 0..4 {
            assert_eq!(
                ring.points.iter().filter(|p| p.1 == i).count(),
                POINTS as usize
            );
        }
    }

    #[test]
    fn the_order_servers_are_listed_in_does_not_matter() {
        let forward = names(5);
        let backward: Vec<String> = forward.iter().rev().cloned().collect();
        let (a, b) = (Ring::new(&forward), Ring::new(&backward));
        for i in 0..2000u32 {
            let key = format!("key:{i}");
            let (x, y) = (
                a.locate(key.as_bytes(), |_| true),
                b.locate(key.as_bytes(), |_| true),
            );
            assert_eq!(forward[x], backward[y], "{key}");
        }
    }

    #[test]
    fn keys_are_spread_evenly() {
        const NODES: usize = 8;
        const KEYS: u32 = 100_000;
        let ring = Ring::new(&names(NODES));
        let mut counts = [0u32; NODES];
        for i in 0..KEYS {
            counts[ring.locate(format!("user:{i}").as_bytes(), |_| true)] += 1;
        }
        let fair = KEYS as f64 / NODES as f64;
        for (i, &c) in counts.iter().enumerate() {
            let off = (c as f64 - fair).abs() / fair;
            assert!(
                off < 0.12,
                "server {i} holds {c} of {KEYS} keys, {off:.3} off fair"
            );
        }
    }

    #[test]
    fn losing_a_server_moves_only_its_own_keys() {
        let all = names(5);
        let ring = Ring::new(&all);
        let short: Vec<String> = all.iter().filter(|n| *n != &all[2]).cloned().collect();
        let smaller = Ring::new(&short);
        let (mut moved, mut kept) = (0, 0);
        for i in 0..5000u32 {
            let key = format!("key:{i}");
            let (key, dead) = (key.as_bytes(), 2);
            let before = ring.locate(key, |_| true);
            let after = &short[smaller.locate(key, |_| true)];
            if before == dead {
                moved += 1;
            } else {
                assert_eq!(&all[before], after, "{key:?} moved off a live server");
                kept += 1;
            }
            // Skipping the dead server on the full ring is the same as
            // never having had it.
            assert_eq!(&all[ring.locate(key, |i| i != dead)], after);
        }
        assert!(moved > 0 && kept > 0, "{moved} moved, {kept} kept");
    }

    #[test]
    fn a_cluster_that_is_all_down_still_points_at_the_key_owner() {
        let all = names(3);
        let ring = Ring::new(&all);
        for i in 0..100u32 {
            let key = format!("key:{i}");
            let key = key.as_bytes();
            assert_eq!(ring.locate(key, |_| false), ring.locate(key, |_| true));
        }
    }

    /// The names and keys `testdata/ring.tsv` pins, as the fixture lays
    /// them out.
    const FIXTURE: &str = "../../testdata/ring.tsv";

    fn fixture_names() -> Vec<String> {
        [
            "10.0.0.1:4433",
            "10.0.0.2:4433",
            "10.0.0.3:4433",
            "[::1]:4433",
            "cache-a:4433",
        ]
        .map(String::from)
        .to_vec()
    }

    fn fixture_body() -> String {
        let names = fixture_names();
        let ring = Ring::new(&names);
        let down = &names[1];
        let mut out = String::new();
        out += "# The ring both clients must agree on; see crates/client-rs/src/ring.rs.\n";
        out += "# Regenerate with UPDATE_RING_FIXTURE=1 cargo test -p oxicache-client.\n";
        out += &format!("points\t{POINTS}\n");
        for name in &names {
            out += &format!("node\t{name}\n");
        }
        for input in ["", "a", "foobar", "user:1", "10.0.0.1:4433#0", "ключ"] {
            out += &format!("hash\t{input}\t{}\n", hash(input.as_bytes()));
        }
        for i in 0..24u32 {
            let key = format!("user:{i}");
            out += &format!(
                "route\t{key}\t{}\n",
                names[ring.locate(key.as_bytes(), |_| true)]
            );
        }
        out += &format!("down\t{down}\n");
        for i in 0..24u32 {
            let key = format!("user:{i}");
            let at = ring.locate(key.as_bytes(), |i| &names[i] != down);
            out += &format!("route-down\t{key}\t{}\n", names[at]);
        }
        out
    }

    /// The fixture is what the TypeScript client is tested against, so a
    /// change here that changes where keys land shows up as this test
    /// failing rather than as two clients disagreeing in production.
    #[test]
    fn the_fixture_is_what_this_ring_does() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(FIXTURE);
        let body = fixture_body();
        if std::env::var_os("UPDATE_RING_FIXTURE").is_some() {
            std::fs::write(&path, &body).unwrap();
            return;
        }
        let want = std::fs::read_to_string(&path).unwrap();
        assert_eq!(want, body, "the ring moved; keys would move with it");
    }

    /// Two names whose point sets collide: `10.0.0.14:4433#494` and
    /// `10.0.0.3:4433#186` both hash to 21 872 133. The lower name keeps the
    /// point, so the ring is the same either way round.
    #[test]
    fn colliding_points_keep_the_lower_name() {
        let (low, high) = ("10.0.0.14:4433", "10.0.0.3:4433");
        assert_eq!(hash(format!("{low}#494").as_bytes()), 21_872_133);
        assert_eq!(hash(format!("{high}#186").as_bytes()), 21_872_133);
        let names = [low.to_string(), high.to_string()];
        let reversed = [high.to_string(), low.to_string()];
        let (a, b) = (Ring::new(&names), Ring::new(&reversed));
        assert_eq!(a.points.len(), 2 * POINTS as usize - 1, "one point dropped");
        assert_eq!(b.points.len(), a.points.len());
        let at = a.points.partition_point(|p| p.0 < 21_872_133);
        assert_eq!(names[a.points[at].1 as usize], low);
        assert_eq!(reversed[b.points[at].1 as usize], low);
        for i in 0..2000u32 {
            let key = format!("key:{i}");
            let (x, y) = (
                a.locate(key.as_bytes(), |_| true),
                b.locate(key.as_bytes(), |_| true),
            );
            assert_eq!(names[x], reversed[y], "{key}");
        }
    }

    #[test]
    fn one_server_owns_everything() {
        let ring = Ring::new(&names(1));
        assert_eq!(ring.locate(b"anything", |_| true), 0);
        assert_eq!(ring.locate(b"anything", |_| false), 0);
    }
}
