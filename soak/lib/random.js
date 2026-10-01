const KB = 1024;
const MB = 1024 * KB;

// A small seeded generator, so a run can be repeated from the seed printed in its report.
function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// An integer in [min, max]
function between(rng, min, max) {
  return min + Math.floor(rng() * (max - min + 1));
}

function pick(rng, items) {
  return items[Math.floor(rng() * items.length)];
}

// The sizes a message body may take, weighted so that every branch of the framing code is
// exercised often: bodies with no content frames at all, bodies that fit one frame, bodies that
// span many, and bodies whose length sits within a few bytes of a frame or socket boundary.
const CLASSES = [
  { name: 'zero', weight: 8 },
  { name: 'tiny', weight: 27 },
  { name: 'small', weight: 25 },
  { name: 'boundary', weight: 15 },
  { name: 'medium', weight: 15 },
  { name: 'large', weight: 10 },
];

const TOTAL_WEIGHT = CLASSES.reduce((sum, c) => sum + c.weight, 0);

function classOf(rng) {
  let roll = rng() * TOTAL_WEIGHT;
  for (const c of CLASSES) {
    roll -= c.weight;
    if (roll < 0) return c.name;
  }
  return CLASSES[CLASSES.length - 1].name;
}

// 8 bytes of every frame are header and frame-end; the rest is body
const FRAME_OVERHEAD = 8;

function sizeFor(rng, { maxSize, frameMax }) {
  const clamp = (n) => Math.max(0, Math.min(maxSize, n));
  switch (classOf(rng)) {
    case 'zero':
      return 0;
    case 'tiny':
      return clamp(between(rng, 1, KB));
    case 'small':
      return clamp(between(rng, KB, 64 * KB));
    case 'boundary': {
      const base = pick(rng, [frameMax - FRAME_OVERHEAD, frameMax, 65536, 16384, 4096]);
      const multiple = between(rng, 1, 4);
      return clamp(base * multiple + between(rng, -3, 3));
    }
    case 'medium':
      return clamp(between(rng, 64 * KB, MB));
    default:
      return clamp(between(rng, MB, maxSize));
  }
}

module.exports = { mulberry32, between, pick, sizeFor, KB, MB };
