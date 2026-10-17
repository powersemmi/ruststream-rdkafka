import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The records each partition holds in the drawing: partitions grow apart, so their offsets do.
export const PARTITIONS = [6, 4, 5] as const;

const CELL = 36;
const STEP = 42;

/*
 * The log: a producer appends to the partitions of one topic, each partition numbers its own
 * records, and every consumer group reads the whole topic, sharing the partitions among its
 * members.
 */
export function log(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {
      id: 'producer',
      x: 90,
      y: 0,
      w: 180,
      h: 44,
      kind: 'service',
      lines: lines(words, 'producer'),
    },
    {id: 'topic', x: 0, y: 72, w: 360, h: 150, kind: 'group', lines: lines(words, 'topic')},
  ];
  PARTITIONS.forEach((count, partition) => {
    const y = 98 + partition * 40;
    boxes.push({
      id: `p${partition}`,
      x: 12,
      y,
      w: 44,
      h: 30,
      kind: 'broker',
      lines: lines(words, `p${partition}`),
    });
    for (let offset = 0; offset < count; offset += 1) {
      boxes.push({
        id: `p${partition}o${offset}`,
        x: 66 + offset * STEP,
        y,
        w: CELL,
        h: 30,
        kind: 'step',
        lines: [String(offset)],
      });
    }
  });
  boxes.push(
    {id: 'billing', x: 0, y: 256, w: 172, h: 112, kind: 'group', lines: lines(words, 'billing')},
    {
      id: 'billing1',
      x: 12,
      y: 282,
      w: 148,
      h: 34,
      kind: 'core',
      lines: lines(words, 'billing1'),
    },
    {
      id: 'billing2',
      x: 12,
      y: 324,
      w: 148,
      h: 34,
      kind: 'core',
      lines: lines(words, 'billing2'),
    },
    {id: 'audit', x: 188, y: 256, w: 172, h: 112, kind: 'group', lines: lines(words, 'audit')},
    {
      id: 'audit1',
      x: 200,
      y: 282,
      w: 148,
      h: 76,
      kind: 'core',
      lines: lines(words, 'audit1'),
    },
    {
      id: 'offsets',
      x: 0,
      y: 384,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'offsets'),
    },
  );
  const links: Link[] = [
    {from: 'producer', to: 'topic', label: word(words, 'appends')},
    {from: 'topic', to: 'billing', exitAt: 86 / 360, label: word(words, 'reads')},
    {from: 'topic', to: 'audit', exitAt: 274 / 360, label: word(words, 'reads')},
  ];
  return {boxes, links};
}
