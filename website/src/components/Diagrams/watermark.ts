import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The deliveries of one partition in the drawing, by offset, and whether each has settled.
export const DELIVERED: ReadonlyArray<{offset: number; settled: boolean}> = [
  {offset: 10, settled: true},
  {offset: 11, settled: true},
  {offset: 12, settled: false},
  {offset: 13, settled: true},
  {offset: 14, settled: true},
  {offset: 15, settled: false},
];

/** The position `Commit::Tracked` stores: just below the lowest unsettled delivery. */
export function storedPosition(delivered: typeof DELIVERED): number {
  const unsettled = delivered.filter((delivery) => !delivery.settled);
  return unsettled.length
    ? Math.min(...unsettled.map((delivery) => delivery.offset)) - 1
    : Math.max(...delivered.map((delivery) => delivery.offset));
}

const STEP = 56;

/*
 * The watermark of `Commit::Tracked` on one partition: acks arrive out of order from concurrent
 * lanes, and the stored position stops below the lowest delivery still unsettled.
 */
export function watermark(words: Words): {boxes: Box[]; links: Link[]} {
  const stored = storedPosition(DELIVERED);
  const boxes: Box[] = [
    {id: 'partition', x: 0, y: 0, w: 360, h: 20, kind: 'note', lines: lines(words, 'partition')},
    ...DELIVERED.map(
      ({offset, settled}, index): Box => ({
        id: `o${offset}`,
        x: 12 + index * STEP,
        y: 28,
        w: 48,
        h: 44,
        kind: settled ? 'good' : 'step',
        lines: [String(offset), word(words, settled ? 'settled' : 'unsettled')],
      }),
    ),
    {id: 'stored', x: 12, y: 112, w: 160, h: 58, kind: 'core', lines: lines(words, 'stored')},
    {
      id: 'committed',
      x: 12,
      y: 212,
      w: 160,
      h: 58,
      kind: 'broker',
      lines: lines(words, 'committed'),
    },
    {id: 'next', x: 188, y: 112, w: 172, h: 158, kind: 'note', lines: lines(words, 'next')},
  ];
  const links: Link[] = [
    {from: 'stored', to: `o${stored}`, label: word(words, 'storedAt')},
    {from: 'stored', to: 'committed', label: word(words, 'commits')},
  ];
  return {boxes, links};
}
