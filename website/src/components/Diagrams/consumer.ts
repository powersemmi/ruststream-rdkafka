import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The librdkafka consumer: its own threads fetch ahead into a local queue, the application
 * takes records off that queue, and the position it stores is what a timer commits.
 */
export function consumer(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (id: string, x: number, y: number, w: number, fields: Partial<Box> = {}): Box => ({
    id,
    x,
    y,
    w,
    h: 44,
    kind: 'step',
    lines: lines(words, id),
    ...fields,
  });
  // The main column leaves room on its right for the labels of the links into the store.
  const main = 150;
  const side = 240;
  const sideW = 108;
  const boxes: Box[] = [
    box('cluster', 12, 0, 336, {kind: 'broker'}),
    {id: 'threads', x: 0, y: 96, w: 360, h: 140, kind: 'group', lines: lines(words, 'threads')},
    box('fetch', 12, 122, main, {kind: 'broker'}),
    box('queue', 12, 182, main, {kind: 'broker'}),
    box('commit', side, 122, sideW, {kind: 'broker', h: 104}),
    {id: 'app', x: 0, y: 264, w: 360, h: 140, kind: 'group', lines: lines(words, 'app')},
    box('poll', 12, 290, main, {kind: 'service'}),
    box('process', 12, 350, main, {kind: 'service'}),
    box('store', side, 290, sideW, {kind: 'core', h: 104}),
  ];
  // Where a box of the main column meets the store level with its own centre.
  const level = (y: number): number => (y + 22 - 290) / 104;
  const links: Link[] = [
    {from: 'cluster', to: 'fetch', exitAt: (12 + main / 2 - 12) / 336, label: word(words, 'records')},
    {from: 'fetch', to: 'queue'},
    {from: 'queue', to: 'poll'},
    {from: 'poll', to: 'process'},
    {from: 'poll', to: 'store', enterAt: level(290), label: word(words, 'byDefault')},
    {
      from: 'process',
      to: 'store',
      exit: 'right',
      enter: 'left',
      enterAt: level(350),
      dashed: true,
      label: word(words, 'later'),
    },
    {from: 'store', to: 'commit', label: word(words, 'read')},
    {
      from: 'commit',
      to: 'cluster',
      enterAt: (side + sideW / 2 - 12) / 336,
      label: word(words, 'offsetCommit'),
    },
  ];
  return {boxes, links};
}
