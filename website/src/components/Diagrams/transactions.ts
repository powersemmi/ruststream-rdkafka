import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * A consume-transform-produce transaction: the records it produces and the offsets it consumed
 * commit together, or an abort discards both.
 */
export function transactions(words: Words): {boxes: Box[]; links: Link[]} {
  const main = (id: string, y: number, fields: Partial<Box> = {}): Box => ({
    id,
    x: 12,
    y,
    w: 200,
    h: 48,
    kind: 'step',
    lines: lines(words, id),
    ...fields,
  });
  const boxes: Box[] = [
    main('begin', 0, {kind: 'service'}),
    main('produce', 74),
    main('offsets', 148),
    main('commit', 222, {kind: 'core'}),
    main('visible', 296, {kind: 'good', h: 62}),
    {
      id: 'abort',
      x: 236,
      y: 222,
      w: 120,
      h: 136,
      kind: 'bad',
      lines: lines(words, 'abort'),
    },
    {
      id: 'fencing',
      x: 0,
      y: 382,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'fencing'),
    },
  ];
  const links: Link[] = [
    {from: 'begin', to: 'produce'},
    {from: 'produce', to: 'offsets'},
    {from: 'offsets', to: 'commit'},
    {from: 'commit', to: 'visible', label: word(words, 'markers')},
    {from: 'offsets', to: 'abort', exit: 'right', enter: 'top', label: word(words, 'onError')},
  ];
  return {boxes, links};
}
