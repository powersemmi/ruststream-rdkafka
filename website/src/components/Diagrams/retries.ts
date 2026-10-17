import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * A retry on Kafka, which holds no record back and counts no deliveries: the runtime settles
 * the original, reads its own count off a header, and publishes a copy to the subscription's
 * topic, to the dead-letter topic, or nowhere.
 */
export function retries(words: Words): {boxes: Box[]; links: Link[]} {
  const boxes: Box[] = [
    {id: 'record', x: 12, y: 0, w: 336, h: 44, kind: 'broker', lines: lines(words, 'record')},
    {id: 'outcome', x: 12, y: 72, w: 336, h: 44, kind: 'service', lines: lines(words, 'outcome')},
    {id: 'cap', x: 12, y: 144, w: 336, h: 44, kind: 'core', lines: lines(words, 'cap')},
    {id: 'copy', x: 0, y: 220, w: 112, h: 104, kind: 'step', lines: lines(words, 'copy')},
    {
      id: 'deadLetter',
      x: 124,
      y: 220,
      w: 112,
      h: 104,
      kind: 'bad',
      lines: lines(words, 'deadLetter'),
    },
    {id: 'dropped', x: 248, y: 220, w: 112, h: 104, kind: 'bad', lines: lines(words, 'dropped')},
    {id: 'arrives', x: 0, y: 352, w: 236, h: 58, kind: 'broker', lines: lines(words, 'arrives')},
    {id: 'crash', x: 0, y: 432, w: 360, h: 44, kind: 'note', lines: lines(words, 'crash')},
  ];
  const along = (x: number): number => (x - 12) / 336;
  const links: Link[] = [
    {from: 'record', to: 'outcome', label: word(words, 'handled')},
    {from: 'outcome', to: 'cap', label: word(words, 'retries')},
    {from: 'cap', to: 'copy', exitAt: along(56), label: word(words, 'below')},
    {from: 'cap', to: 'deadLetter', exitAt: along(180), label: word(words, 'atCap')},
    {from: 'cap', to: 'dropped', exitAt: along(304)},
    {from: 'copy', to: 'arrives', enterAt: 56 / 236},
  ];
  return {boxes, links};
}
