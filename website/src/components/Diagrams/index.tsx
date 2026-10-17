import type {ReactNode} from 'react';
import {consumer} from './consumer';
import {design} from './design';
import {groups} from './groups';
import {lanes} from './lanes';
import {lifecycle} from './lifecycle';
import {log} from './log';
import {path} from './path';
import {retries} from './retries';
import {transactions} from './transactions';
import {watermark} from './watermark';
import {Flow, type Lane} from './Flow';
import type {Box, Link} from './flow';
import {word, type Words} from './words';

/*
 * The diagrams of the Architecture pages. A layout places the boxes; the page supplies every word,
 * so a translation changes the words and never the drawing.
 */

interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

const LAYOUTS = {
  log,
  groups,
  consumer,
  transactions,
  design,
  lifecycle,
  path,
  lanes,
  watermark,
  retries,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;

export function Diagram({name, words}: {name: DiagramName; words: Words}): ReactNode {
  const layout: Layout = LAYOUTS[name](words);
  return <Flow title={word(words, 'title')} text={word(words, 'text')} {...layout} />;
}
