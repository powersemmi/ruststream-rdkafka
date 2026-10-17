import {consumer} from './consumer';
import {design} from './design';
import type {Box, Lane, Link} from './flow';
import {groups} from './groups';
import {lanes} from './lanes';
import {lifecycle} from './lifecycle';
import {log} from './log';
import {path} from './path';
import {retries} from './retries';
import {transactions} from './transactions';
import {watermark} from './watermark';
import type {Words} from './words';

export interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

/** Every diagram a page can name: a layout places the boxes, the page supplies the words. */
export const LAYOUTS = {
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
