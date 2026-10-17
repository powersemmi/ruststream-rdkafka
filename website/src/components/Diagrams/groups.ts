import type {Box, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

// The columns of the timeline: the member that held every partition, the group coordinator on
// the broker, and the member that joins.
const COLUMNS = {first: 62, coordinator: 180, second: 298} as const;
const W = 112;

/*
 * A rebalance as a timeline: a member joins, the coordinator takes partitions from the member
 * that held them and hands them over, and the new owner resumes from the group's committed
 * offset.
 */
export function groups(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const step = (
    id: string,
    column: keyof typeof COLUMNS,
    y: number,
    fields: Partial<Box> = {},
  ): Box => ({
    id,
    x: COLUMNS[column] - W / 2,
    y,
    w: W,
    h: 48,
    kind: 'step',
    lines: lines(words, id),
    ...fields,
  });
  const boxes: Box[] = [
    step('reading', 'first', 28, {kind: 'core'}),
    step('joins', 'second', 28, {kind: 'core'}),
    step('rebalance', 'coordinator', 104, {kind: 'broker', h: 62}),
    step('revoked', 'first', 196),
    step('keeps', 'first', 280, {kind: 'core'}),
    step('resumes', 'second', 280, {kind: 'core', h: 62}),
    {
      id: 'session',
      x: 0,
      y: 366,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'session'),
    },
  ];
  const links: Link[] = [
    {from: 'joins', to: 'rebalance', label: word(words, 'join')},
    {from: 'rebalance', to: 'revoked', label: word(words, 'revoke')},
    {from: 'revoked', to: 'keeps'},
    {from: 'rebalance', to: 'resumes', label: word(words, 'assign')},
  ];
  const lanes: Lane[] = [
    {x: COLUMNS.first, label: word(words, 'firstLane')},
    {x: COLUMNS.coordinator, label: word(words, 'coordinatorLane')},
    {x: COLUMNS.second, label: word(words, 'secondLane')},
  ];
  return {boxes, links, lanes};
}
