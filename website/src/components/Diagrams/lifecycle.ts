import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The broker's three states as types on the left, and on the right what each transition does
 * on Kafka: the probe at connect, a consumer per subscription opened at startup, the producer on
 * the first publish, and the flush at shutdown.
 */
export function lifecycle(words: Words): {boxes: Box[]; links: Link[]} {
  const left = (id: string, y: number, href?: string): Box => ({
    id,
    x: 12,
    y,
    w: 156,
    h: 58,
    kind: 'broker',
    code: true,
    lines: lines(words, id),
    href,
  });
  const right = (id: string, y: number, {h = 58, href}: {h?: number; href?: string} = {}): Box => ({
    id,
    x: 192,
    y,
    w: 156,
    h,
    kind: 'core',
    lines: lines(words, id),
    href,
  });
  const boxes: Box[] = [
    {id: 'states', x: 0, y: 0, w: 176, h: 410, kind: 'group', lines: lines(words, 'states')},
    {id: 'kafka', x: 184, y: 0, w: 176, h: 410, kind: 'group', lines: lines(words, 'kafka')},
    left('unconnected', 28, '/components/broker/'),
    left('connected', 168),
    left('closed', 340, '/components/broker/shutdown/'),
    right('config', 28),
    right('probe', 98, {href: '/components/broker/startup/'}),
    right('subscriptions', 168, {h: 72, href: '/components/subscriptions/assignment/'}),
    right('producer', 252, {href: '/components/publishing/'}),
    right('flush', 340, {href: '/components/broker/shutdown/'}),
    {
      id: 'afterShutdown',
      x: 0,
      y: 432,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'afterShutdown'),
      href: '/components/broker/shutdown/',
    },
  ];
  const links: Link[] = [
    {from: 'unconnected', to: 'connected', label: word(words, 'connect')},
    {from: 'connected', to: 'closed', label: word(words, 'shutdown')},
    {from: 'config', to: 'probe'},
    {from: 'probe', to: 'subscriptions'},
    {from: 'subscriptions', to: 'producer', dashed: true},
    {from: 'producer', to: 'flush'},
  ];
  return {boxes, links};
}
