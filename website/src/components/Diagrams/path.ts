import type {Box, Link} from './flow';
import {chain} from './chain';
import type {Words} from './words';

/** The path of one record that is answered, from the fetch queue to its committed offset. */
export function path(words: Words): {boxes: Box[]; links: Link[]} {
  return chain(words, [
    {key: 'queue', kind: 'broker'},
    {key: 'take', kind: 'broker', href: '/components/context/'},
    {key: 'dispatch', kind: 'core', href: '/architecture/lanes/'},
    {key: 'decode'},
    {key: 'body', kind: 'service', href: '/components/subscriptions/'},
    {key: 'encode'},
    {key: 'publisher', kind: 'broker', href: '/components/publishing/'},
    {key: 'settle', kind: 'core', href: '/components/settlement/'},
    {key: 'commit', kind: 'broker', href: '/architecture/kafka/consumer/'},
  ]);
}
