import {expect, it} from 'vitest';
import {retries} from './retries';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('decides on the attempt cap before a retry goes anywhere', () => {
  const {links} = retries(words);
  const from = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  expect(from('outcome')).toEqual(['cap']);
  expect(from('cap')).toEqual(['copy', 'deadLetter', 'dropped']);
  // Only the copy below the cap comes back to the subscription; the other answers end there.
  expect(from('copy')).toEqual(['arrives']);
  expect([...from('deadLetter'), ...from('dropped')]).toEqual([]);
});
