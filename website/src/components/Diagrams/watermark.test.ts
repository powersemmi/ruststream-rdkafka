import {describe, expect, it} from 'vitest';
import {DELIVERED, storedPosition, watermark} from './watermark';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

describe('the position Commit::Tracked stores', () => {
  it('stops just below the lowest delivery still unsettled, whatever settled after it', () => {
    expect(storedPosition(DELIVERED)).toBe(11);
  });

  it('reaches the highest delivered offset once nothing is outstanding', () => {
    const settled = DELIVERED.map((delivery) => ({...delivery, settled: true}));
    expect(storedPosition(settled)).toBe(15);
  });

  it('is drawn pointing at that offset', () => {
    const {links} = watermark(words);
    expect(links.find((link) => link.from === 'stored' && link.to.startsWith('o'))?.to).toBe(
      'o11',
    );
  });
});
