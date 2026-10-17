import {existsSync} from 'node:fs';
import {join} from 'node:path';
import {describe, expect, it} from 'vitest';
import {LAYOUTS, type DiagramName} from './layouts';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

// The docs are served from the site root, so a route names the page's file under `docs/`.
const DOCS = join(__dirname, '..', '..', '..', '..', 'docs');

const hrefs = (name: DiagramName): Record<string, string> =>
  Object.fromEntries(
    LAYOUTS[name](words)
      .boxes.filter((box) => box.href !== undefined)
      .map((box) => [box.id, box.href!]),
  );

const page = (href: string): boolean => {
  const route = href.replace(/^\/|\/$/g, '');
  return ['.md', '.mdx', '/index.md', '/index.mdx'].some((ending) =>
    existsSync(join(DOCS, route + ending)),
  );
};

describe('a box that links to a page', () => {
  const every = (Object.keys(LAYOUTS) as DiagramName[]).flatMap((name) =>
    Object.entries(hrefs(name)).map(([box, href]) => ({name, box, href})),
  );

  it.each(every)('$name, $box: $href is a page of the site', ({href}) => {
    // A root-relative route: the link prepends the language's base URL, so one route serves
    // every translation, and the site ends every route with a slash.
    expect(href).toMatch(/^\/[a-z0-9/-]+\/$/);
    expect(page(href), `no page under docs/ for ${href}`).toBe(true);
  });
});

describe('a box that names a component', () => {
  it('links to that component page on the path of a record', () => {
    expect(hrefs('path')).toEqual({
      take: '/components/context/',
      dispatch: '/architecture/lanes/',
      body: '/components/subscriptions/',
      publisher: '/components/publishing/',
      settle: '/components/settlement/',
      commit: '/architecture/kafka/consumer/',
    });
  });

  it('links to that component page in the design of the crate', () => {
    expect(hrefs('design')).toEqual({
      publishers: '/components/publishing/',
      tracker: '/components/settlement/tracked/',
      eos: '/components/transactions/exactly-once/eos-pipeline/',
      broker: '/components/broker/',
      descriptors: '/components/subscriptions/',
      consumer: '/architecture/kafka/consumer/',
    });
  });

  it('links to that component page in the lifecycle', () => {
    expect(hrefs('lifecycle')).toEqual({
      unconnected: '/components/broker/',
      probe: '/components/broker/startup/',
      subscriptions: '/components/subscriptions/assignment/',
      producer: '/components/publishing/',
      closed: '/components/broker/shutdown/',
      flush: '/components/broker/shutdown/',
      afterShutdown: '/components/broker/shutdown/',
    });
  });

  it('links to that component page in the lanes, the watermark and the retries', () => {
    expect(hrefs('lanes')).toEqual({
      keyed: '/components/subscriptions/lane-key/',
      recordKey: '/components/subscriptions/lane-key/',
    });
    expect(hrefs('watermark')).toEqual({stored: '/components/settlement/tracked/'});
    expect(hrefs('retries')).toEqual({
      outcome: '/components/settlement/retries/',
      cap: '/components/settlement/retries/',
      deadLetter: '/components/settlement/dead-letters/',
    });
  });
});
