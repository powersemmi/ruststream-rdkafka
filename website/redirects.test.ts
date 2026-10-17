import fs from 'node:fs';
import path from 'node:path';
import {describe, expect, it} from 'vitest';
import {redirects} from './redirects';

const docsRoot = path.resolve(__dirname, '../docs');
const moved = redirects('https://docs.rs/ruststream-rdkafka/0.8/');

// Targets whose pages land in a later change; the redirect is written now so the old address
// keeps a home. Once the page exists, its entry here goes.
const awaited = new Set<string>();

// The page a site address is built from, if there is one: `a/b/` comes from `a/b.md` or
// `a/b/index.md`. A page no locale translated is served from the English file, so an address
// with an English page exists in every language.
function page(address: string): string | undefined {
  const relative = address.replace(/^\/|\/$/g, '');
  return [`${relative}.md`, `${relative}/index.md`, `${relative}.mdx`, `${relative}/index.mdx`]
    .map((file) => path.join(docsRoot, file))
    .find((file) => fs.existsSync(file));
}

describe('redirects', () => {
  it('sends the benchmarks page of the old site to its home in Architecture', () => {
    expect(moved.benchmarks).toBe('/architecture/benchmarks/');
  });

  it('sends the guides of the old site that moved to docs.rs to the rustdoc of the version built', () => {
    const rustdoc = 'https://docs.rs/ruststream-rdkafka/0.8/ruststream_rdkafka';
    expect(moved.topics).toBe(`${rustdoc}/index.html#subscribing`);
    expect(moved.publishing).toBe(`${rustdoc}/index.html#publishing`);
    expect(moved['schema-registry']).toBe(`${rustdoc}/schema_registry/index.html`);
  });

  it('sends the testing page of the old site to the testing guide', () => {
    expect(moved.testing).toBe('/guides/testing/');
  });

  it('points every address on the site at a page the build has', () => {
    const internal = Object.entries(moved).filter(([, to]) => to.startsWith('/'));
    expect(internal.length).toBeGreaterThan(0);
    for (const [from, to] of internal) {
      if (awaited.has(to)) {
        expect(page(to), `${to} exists now: drop it from the awaited targets`).toBeUndefined();
      } else {
        expect(page(to), `${from} -> ${to}`).toBeDefined();
      }
    }
  });

  it('never writes over a page: an old address has no page of its own', () => {
    for (const from of Object.keys(moved)) {
      expect(page(from), from).toBeUndefined();
    }
  });
});
