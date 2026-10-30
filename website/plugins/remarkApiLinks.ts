import fs from 'node:fs';
import path from 'node:path';
import type {Link, Root} from 'mdast';
import type {VFile} from 'vfile';
import {visit} from 'unist-util-visit';

/**
 * Turns a link to a Rust item into its page on docs.rs, in the version the build documents.
 *
 * A page writes `[KafkaBroker](api:ruststream_rdkafka::KafkaBroker)`: an item, a module
 * (`api:ruststream_rdkafka::schema_registry`), a member of a type
 * (`api:ruststream_rdkafka::KafkaBroker::new`), or a section of a page
 * (`api:ruststream_rdkafka#subscribing`). The path starts with the crate: this crate, or the core
 * it builds on (`api:ruststream::testing::TestApp`). A path that names two items, a struct and the
 * derive macro of the same name, takes rustdoc's kind prefix
 * (`api:derive@ruststream_rdkafka::schema::Schema`); a member resolves on the one item that has it. The link resolves against the rustdoc of the
 * checkout being built, which `scripts/site_rustdoc.sh` produces for both crates the way docs.rs
 * renders them; a crate, a path or a section that rustdoc does not have fails the build with the
 * page and the path. The rustdoc is read on the first such link, so a build whose pages have
 * none (an archive) needs none.
 */
export interface Options {
  /**
   * Each crate a link may name, to its address on docs.rs for the version built, with a
   * trailing slash.
   */
  docsRs: Record<string, string>;
  /** The rustdoc output of the checkout being built (`target/doc`). */
  rustdocDir: string;
}

const SCHEME = 'api:';
// The anchor prefixes rustdoc gives the members on the page of their type.
const MEMBER_ANCHORS = [
  'method',
  'tymethod',
  'structfield',
  'variant',
  'associatedtype',
  'associatedconstant',
];
const ITEM = /<li><a href="([^"]+)">([^<]+)<\/a><\/li>/g;
// rustdoc's kind prefixes, to the start of the page name rustdoc gives an item of that kind.
const KINDS: Record<string, string> = {
  struct: 'struct',
  enum: 'enum',
  union: 'union',
  trait: 'trait',
  fn: 'fn',
  function: 'fn',
  derive: 'derive',
  macro: 'macro',
  attr: 'attr',
  type: 'type',
  const: 'constant',
  constant: 'constant',
  static: 'static',
};
const ID = /\bid="([^"]+)"/g;

class Rustdoc {
  private readonly items = new Map<string, Map<string, string[]>>();
  private readonly anchors = new Map<string, Set<string>>();

  constructor(private readonly dir: string) {}

  /**
   * The pages of the items or the module a path names, relative to the crate's directory: one
   * page, or one per item when a struct and a derive macro share the name. `kind` keeps the
   * items of that kind only.
   */
  pages(crate: string, itemPath: string[], kind?: string): string[] {
    const items = this.itemsOf(crate);
    if (itemPath.length === 0) {
      return kind ? [] : ['index.html'];
    }
    const pages = (items.get(itemPath.join('::')) ?? []).filter(
      (page) => !kind || path.basename(page).startsWith(`${kind}.`),
    );
    if (pages.length > 0 || kind) {
      return pages;
    }
    const module = `${itemPath.join('/')}/index.html`;
    return fs.existsSync(path.join(this.dir, crate, module)) ? [module] : [];
  }

  hasAnchor(crate: string, page: string, anchor: string): boolean {
    const file = path.join(this.dir, crate, page);
    let ids = this.anchors.get(file);
    if (!ids) {
      ids = new Set(Array.from(fs.readFileSync(file, 'utf8').matchAll(ID), (m) => m[1]!));
      this.anchors.set(file, ids);
    }
    return ids.has(anchor);
  }

  private itemsOf(crate: string): Map<string, string[]> {
    let items = this.items.get(crate);
    if (!items) {
      const list = path.join(this.dir, crate, 'all.html');
      if (!fs.existsSync(list)) {
        throw new Error(
          `API links resolve against the rustdoc of this checkout, and ${list} is missing: ` +
            'run `scripts/site_rustdoc.sh` in the repository first',
        );
      }
      items = new Map<string, string[]>();
      for (const [, page, name] of fs.readFileSync(list, 'utf8').matchAll(ITEM)) {
        items.set(name!, [...(items.get(name!) ?? []), page!]);
      }
      this.items.set(crate, items);
    }
    return items;
  }
}

/** The URL of `target` (`crate::path::Item[::member][#section]`), or the reason it has none. */
function resolve(
  rustdoc: Rustdoc,
  docsRs: Record<string, string>,
  target: string,
): string | Error {
  const [prefixed, section] = target.split('#') as [string, string | undefined];
  const at = prefixed.indexOf('@');
  const prefix = at === -1 ? undefined : prefixed.slice(0, at);
  const itemPath = prefixed.slice(at + 1);
  const kind = prefix === undefined ? undefined : KINDS[prefix];
  if (prefix !== undefined && !kind) {
    return new Error(`'${prefix}@' is no item kind; use one of ${Object.keys(KINDS).join(', ')}`);
  }
  const [crate, ...rest] = itemPath.split('::');
  if (!crate) {
    return new Error('the path names no crate');
  }
  const crateDocs = Object.hasOwn(docsRs, crate) ? docsRs[crate] : undefined;
  if (!crateDocs) {
    return new Error(
      `the site documents no crate '${crate}'; links name one of ${Object.keys(docsRs).join(', ')}`,
    );
  }
  const pages = rustdoc.pages(crate, rest, kind);
  if (pages.length > 1) {
    return new Error(
      `the path names ${pages.join(' and ')}; prefix the kind of the one meant (${pages
        .map((page) => `${path.basename(page).split('.')[0]}@`)
        .join(', ')})`,
    );
  }
  let page = pages[0];
  let anchor = section;
  if (!page && rest.length > 1 && !section) {
    // The last segment may be a member on the page of its type.
    const member = rest[rest.length - 1]!;
    for (const owner of rustdoc.pages(crate, rest.slice(0, -1), kind)) {
      const found = MEMBER_ANCHORS.map((memberKind) => `${memberKind}.${member}`).find((id) =>
        rustdoc.hasAnchor(crate, owner, id),
      );
      if (found) {
        page = owner;
        anchor = found;
        break;
      }
    }
  }
  if (!page) {
    return new Error('the rustdoc of this version has no such item');
  }
  if (section && !rustdoc.hasAnchor(crate, page, section)) {
    return new Error(`the page of the item has no section '${section}'`);
  }
  return `${crateDocs}${crate}/${page}${anchor ? `#${anchor}` : ''}`;
}

export default function remarkApiLinks({docsRs, rustdocDir}: Options) {
  const rustdoc = new Rustdoc(rustdocDir);
  return (tree: Root, file: VFile): void => {
    visit(tree, 'link', (node: Link) => {
      if (!node.url.startsWith(SCHEME)) {
        return;
      }
      const target = node.url.slice(SCHEME.length);
      const url = resolve(rustdoc, docsRs, target);
      if (url instanceof Error) {
        file.fail(`${file.path}: API link '${target}': ${url.message}`, node);
      }
      node.url = url;
    });
  };
}
