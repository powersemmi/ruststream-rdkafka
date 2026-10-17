// Pages of the earlier site, by their address within a version. Every language of a build gets
// the same set.

// The guides of the earlier site moved to the crate's rustdoc, where each topic is a section of
// the crate overview or a module of its own.
const guides: Record<string, string> = {
  topics: 'index.html#subscriptions',
  publishing: 'index.html#publishing',
  'schema-registry': 'schema_registry/index.html',
};

// Pages that moved within the site, and the address of the guides category, whose overview is the
// front page of the version.
const pages: Record<string, string> = {
  guides: '/',
  testing: '/guides/testing/',
  // The benchmarks explain what the architecture costs.
  benchmarks: '/architecture/benchmarks/',
};

/**
 * Old address to its target, relative to the build's base URL. `docsRs` is the crate's rustdoc
 * for the version built, with a trailing slash.
 */
export function redirects(docsRs: string): Record<string, string> {
  return {
    ...Object.fromEntries(
      Object.entries(guides).map(([from, to]) => [from, `${docsRs}ruststream_rdkafka/${to}`]),
    ),
    ...pages,
  };
}
