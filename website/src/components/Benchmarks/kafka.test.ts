import fs from 'node:fs';
import path from 'node:path';
import {describe, expect, it} from 'vitest';
import {KAFKA, environmentRows, kafka, type EnvironmentLabels} from './kafka';
import {SCHEMAS, tabContent, type Labels, type Results} from './model';

// The document as the benchmark recipe wrote it, read apart from the module under test, so a
// test compares what the page shows with what the file says.
const document = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, '../../../../docs/benchmarks/results.json'), 'utf8'),
) as Results;

// Every word is its own key, so the tests read the rules and never a page's wording.
const labels = new Proxy({}, {get: (_, key) => String(key)}) as Labels;

const words: EnvironmentLabels = {
  cpu: 'Processor',
  round_trip: 'Round trip',
};

describe("this crate's results", () => {
  it('are the document committed beside the pages, under the catalog entry of this crate', () => {
    expect(kafka.broker).toBe(KAFKA);
    expect(KAFKA.repo).toBe('ruststream-rdkafka');
    expect(kafka.results).toEqual(document);
  });

  it('declare a schema the model renders', () => {
    expect(SCHEMAS).toContain(kafka.results!.schema);
  });

  it('draw one comparison of every scenario the document measured, in its order', () => {
    const {comparison, code, pending} = tabContent(kafka, 'en', labels);
    expect(comparison).toHaveLength(1);
    expect(comparison[0]!.rows.map((row) => row.scenario)).toEqual(
      document.scenarios!.map((scenario) => scenario.name),
    );
    expect(code!.rows.map((row) => row.scenario)).toEqual(
      document.code!.map((scenario) => scenario.name),
    );
    expect(pending).toBeNull();
  });
});

describe('the machine the numbers were taken on', () => {
  it('lists every field of the document, in its order and as recorded', () => {
    const rows = environmentRows(document.environment!, words);
    expect(rows.map((row) => row.field)).toEqual(Object.keys(document.environment!));
    expect(rows.map((row) => row.value)).toEqual(Object.values(document.environment!));
  });

  it('keeps the fields the broker-bound arithmetic and the build need', () => {
    const fields = environmentRows(document.environment!, words).map((row) => row.field);
    expect(fields).toEqual(expect.arrayContaining(['round_trip', 'rustflags', 'profile', 'broker']));
  });

  it('names a field in the words of the page', () => {
    const rows = environmentRows({cpu: 'x86', round_trip: '77 µs'}, words);
    expect(rows.map((row) => row.label)).toEqual(['Processor', 'Round trip']);
  });

  it('names a field the page has no word for by its key, so a new field is never dropped', () => {
    const rows = environmentRows({kernel_tuning: 'none'}, words);
    expect(rows).toEqual([{field: 'kernel_tuning', label: 'kernel_tuning', value: 'none'}]);
  });

  it('leaves out a field the document left empty', () => {
    expect(environmentRows({cpu: 'x86', memory_speed: undefined}, words)).toHaveLength(1);
  });
});
