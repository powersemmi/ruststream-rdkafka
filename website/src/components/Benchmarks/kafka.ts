/*
 * This crate's results, as the pages of this site show them. The document is the one the
 * benchmark recipe committed beside the pages, read when the site is built: a version of the
 * documentation shows the numbers measured for it, and an archive keeps its own.
 */

import document from '../../../../docs/benchmarks/results.json';
import {BROKERS, type Broker, type Loaded, type Results} from './model';

export const KAFKA: Broker = BROKERS.find((broker) => broker.repo === 'ruststream-rdkafka')!;

export const kafka: Loaded = {broker: KAFKA, results: document as Results};

/** The page's word for each field of the document's environment, keyed by the field. */
export type EnvironmentLabels = Record<string, string>;

export interface EnvironmentRow {
  field: string;
  label: string;
  value: string;
}

/**
 * The machine and the build a run was taken on, field by field as the document records it. A
 * field the page has no word for is named by its key, so a field the recipe adds later shows
 * up rather than being dropped.
 */
export function environmentRows(
  environment: Record<string, string | undefined>,
  labels: EnvironmentLabels,
): EnvironmentRow[] {
  return Object.entries(environment)
    .filter((entry): entry is [string, string] => typeof entry[1] === 'string')
    .map(([field, value]) => ({field, label: labels[field] ?? field, value}));
}
