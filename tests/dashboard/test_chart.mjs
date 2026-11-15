// Run directly with node, or via pytest's installed-asset test.
import assert from 'node:assert/strict';
import {test} from 'node:test';

globalThis.document = {querySelectorAll: () => []};
const hostChart = {syntheticHostSentinel: true};
globalThis.Chart = hostChart;
globalThis.window = {Chart: hostChart, requestAnimationFrame: callback => callback()};
const {outcomeChartData} = await import(process.env.DEWEY_DASHBOARD_JS_FILE || new URL('../../src/dewey/dashboard/assets/dashboard.js', import.meta.url));
const observedAt = '2026-10-07T12:00:00.000Z';
const history = {
  recorded_from: '2026-10-07T10:15:00.000Z', bucket_seconds: 3600,
  buckets: [
    {at: '2026-10-07T09:00:00.000Z', completed: null, dead: null, coverage: 'unavailable'},
    {at: '2026-10-07T10:00:00.000Z', completed: 2, dead: 1, coverage: 'partial'},
    {at: '2026-10-07T11:00:00.000Z', completed: 3, dead: 2, coverage: 'recorded'},
  ],
};

test('cumulative outcomes preserve unavailable history and start from a known baseline', () => {
  const data = outcomeChartData(history, {observedAt});
  assert.deepEqual(data.datasets.map(dataset => dataset.label), ['Completed', 'DLQ']);
  assert.deepEqual(data.datasets[0].data, [null, 0, 2, 5]);
  assert.deepEqual(data.datasets[1].data, [null, 0, 1, 3]);
  assert.deepEqual(data.labels, ['2026-10-07T10:00:00.000Z', '2026-10-07T10:15:00.000Z', '2026-10-07T11:00:00.000Z', observedAt]);
  assert.deepEqual(data.coverage, ['unavailable', 'recording baseline', 'partial', 'recorded']);
});

test('per-bucket mode is not cumulative and does not invent zero legacy buckets', () => {
  const data = outcomeChartData(history, {mode: 'bucket', observedAt});
  assert.deepEqual(data.datasets[0].data, [null, 2, 3]);
  assert.deepEqual(data.datasets[1].data, [null, 1, 2]);
});

test('the partial last point stops at observation time rather than projecting into the future', () => {
  const data = outcomeChartData({...history, buckets: [...history.buckets, {at: observedAt, completed: 4, dead: 0, coverage: 'partial'}]}, {observedAt: '2026-10-07T12:25:00.000Z'});
  assert.equal(data.labels.at(-1), '2026-10-07T12:25:00.000Z');
  assert.equal(data.datasets[0].data.at(-1), 9);
});

test('available history baseline is clipped to the selected window, not the migration date', () => {
  const data = outcomeChartData({...history, recorded_from: '2026-10-01T00:00:00Z', buckets: [history.buckets[2]]}, {observedAt, hours: 1});
  assert.equal(data.labels[0], '2026-10-07T11:00:00.000Z');
  assert.deepEqual(data.datasets[0].data, [0, 3]);
});

test('the vendored chart constructor does not replace the host global', () => {
  assert.equal(globalThis.Chart, hostChart);
  assert.equal(globalThis.window.Chart, hostChart);
});
