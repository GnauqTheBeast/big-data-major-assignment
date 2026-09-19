import assert from 'node:assert/strict';
import test from 'node:test';

import { DataNodes } from './datanodes.js';

test('recreates an optional DataNode so it attaches to the current Compose network', async () => {
  const dockerCalls = [];
  const hadoop = {
    async hdfs() {},
    async docker(args) { dockerCalls.push(args); },
  };
  const dataNodes = new DataNodes(hadoop);

  dataNodes.prepare = async () => {};
  dataNodes.settings = async () => ({ enabled: ['datanode'], excluded: [] });
  dataNodes.save = async () => {};
  dataNodes.waitFor = async () => {};

  await dataNodes.add('datanode-2', () => {});

  assert.deepEqual(dockerCalls, [
    ['compose', 'up', '-d', '--no-deps', '--force-recreate', 'datanode-2'],
  ]);
});

test('recreates enabled optional DataNodes when restoring the cluster', async () => {
  const dockerCalls = [];
  const hadoop = {
    async startCluster() {},
    async docker(args) { dockerCalls.push(args); },
  };
  const dataNodes = new DataNodes(hadoop);

  dataNodes.settings = async () => ({
    enabled: ['datanode', 'datanode-2', 'datanode-3'],
    excluded: [],
  });
  dataNodes.save = async () => {};

  await dataNodes.startCluster(() => {});

  assert.deepEqual(dockerCalls, [[
    'compose', 'up', '-d', '--no-deps', '--force-recreate',
    'datanode-2', 'datanode-3',
  ]]);
});
