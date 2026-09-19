import { readFile, writeFile, mkdir, rename } from 'node:fs/promises';
import path from 'node:path';
import { ROOT } from './hadoop.js';

export const DATA_NODES = ['datanode', 'datanode-2', 'datanode-3'];
export const isDataNode = service => DATA_NODES.includes(service);
const stateDirectory = path.join(ROOT, 'dashboard/state');
const stateFile = path.join(stateDirectory, 'datanodes.json');
const excludeFile = path.join(stateDirectory, 'excluded-datanodes');
const containerExcludeFile = '/etc/hadoop/dashboard/excluded-datanodes';
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const normalized = value => String(value || '').toLowerCase().replace(/[^a-z]/g, '');

// The original DataNode keeps its existing volume. Extra nodes have their own
// Compose service and volume, so scale changes never share a storage directory.
export class DataNodes {
  constructor(hadoop) { this.hadoop = hadoop; }

  async settings() {
    try {
      const saved = JSON.parse(await readFile(stateFile, 'utf8'));
      if (!Array.isArray(saved.enabled) || !Array.isArray(saved.excluded) ||
          !saved.enabled.includes('datanode') || saved.excluded.includes('datanode') ||
          [...saved.enabled, ...saved.excluded].some(name => !DATA_NODES.includes(name))) {
        throw new Error('Invalid DataNode state file.');
      }
      return saved;
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
      return { enabled: ['datanode'], excluded: [] };
    }
  }

  async save(settings) {
    await mkdir(stateDirectory, { recursive: true });
    // A directory bind mount allows atomic file replacement to reach NameNode.
    await writeFile(excludeFile + '.tmp', settings.excluded.join('\n') + '\n');
    await rename(excludeFile + '.tmp', excludeFile);
    await writeFile(stateFile + '.tmp', JSON.stringify(settings, null, 2) + '\n');
    await rename(stateFile + '.tmp', stateFile);
  }

  async report() {
    const response = await fetch('http://127.0.0.1:9870/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo', { signal: AbortSignal.timeout(5000) });
    if (!response.ok) throw new Error(`NameNode metrics unavailable (${response.status}).`);
    const info = (await response.json()).beans?.[0];
    if (!info?.LiveNodes) throw new Error('NameNode has not published DataNode metrics yet.');
    const unpack = (value, live) => Object.entries(JSON.parse(value || '{}')).map(([address, item]) => ({ ...item, address, live }));
    return { live: unpack(info.LiveNodes, true), dead: unpack(info.DeadNodes, false) };
  }

  match(service, report) {
    return report.live.find(item => this.matches(service, item)) || report.dead.find(item => this.matches(service, item));
  }

  matches(service, item) {
    const hosts = [item.address, item.xferaddr, item.infoAddr].filter(Boolean).map(address => address.split(':')[0]);
    return hosts.includes(service.name) || hosts.some(host => service.addresses.includes(host));
  }

  async status() {
    const [settings, containers] = await Promise.all([this.settings(), this.hadoop.status()]);
    let report = { live: [], dead: [] }, error = containers.error;
    try { report = await this.report(); } catch (failure) { error = failure.message; }
    return {
      error, maximum: DATA_NODES.length,
      nodes: DATA_NODES.map((name, index) => {
        const service = containers.services.find(item => item.name === name) || { name, state: 'not-created', addresses: [] };
        const metrics = this.match(service, report);
        return {
          ...service, label: `DataNode ${index + 1}`, primary: index === 0,
          enabled: settings.enabled.includes(name), excluded: settings.excluded.includes(name),
          live: Boolean(metrics?.live), adminState: metrics?.adminState || 'Unavailable',
          capacity: metrics?.capacity ?? null, used: metrics?.usedSpace ?? null,
          remaining: metrics?.remaining ?? null, blocks: metrics?.numBlocks ?? null,
        };
      }),
    };
  }

  async waitFor(check, log, message, timeout = 180000) {
    const deadline = Date.now() + timeout;
    let lastError;
    while (Date.now() < deadline) {
      try { if (await check()) return; } catch (error) { lastError = error; }
      log(message + (lastError ? ` (${lastError.message})` : '') + '\n');
      lastError = null;
      await pause(5000);
    }
    throw new Error('Timed out: ' + message + ' The DataNode was not forcibly removed.');
  }

  async prepare(log, { waitForSafeMode = true } = {}) {
    await this.save(await this.settings());
    let configured = '';
    try { configured = (await this.hadoop.hdfs(['getconf', '-confKey', 'dfs.hosts.exclude'])).trim(); } catch { /* First setup may not have a running NameNode. */ }
    if (configured !== containerExcludeFile) {
      log('Applying NameNode decommission configuration. Its existing data volume is preserved.\n');
      await this.hadoop.docker(['compose', 'up', '-d', '--no-deps', 'namenode'], { timeout: 120000, onData: log });
    }
    await this.waitFor(async () => { await this.report(); return true; }, log, 'Waiting for NameNode');
    if (waitForSafeMode) await this.hadoop.hdfs(['dfsadmin', '-safemode', 'wait'], { timeout: 180000, onData: log });
  }

  async startCluster(log) {
    const settings = await this.settings();
    await this.save(settings);
    await this.hadoop.startCluster(log);
    const extras = settings.enabled.filter(name => name !== 'datanode');
    if (extras.length) await this.hadoop.docker(['compose', 'up', '-d', '--no-deps', '--force-recreate', ...extras], { timeout: 120000, onData: log });
  }

  async add(service, log) {
    if (!DATA_NODES.slice(1).includes(service)) throw new Error('Choose DataNode 2 or 3. The primary node is already configured.');
    // A missing node may hold the very blocks needed for NameNode to exit safe
    // mode. Start/rejoin it before waiting for safe mode to end.
    await this.prepare(log, { waitForSafeMode: false });
    const settings = await this.settings();
    settings.enabled = [...new Set([...settings.enabled, service])];
    settings.excluded = settings.excluded.filter(name => name !== service);
    await this.save(settings);
    await this.hadoop.hdfs(['dfsadmin', '-refreshNodes'], { onData: log });
    log(`Starting ${service} with its own persistent volume…\n`);
    await this.hadoop.docker(['compose', 'up', '-d', '--no-deps', '--force-recreate', service], { timeout: 120000, onData: log });
    await this.waitFor(async () => {
      const data = await this.status();
      const node = data.nodes.find(item => item.name === service);
      return !data.error && node.live && normalized(node.adminState) === 'inservice';
    }, log, `Waiting for ${service} to join HDFS`);
    await this.hadoop.hdfs(['dfsadmin', '-safemode', 'wait'], { timeout: 180000, onData: log });
    log(`${service} joined. New writes can use it. Use Rebalance to redistribute existing blocks.\n`);
  }

  async remove(service, log) {
    if (!DATA_NODES.slice(1).includes(service)) throw new Error('Keep the primary DataNode; at least one node must remain.');
    await this.prepare(log);
    const before = await this.status();
    const target = before.nodes.find(item => item.name === service);
    if (before.error || !target.live) throw new Error('The target must be running and visible to NameNode before safe removal. Start/rejoin it first.');
    if (!before.nodes.some(item => item.name !== service && item.live && normalized(item.adminState) === 'inservice')) throw new Error('No remaining live DataNode can receive the blocks.');
    const settings = await this.settings();
    settings.excluded = [...new Set([...settings.excluded, service])];
    await this.save(settings);
    log(`Decommissioning ${service}. HDFS must preserve the required replicas before it can stop.\n`);
    await this.hadoop.hdfs(['dfsadmin', '-refreshNodes'], { onData: log });
    await this.waitFor(async () => {
      const data = await this.status();
      const node = data.nodes.find(item => item.name === service);
      if (data.error) throw new Error(data.error);
      return node.live && normalized(node.adminState) === 'decommissioned';
    }, log, 'Waiting for HDFS to complete decommissioning (check remaining capacity and replica requirements)', 30 * 60 * 1000);
    log('NameNode confirmed decommissioning complete. Stopping the container; its volume is kept.\n');
    await this.hadoop.docker(['compose', 'stop', service], { timeout: 75000, onData: log });
    settings.enabled = settings.enabled.filter(name => name !== service);
    await this.save(settings);
  }

  async rebalance(log) {
    await this.prepare(log);
    log('Balancing HDFS block storage. This does not change replication or the sort execution mode.\n');
    await this.hadoop.hdfs(['balancer', '-threshold', '10'], { timeout: 30 * 60 * 1000, onData: log });
  }
}
