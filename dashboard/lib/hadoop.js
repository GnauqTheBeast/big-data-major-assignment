import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { streamToProcess } from './transfer.js';

export const ROOT = fileURLToPath(new URL('../../', import.meta.url));
export const SERVICES = [
  { name: 'namenode', label: 'NameNode', role: 'HDFS metadata & namespace', group: 'hdfs', port: 9870 },
  { name: 'datanode', label: 'DataNode', role: 'Stores file blocks', group: 'hdfs', port: 9864 },
  { name: 'datanode-2', label: 'DataNode 2', role: 'Stores file blocks', group: 'hdfs', port: 9865, optional: true },
  { name: 'datanode-3', label: 'DataNode 3', role: 'Stores file blocks', group: 'hdfs', port: 9867, optional: true },
  { name: 'resourcemanager', label: 'ResourceManager', role: 'YARN resource scheduling', group: 'yarn', port: 8088 },
  { name: 'nodemanager', label: 'NodeManager', role: 'YARN task execution', group: 'yarn', port: 8042 },
  { name: 'spark-master', label: 'Spark Master', role: 'Spark application scheduling', group: 'spark', port: 8080 },
  { name: 'spark-worker', label: 'Spark Worker', role: 'Spark task execution', group: 'spark' },
];

export function validateServiceAction(service, action) {
  if (!SERVICES.some(item => item.name === service)) throw Object.assign(new Error('Unknown service.'), { status: 400 });
  if (!['start', 'stop'].includes(action)) throw Object.assign(new Error('Unsupported action. Use start or stop.'), { status: 400 });
}

export function validatePath(value) {
  if (typeof value !== 'string' || !value.startsWith('/') || value.length > 2048 || /[\x00-\x1f\x7f*?\[\]{}\\]/.test(value) || value.split('/').includes('..')) {
    throw Object.assign(new Error('Choose an absolute HDFS path without wildcards or parent traversal.'), { status: 400 });
  }
  return path.posix.normalize(value);
}

export function uploadName(filename) {
  const safeName = filename.replace(/[^a-zA-Z0-9._ -]/g, '_').slice(0, 160) || 'numbers.txt';
  return /^[._]/.test(safeName) ? 'file-' + safeName : safeName;
}

export function validateSortInput(value) {
  const file = validatePath(value);
  if (/^[._]/.test(path.posix.basename(file))) throw Object.assign(new Error('Hadoop ignores filenames beginning with . or _. Upload this file through the dashboard to give it a sortable name.'), { status: 400 });
  return file;
}

export function parseListing(output) {
  return output.split('\n').flatMap(line => {
    const m = line.match(/^([d-]\S+)\s+(\S+)\s+\S+\s+\S+\s+(\d+)\s+(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})\s+(.+)$/);
    return m ? [{ path: m[6], name: path.posix.basename(m[6]), directory: m[1][0] === 'd', size: Number(m[3]), replication: m[2] === '-' ? null : Number(m[2]), modified: `${m[4]} ${m[5]}` }] : [];
  });
}

export function parseBlocks(output) {
  return output.split('\n').flatMap(line => {
    const m = line.match(/(blk_\d+_\d+)\s+len=(\d+)\s+Live_repl=(\d+)/);
    if (!m) return [];
    const locations = [...line.matchAll(/DatanodeInfoWithStorage\[([^,\]]+)/g)].map(match => match[1]);
    return [{ id: m[1], size: Number(m[2]), replicas: Number(m[3]), locations }];
  });
}

export function run(command, args, { timeout = 30000, onData, maxBytes = 4 * 1024 * 1024 } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '', failure;
    const timer = setTimeout(() => { failure = new Error('Command timed out. Check container and HDFS health.'); child.kill(); }, timeout);
    const collect = (chunk, error) => {
      const text = chunk.toString();
      onData?.(text);
      if (error) stderr = (stderr + text).slice(-65536);
      else stdout = onData ? (stdout + text).slice(-65536) : stdout + text;
      if (!onData && Buffer.byteLength(stdout) > maxBytes) { failure = new Error('Response too large. Choose a smaller directory.'); child.kill(); }
    };
    child.stdout.on('data', chunk => collect(chunk, false));
    child.stderr.on('data', chunk => collect(chunk, true));
    child.on('error', error => { clearTimeout(timer); reject(error); });
    child.on('close', code => {
      clearTimeout(timer);
      if (failure) reject(failure);
      else if (code !== 0) reject(new Error(stderr.trim() || stdout.trim() || `Command exited with code ${code}`));
      else resolve(stdout);
    });
  });
}

export class Hadoop {
  docker(args, options) { return run('docker', args, options); }
  hdfs(args, options) { return this.docker(['exec', 'namenode', 'hdfs', ...args], options); }

  async status() {
    let rows = [], error = null;
    try {
      const raw = (await this.docker(['compose', 'ps', '--all', '--format', 'json'])).trim();
      rows = raw ? (raw.startsWith('[') ? JSON.parse(raw) : raw.split('\n').map(line => JSON.parse(line))) : [];
    } catch (e) { error = e.message; }
    let inspected = [];
    const names = rows.map(row => row.Name).filter(name => SERVICES.some(service => service.name === name));
    if (names.length) {
      try { inspected = JSON.parse(await this.docker(['inspect', ...names])); }
      catch (e) { error = e.message; }
    }
    return {
      error, executionMode: 'LocalJobRunner',
      services: SERVICES.filter(service => !service.optional || rows.some(row => row.Service === service.name)).map(service => {
        const row = rows.find(row => row.Service === service.name);
        const info = inspected.find(item => item.Name === '/' + service.name);
        const addresses = Object.values(info?.NetworkSettings?.Networks || {}).map(network => network.IPAddress).filter(Boolean);
        return { ...service, state: row?.State || (error ? 'unknown' : 'not-created'), health: row?.Health || '', addresses, status: row?.Status || '', image: row?.Image || '' };
      }),
    };
  }

  async startCluster(log) {
    await this.docker(['compose', 'up', '-d'], { timeout: 15 * 60 * 1000, onData: log });
  }

  async controlService(service, action) {
    validateServiceAction(service, action);
    await this.docker(action === 'stop' ? ['compose', 'stop', service] : ['compose', 'up', '-d', '--no-deps', service], { timeout: 75000 });
  }

  async list(input) {
    return parseListing(await this.hdfs(['dfs', '-ls', validatePath(input)]));
  }

  async requireFile(input) {
    const file = validatePath(input);
    await this.hdfs(['dfs', '-test', '-f', file]).catch(() => {
      throw Object.assign(new Error('File is unavailable. Select an existing HDFS file and check that HDFS is running.'), { status: 400 });
    });
    return file;
  }

  async preview(input) {
    const file = await this.requireFile(input);
    return { text: await this.hdfs(['dfs', '-head', file]), limit: 1024 };
  }

  async blocks(input) {
    const file = await this.requireFile(input);
    const report = await this.hdfs(['fsck', file, '-files', '-blocks', '-locations']);
    if (!/Status:\s+HEALTHY/.test(report)) throw new Error('HDFS did not return a healthy block report.\n' + report);
    return { blocks: parseBlocks(report), report };
  }

  async upload(input, filename) {
    const safeName = uploadName(filename);
    const folder = `/training/integer-sort/uploads/${randomUUID()}`;
    const destination = `${folder}/${safeName}`;
    const temporary = `${folder}/.uploading`;
    try {
      await this.hdfs(['dfs', '-mkdir', '-p', folder]);
      if (input.destroyed) throw new Error('Upload interrupted.');
      const child = spawn('docker', ['exec', '-i', 'namenode', 'hdfs', 'dfs', '-put', '-', temporary], { cwd: ROOT, stdio: ['pipe', 'pipe', 'pipe'] });
      const size = await streamToProcess(input, child);
      if (!size) throw Object.assign(new Error('Choose a non-empty file.'), { status: 400 });
      // Publish only after stdin finished and HDFS confirmed a successful write.
      await this.hdfs(['dfs', '-mv', temporary, destination]);
      return destination;
    } catch (error) {
      await this.hdfs(['dfs', '-rm', '-r', '-f', folder]).catch(() => {});
      throw error;
    }
  }

  async sort(input, output, log) {
    validateSortInput(input);
    await this.requireFile(input);
    validatePath(output);
    const build = `/tmp/integer-dashboard-${randomUUID()}`;
    log('Building the integer-sort JAR with the container Hadoop libraries…\n');
    try {
      await this.docker(['exec', 'namenode', 'mkdir', '-p', build]);
      await this.docker(['cp', path.join(ROOT, 'integer-sort/src/main/java/com/example/hadoop/IntegerSortJob.java'), `namenode:${build}/IntegerSortJob.java`]);
      // This script is constant; the directory is a positional argument, never shell source.
      await this.docker(['exec', 'namenode', 'sh', '-c', 'cd "$1" && mkdir classes && javac -cp "$(hadoop classpath)" -d classes IntegerSortJob.java && jar cfe job.jar com.example.hadoop.IntegerSortJob -C classes .', 'build', build], { timeout: 120000, onData: log });
      log(`Running LocalJobRunner: ${input} → ${output}\n`);
      await this.docker(['exec', 'namenode', 'hadoop', 'jar', `${build}/job.jar`, input, output], { timeout: 60 * 60 * 1000, onData: log });
    } finally {
      await this.docker(['exec', 'namenode', 'rm', '-rf', build]).catch(() => {});
    }
  }

  download(input) {
    return spawn('docker', ['exec', 'namenode', 'hdfs', 'dfs', '-cat', validatePath(input)], { cwd: ROOT, stdio: ['ignore', 'pipe', 'pipe'] });
  }
}
